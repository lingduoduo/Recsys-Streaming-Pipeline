#!/usr/bin/env bash
# End-to-end engagement time-series simulation:
#   docker (Kafka+Redis) → backfill_producer → OnlineJoinerStreamingJob → date-partitioned Parquet.
# The Parquet output under $SIM_ROOT/training-samples is the time-series store the engagement
# report reads (services/python-modeling/engagement_report.py).
set -euo pipefail
cd "$(dirname "$0")"

SIM_ROOT="${SIM_ROOT:-/tmp/spark-recsys/engagement-sim}"
OUT_DIR="$SIM_ROOT/training-samples"
CKPT_DIR="$SIM_ROOT/checkpoint"
LOG_FILE="$SIM_ROOT/online-joiner.log"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-360}"   # seconds to wait for the backlog to drain

echo "==> fresh sim dirs under $SIM_ROOT"
rm -rf "$SIM_ROOT"; mkdir -p "$SIM_ROOT"

echo "==> starting Kafka + Redis (docker compose up -d)"
docker compose up -d zookeeper kafka redis

echo "==> waiting for Kafka to be healthy"
for _ in $(seq 1 60); do
  status="$(docker compose ps kafka --format '{{.Health}}' 2>/dev/null || echo "")"
  [[ "$status" == "healthy" ]] && break
  sleep 3
done
[[ "$(docker compose ps kafka --format '{{.Health}}')" == "healthy" ]] || { echo "Kafka not healthy"; exit 1; }

if [[ ! -f services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar ]]; then
  echo "==> building Spark job jar"; (cd services/spark-streaming-job && sbt assembly)
fi

echo "==> backfilling synthetic engagement events"
BACKFILL_DAYS="${BACKFILL_DAYS:-21}" SLATES_PER_HOUR="${SLATES_PER_HOUR:-12}" \
  python services/python-modeling/backfill_producer.py

echo "==> starting OnlineJoiner to drain the backlog into Parquet"
SPARK_MAIN_CLASS=com.demo.process.OnlineJoinerStreamingJob \
SPARK_CHECKPOINT_LOCATION="$CKPT_DIR" \
ONLINE_JOINER_HDFS_OUTPUT_PATH="$OUT_DIR" \
KAFKA_STARTING_OFFSETS=earliest \
EVENT_WATERMARK_DELAY="3650 days" \
MAX_OFFSETS_PER_TRIGGER="${MAX_OFFSETS_PER_TRIGGER:-500000}" \
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-2 seconds}" \
  ./run-streaming-job.sh >"$LOG_FILE" 2>&1 &
JOB_PID=$!
trap 'kill "$JOB_PID" 2>/dev/null || true' EXIT

echo "==> draining (poll Parquet until stable; timeout ${DRAIN_TIMEOUT}s) — log: $LOG_FILE"
mkdir -p "$OUT_DIR"   # so the poll's find never fails before the job's first write
prev=-1; stable=0; waited=0
while (( waited < DRAIN_TIMEOUT )); do
  sleep 6; waited=$((waited + 6))
  count="$(find "$OUT_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ' || true)"
  echo "   t=${waited}s parquet_files=$count"
  if [[ "$count" -gt 0 && "$count" == "$prev" ]]; then
    stable=$((stable + 1)); (( stable >= 3 )) && break
  else
    stable=0
  fi
  prev="$count"
done

kill "$JOB_PID" 2>/dev/null || true; trap - EXIT
parts="$(find "$OUT_DIR" -maxdepth 1 -type d -name 'date=*' 2>/dev/null | wc -l | tr -d ' ' || true)"
echo "==> done. $parts date partitions under $OUT_DIR"
echo "    next: python services/python-modeling/engagement_report.py --input $OUT_DIR"
echo "    stop infra with: docker compose down"
