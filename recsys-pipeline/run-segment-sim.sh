#!/usr/bin/env bash
# End-to-end user-segment engagement simulation:
#   docker (Kafka+Redis) → segment_producer → recsys_events → OnlineJoinerStreamingJob →
#   date-partitioned Parquet (carries user_features/context_features) → segment_report.py.
set -euo pipefail
cd "$(dirname "$0")"

SIM_ROOT="${SIM_ROOT:-/tmp/spark-recsys/segment-sim}"
OUT_DIR="$SIM_ROOT/training-samples"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-360}"

# Poll a numeric probe until it is >0 and unchanged for 3 reads, then return.
wait_stable() {
  local label="$1" probe="$2" prev=-1 stable=0 waited=0 count
  while (( waited < DRAIN_TIMEOUT )); do
    sleep 6; waited=$((waited + 6))
    count="$(eval "$probe" 2>/dev/null | tr -d ' \r' || true)"; count="${count:-0}"
    echo "   [$label] t=${waited}s count=$count"
    if [[ "$count" =~ ^[0-9]+$ && "$count" -gt 0 && "$count" == "$prev" ]]; then
      stable=$((stable + 1)); (( stable >= 3 )) && break
    else
      stable=0
    fi
    prev="$count"
  done
}

echo "==> fresh sim dirs under $SIM_ROOT"
rm -rf "$SIM_ROOT"; mkdir -p "$OUT_DIR"

echo "==> starting Kafka + Redis (docker compose up -d)"
docker compose up -d zookeeper kafka redis

echo "==> waiting for Kafka to be healthy"
for _ in $(seq 1 60); do
  [[ "$(docker compose ps kafka --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && break
  sleep 3
done
[[ "$(docker compose ps kafka --format '{{.Health}}')" == "healthy" ]] || { echo "Kafka not healthy"; exit 1; }

if [[ ! -f services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar ]]; then
  echo "==> building Spark job jar"; (cd services/spark-streaming-job && sbt assembly)
fi

echo "==> producing segment-attributed events"
NUM_USERS="${NUM_USERS:-800}" NUM_SLATES="${NUM_SLATES:-20000}" \
  python services/python-modeling/segment_producer.py

echo "==> running OnlineJoiner to drain into Parquet"
SPARK_MAIN_CLASS=com.demo.process.OnlineJoinerStreamingJob \
SPARK_CHECKPOINT_LOCATION="$SIM_ROOT/oj-ckpt" \
ONLINE_JOINER_HDFS_OUTPUT_PATH="$OUT_DIR" \
KAFKA_STARTING_OFFSETS=earliest \
EVENT_WATERMARK_DELAY="3650 days" \
MAX_OFFSETS_PER_TRIGGER="${MAX_OFFSETS_PER_TRIGGER:-1000000}" \
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-2 seconds}" \
  ./run-streaming-job.sh >"$SIM_ROOT/online-joiner.log" 2>&1 &
JOB_PID=$!
trap 'kill "$JOB_PID" 2>/dev/null || true' EXIT
wait_stable parquet "find \"$OUT_DIR\" -name '*.parquet' | wc -l"
kill "$JOB_PID" 2>/dev/null || true; trap - EXIT; wait "$JOB_PID" 2>/dev/null || true

echo
echo "==> SEGMENT REPORT"
"$SPARK_HOME/bin/spark-submit" services/python-modeling/segment_report.py --input "$OUT_DIR" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"

echo
echo "==> done. report CSVs under $SIM_ROOT/report-segments ; stop infra with: docker compose down"
