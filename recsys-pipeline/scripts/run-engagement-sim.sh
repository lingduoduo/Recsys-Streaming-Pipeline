#!/usr/bin/env bash
# End-to-end engagement simulation through the real pipeline:
#   docker (Kafka+Redis) → backfill_producer → recsys_events, then BOTH consumers:
#     • OnlineJoinerStreamingJob  → date-partitioned Parquet  (time-series store for the report)
#     • UserEventStreamingJob     → Redis global:item_popularity  (Kafka → Redis path)
# The Parquet output under $SIM_ROOT/training-samples is what EngagementReportJob (Scala) reads.
set -euo pipefail
cd "$(dirname "$0")/.."

SIM_ROOT="${SIM_ROOT:-/tmp/spark-recsys/engagement-sim}"
OUT_DIR="$SIM_ROOT/training-samples"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-360}"   # seconds to wait for a job's backlog to drain

# Poll a numeric probe until it is >0 and unchanged for 3 reads (backlog drained), then return.
#   $1 = label   $2 = shell command that echoes a single integer
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

# Run one streaming job in the background, drain it via a probe, then stop it.
#   $1 = main class   $2 = checkpoint subdir   $3 = label   $4 = probe   $5.. = extra "K=V" env
run_and_drain() {
  local main_class="$1" ckpt="$2" label="$3" probe="$4"; shift 4
  local log="$SIM_ROOT/${label}.log"
  echo "==> running $main_class (log: $log)"
  env "$@" \
    SPARK_MAIN_CLASS="$main_class" \
    SPARK_CHECKPOINT_LOCATION="$SIM_ROOT/$ckpt" \
    KAFKA_STARTING_OFFSETS=earliest \
    EVENT_WATERMARK_DELAY="3650 days" \
    MAX_OFFSETS_PER_TRIGGER="${MAX_OFFSETS_PER_TRIGGER:-500000}" \
    TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-2 seconds}" \
    ./scripts/run-streaming-job.sh >"$log" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' EXIT
  wait_stable "$label" "$probe"
  kill "$pid" 2>/dev/null || true; trap - EXIT
  wait "$pid" 2>/dev/null || true
}

redis_cli() { docker compose exec -T redis redis-cli "$@"; }

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

# This sim produces to recsys_events, a catalog topic, and provisions nothing of its own. Broker
# auto-creation is disabled, so on fresh volumes -- or after another sim's `docker compose down -v`
# -- the topic is absent and backfill_producer dies on "Failed to update metadata". Provisioning
# the catalog is idempotent, so this is safe to repeat.
echo "==> provisioning the Kafka catalog (recsys_events)"
python3 scripts/provision-kafka-topics.py --bootstrap-server localhost:9092 \
  --command-mode docker-compose >/dev/null

if [[ ! -f services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar ]]; then
  echo "==> building Spark job jar"; (cd services/spark-streaming-job && sbt assembly)
fi

echo "==> backfilling synthetic engagement events"
# Hoisted so the report command printed at the end can bound itself to the same window.
BACKFILL_DAYS="${BACKFILL_DAYS:-21}"
BACKFILL_DAYS="$BACKFILL_DAYS" SLATES_PER_HOUR="${SLATES_PER_HOUR:-12}" \
  python services/python-modeling/backfill_producer.py

# Phase A — Kafka → Parquet (the time-series store)
run_and_drain com.demo.process.OnlineJoinerStreamingJob oj-ckpt parquet \
  "find \"$OUT_DIR\" -name '*.parquet' | wc -l" \
  "ONLINE_JOINER_HDFS_OUTPUT_PATH=$OUT_DIR"

# Phase B — Kafka → Redis (global:item_popularity, click counts per item)
run_and_drain com.demo.task.UserEventStreamingJob ue-ckpt redis \
  "redis_cli ZCARD global:item_popularity"

echo
echo "==> VERIFY Redis (global:item_popularity)"
zcard="$(redis_cli ZCARD global:item_popularity | tr -d ' \r')"
# ZRANGE ... WITHSCORES alternates member / score lines → sum the score lines.
total="$(redis_cli ZRANGE global:item_popularity 0 -1 WITHSCORES | awk 'NR%2==0{s+=$1} END{printf "%d", s+0}')"
echo "   distinct items (ZCARD): $zcard"
echo "   total clicks (sum of scores): $total"
echo "   top 10 items by clicks:"
redis_cli ZREVRANGE global:item_popularity 0 9 WITHSCORES | paste - - | sed 's/^/     /'

parts="$(find "$OUT_DIR" -maxdepth 1 -type d -name 'date=*' 2>/dev/null | wc -l | tr -d ' ' || true)"
echo
echo "==> done. $parts date partitions under $OUT_DIR; Redis populated with $zcard items."
# EngagementReportJob defaults to a 30-day window, so a backfill longer than that would be
# silently truncated in the report. Bound the report to whatever this run actually produced.
echo "    report:  SPARK_MAIN_CLASS=com.demo.report.EngagementReportJob ENGAGEMENT_REPORT_INPUT_PATH=$OUT_DIR ENGAGEMENT_REPORT_LOOKBACK_DAYS=$BACKFILL_DAYS ./scripts/run-streaming-job.sh"
echo "    stop:    docker compose down"
