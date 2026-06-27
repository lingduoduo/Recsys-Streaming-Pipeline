#!/usr/bin/env bash
# MovieLens-aligned user-segment simulation through the REAL pipeline paths:
#   docker (Kafka+Redis) → movielens_segment_producer
#     → movielens_context → MovieLensContextCollectorStreamingJob → Redis user:{id}:features
#     → recsys_events     → OnlineJoinerStreamingJob              → Parquet (engagement)
#   → movielens_segment_report.py joins Parquet engagement with Redis demographics.
set -euo pipefail
cd "$(dirname "$0")"

SIM_ROOT="${SIM_ROOT:-/tmp/spark-recsys/movielens-segment-sim}"
OUT_DIR="$SIM_ROOT/training-samples"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-360}"

wait_stable() {  # $1=label  $2=probe(echoes an int)
  local label="$1" probe="$2" prev=-1 stable=0 waited=0 count
  while (( waited < DRAIN_TIMEOUT )); do
    sleep 6; waited=$((waited + 6))
    count="$(eval "$probe" 2>/dev/null | tr -d ' \r' || true)"; count="${count:-0}"
    echo "   [$label] t=${waited}s count=$count"
    if [[ "$count" =~ ^[0-9]+$ && "$count" -gt 0 && "$count" == "$prev" ]]; then
      stable=$((stable + 1)); (( stable >= 3 )) && break
    else stable=0; fi
    prev="$count"
  done
}

run_and_drain() {  # $1=class $2=ckpt $3=label $4=probe ; remaining: K=V env
  local cls="$1" ckpt="$2" label="$3" probe="$4"; shift 4
  echo "==> running $cls"
  env "$@" SPARK_MAIN_CLASS="$cls" SPARK_CHECKPOINT_LOCATION="$SIM_ROOT/$ckpt" \
    KAFKA_STARTING_OFFSETS=earliest EVENT_WATERMARK_DELAY="3650 days" \
    MAX_OFFSETS_PER_TRIGGER="${MAX_OFFSETS_PER_TRIGGER:-1000000}" \
    TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-2 seconds}" \
    ./run-streaming-job.sh >"$SIM_ROOT/${label}.log" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' EXIT
  wait_stable "$label" "$probe"
  kill "$pid" 2>/dev/null || true; trap - EXIT; wait "$pid" 2>/dev/null || true
}

redis_cli() { docker compose exec -T redis redis-cli "$@"; }

echo "==> fresh sim dirs under $SIM_ROOT"; rm -rf "$SIM_ROOT"; mkdir -p "$OUT_DIR"

echo "==> starting Kafka + Redis"; docker compose up -d zookeeper kafka redis
echo "==> waiting for Kafka to be healthy"
for _ in $(seq 1 60); do
  [[ "$(docker compose ps kafka --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && break; sleep 3
done
[[ "$(docker compose ps kafka --format '{{.Health}}')" == "healthy" ]] || { echo "Kafka not healthy"; exit 1; }

echo "==> building Spark job jar (picks up the collector offset change)"
(cd services/spark-streaming-job && sbt -error assembly)

echo "==> producing demographics (movielens_context) + behavior (recsys_events)"
NUM_USERS="${NUM_USERS:-800}" NUM_SLATES="${NUM_SLATES:-20000}" \
  python services/python-modeling/movielens_segment_producer.py

# Demographics → Redis user:{id}:features  (count the keys until stable)
run_and_drain com.demo.process.MovieLensContextCollectorStreamingJob ctx-ckpt redis \
  "redis_cli --scan --pattern 'user:*:features' | wc -l"
# Engagement → Parquet
run_and_drain com.demo.process.OnlineJoinerStreamingJob oj-ckpt parquet \
  "find \"$OUT_DIR\" -name '*.parquet' | wc -l" \
  "ONLINE_JOINER_HDFS_OUTPUT_PATH=$OUT_DIR"

echo
echo "==> SEGMENT REPORT (Parquet engagement ⨝ Redis demographics)"
REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit" \
  services/python-modeling/movielens_segment_report.py --input "$OUT_DIR" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"

echo
echo "==> done. CSVs under $SIM_ROOT/report-segments ; stop infra with: docker compose down"
