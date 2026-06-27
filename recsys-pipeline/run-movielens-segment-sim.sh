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
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-600}"
# Unique per-run topics so each run reads only its own data (avoids the async topic-delete race
# that otherwise lets a previous run's messages accumulate and starve the drain).
RUN_ID="${RUN_ID:-r$(date +%s)}"
RECSYS_TOPIC="recsys_events_${RUN_ID}"
CONTEXT_TOPIC="movielens_context_${RUN_ID}"
NUM_USERS="${NUM_USERS:-800}"
NUM_SLATES="${NUM_SLATES:-20000}"

# Drain by waiting until a probe reaches `target` (>0), or — when target=0 — until it is >0 and
# unchanged for 3 reads. The target form is needed for the collector: it writes ~NUM_USERS keys
# in one batch, but startup + per-user Redis round-trips create gaps that a "stable" check would
# mistake for completion (killing it after only a handful of keys).
drain() {  # $1=label  $2=probe(echoes an int)  $3=target(0=stable)
  local label="$1" probe="$2" target="$3" prev=-1 stable=0 waited=0 count
  while (( waited < DRAIN_TIMEOUT )); do
    sleep 6; waited=$((waited + 6))
    count="$(eval "$probe" 2>/dev/null | tr -d ' \r' || true)"; count="${count:-0}"
    [[ "$count" =~ ^[0-9]+$ ]] || count=0
    echo "   [$label] t=${waited}s count=$count${target:+/$target}"
    if (( target > 0 )); then
      (( count >= target )) && break
    elif (( count > 0 && count == prev )); then
      stable=$((stable + 1)); (( stable >= 3 )) && break
    else
      stable=0
    fi
    prev="$count"
  done
}

run_and_drain() {  # $1=class $2=ckpt $3=label $4=probe $5=target ; remaining: K=V env
  local cls="$1" ckpt="$2" label="$3" probe="$4" target="$5"; shift 5
  echo "==> running $cls"
  env "$@" SPARK_MAIN_CLASS="$cls" SPARK_CHECKPOINT_LOCATION="$SIM_ROOT/$ckpt" \
    KAFKA_STARTING_OFFSETS=earliest EVENT_WATERMARK_DELAY="3650 days" \
    MAX_OFFSETS_PER_TRIGGER="${MAX_OFFSETS_PER_TRIGGER:-1000000}" \
    TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-2 seconds}" \
    ./run-streaming-job.sh >"$SIM_ROOT/${label}.log" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' EXIT
  drain "$label" "$probe" "$target"
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

# Flush Redis (user:{id}:features are keyed by user_id and shared across runs) so the drain's
# key-count heuristic starts from zero. Topics are unique per run, so no topic cleanup needed.
echo "==> clean slate: flush Redis; per-run topics $RECSYS_TOPIC / $CONTEXT_TOPIC"
docker compose exec -T redis redis-cli FLUSHALL >/dev/null 2>&1 || true

echo "==> building Spark job jar (picks up the collector offset change)"
(cd services/spark-streaming-job && sbt -error assembly)

echo "==> producing demographics ($CONTEXT_TOPIC) + behavior ($RECSYS_TOPIC)"
NUM_USERS="$NUM_USERS" NUM_SLATES="$NUM_SLATES" \
RECSYS_TOPIC="$RECSYS_TOPIC" MOVIELENS_CONTEXT_TOPIC="$CONTEXT_TOPIC" \
  python services/python-modeling/movielens_segment_producer.py

# Demographics → Redis user:{id}:features  (wait until all NUM_USERS keys are written)
run_and_drain com.demo.process.MovieLensContextCollectorStreamingJob ctx-ckpt redis \
  "redis_cli --scan --pattern 'user:*:features' | wc -l" "$NUM_USERS" \
  "MOVIELENS_CONTEXT_INPUT_TOPIC=$CONTEXT_TOPIC"
# Engagement → Parquet (stable file count)
run_and_drain com.demo.process.OnlineJoinerStreamingJob oj-ckpt parquet \
  "find \"$OUT_DIR\" -name '*.parquet' | wc -l" 0 \
  "ONLINE_JOINER_HDFS_OUTPUT_PATH=$OUT_DIR" "ONLINE_JOINER_INPUT_TOPIC=$RECSYS_TOPIC"

echo
echo "==> SEGMENT REPORT (Parquet engagement ⨝ Redis demographics)"
REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit" \
  services/python-modeling/movielens_segment_report.py --input "$OUT_DIR" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"

echo
echo "==> done. CSVs under $SIM_ROOT/report-segments ; stop infra with: docker compose down"
