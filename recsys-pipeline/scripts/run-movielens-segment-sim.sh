#!/usr/bin/env bash
# MovieLens-aligned user-segment simulation through the REAL pipeline paths:
#   docker (Kafka+Redis) → movielens_segment_producer
#     → movielens_context → MovieLensContextCollectorStreamingJob → Redis user:{id}:features
#     → recsys_events     → OnlineJoinerStreamingJob              → Parquet (engagement)
#   → SegmentReportJob (Scala) joins Parquet engagement with Redis demographics.
set -euo pipefail
cd "$(dirname "$0")/.."

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

# The floor moves with FEEDBACK_DELAY_SCALE (default 150s * scale, minimum 10s) so compressing
# the producer's feedback tail via FEEDBACK_DELAY_SCALE also compresses how long the drain
# blocks for it — bash has no floating-point arithmetic, hence awk. An explicit
# FEEDBACK_TAIL_SECONDS still overrides this default.
FEEDBACK_DELAY_SCALE="${FEEDBACK_DELAY_SCALE:-1.0}"
FEEDBACK_TAIL_SECONDS="${FEEDBACK_TAIL_SECONDS:-$(awk -v scale="$FEEDBACK_DELAY_SCALE" \
  'BEGIN { v = 150 * scale; if (v < 10) v = 10; printf "%d", v }')}"

# The joiner holds each slate FEEDBACK_JOIN_WAIT past its impression before publishing its
# sample, so it scales with FEEDBACK_DELAY_SCALE exactly like the producer's tail does.
FEEDBACK_JOIN_WAIT_SECONDS="${FEEDBACK_JOIN_WAIT_SECONDS:-$(awk -v scale="$FEEDBACK_DELAY_SCALE" \
  'BEGIN { v = 180 * scale; if (v < 10) v = 10; printf "%d", v }')}"
# The parquet drain must outlast the last order's arrival *and* the window the joiner then holds
# it for, plus one trigger interval for the publishing batch itself.
SAMPLE_DRAIN_SECONDS="${SAMPLE_DRAIN_SECONDS:-$((FEEDBACK_TAIL_SECONDS + FEEDBACK_JOIN_WAIT_SECONDS + 10))}"

# macOS ships bash 3.2, which has no associative arrays: `declare -A` fails and every
# key silently collapses to index 0, so a pid map would return the wrong process.
# Each caller keeps its own pid variable instead.
start_job() {  # $1=class $2=ckpt $3=label ; remaining: K=V env ; sets LAST_JOB_PID
  local cls="$1" ckpt="$2" label="$3"; shift 3
  echo "==> starting $cls"
  env "$@" SPARK_MAIN_CLASS="$cls" SPARK_CHECKPOINT_LOCATION="$SIM_ROOT/$ckpt" \
    KAFKA_STARTING_OFFSETS=earliest EVENT_WATERMARK_DELAY="3650 days" \
    MAX_OFFSETS_PER_TRIGGER="${MAX_OFFSETS_PER_TRIGGER:-1000000}" \
    TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-2 seconds}" \
    ./scripts/run-streaming-job.sh >"$SIM_ROOT/${label}.log" 2>&1 &
  LAST_JOB_PID=$!
}

stop_job() {  # $1=pid
  local pid="${1:-}"
  [[ -n "$pid" ]] || return 0
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# $4=min_wait: a floor before stability may end the drain. Feedback arrives up to
# FEEDBACK_TAIL_SECONDS after its impression, and stability alone would end the drain
# after three unchanged six-second reads — long before the last order lands.
drain() {  # $1=label $2=probe $3=target $4=min_wait
  local label="$1" probe="$2" target="$3" min_wait="${4:-0}" prev=-1 stable=0 waited=0 count
  while (( waited < DRAIN_TIMEOUT )); do
    sleep 6; waited=$((waited + 6))
    count="$(eval "$probe" 2>/dev/null | tr -d ' \r' || true)"; count="${count:-0}"
    [[ "$count" =~ ^[0-9]+$ ]] || count=0
    echo "   [$label] t=${waited}s count=$count${target:+/$target}"
    if (( waited < min_wait )); then prev="$count"; continue; fi
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

# Never `kill 0` — that signals the whole process group. Guard each pid.
trap 'for p in "${CTX_PID:-}" "${OJ_PID:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null; done' EXIT

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

# Hazard: these jobs now start before the producer sends anything, using per-run topics
# ($RECSYS_TOPIC / $CONTEXT_TOPIC) not in the checked-in catalog. run-streaming-job.sh's
# topic-provisioning guard only runs for UserEventStreamingJob, and Spark's Kafka AdminClient
# does not trigger broker-side topic auto-creation on its own. If a job fails immediately on
# startup here, check whether the topic actually exists on the broker before assuming an
# application bug.
start_job com.demo.process.MovieLensContextCollectorStreamingJob ctx-ckpt redis \
  "MOVIELENS_CONTEXT_INPUT_TOPIC=$CONTEXT_TOPIC"
CTX_PID="$LAST_JOB_PID"
start_job com.demo.process.OnlineJoinerStreamingJob oj-ckpt parquet \
  "ONLINE_JOINER_HDFS_OUTPUT_PATH=$OUT_DIR" "ONLINE_JOINER_INPUT_TOPIC=$RECSYS_TOPIC" \
  "FEEDBACK_JOIN_WAIT=$FEEDBACK_JOIN_WAIT_SECONDS seconds"
OJ_PID="$LAST_JOB_PID"

echo "==> producing demographics ($CONTEXT_TOPIC) + behavior ($RECSYS_TOPIC)"
NUM_USERS="$NUM_USERS" NUM_SLATES="$NUM_SLATES" \
RECSYS_TOPIC="$RECSYS_TOPIC" MOVIELENS_CONTEXT_TOPIC="$CONTEXT_TOPIC" \
  python services/python-modeling/movielens_segment_producer.py

drain redis "redis_cli --scan --pattern 'user:*:features' | wc -l" "$NUM_USERS" 0
stop_job "$CTX_PID"; CTX_PID=""
drain parquet "find \"$OUT_DIR\" -name '*.parquet' | wc -l" 0 "$SAMPLE_DRAIN_SECONDS"
stop_job "$OJ_PID"; OJ_PID=""

echo
echo "==> SEGMENT REPORT (Parquet engagement ⨝ Redis demographics)"
SPARK_MAIN_CLASS=com.demo.report.SegmentReportJob \
SEGMENT_REPORT_INPUT_PATH="$OUT_DIR" REDIS_HOST=localhost \
  ./scripts/run-streaming-job.sh 2>&1 | grep -vE "INFO|WARN|^[0-9]{2}/"

echo
echo "==> done. CSVs under $SIM_ROOT/report-segments ; stop infra with: docker compose down"
