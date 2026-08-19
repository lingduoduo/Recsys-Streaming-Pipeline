#!/usr/bin/env bash
# Movie-category engagement simulation through the REAL pipeline paths:
#   docker (Kafka+Redis) → movie_segment_producer
#     → movielens_context → MovieLensContextCollectorStreamingJob → Redis movie:{id}:features
#     → recsys_events     → OnlineJoinerStreamingJob              → Parquet (engagement)
#   → MovieCategoryReportJob (Scala) joins Parquet engagement with Redis movie categories (l1/l2/l3).
set -euo pipefail
cd "$(dirname "$0")/.."

SIM_ROOT="${SIM_ROOT:-/tmp/spark-recsys/movie-category-sim}"
OUT_DIR="$SIM_ROOT/training-samples"
SLATE_DIR="$SIM_ROOT/slates"
LIVE_METRICS="$SIM_ROOT/live-metrics.json"
SERVICE_PORT="${RETRIEVAL_SERVICE_PORT:-8080}"
BURST_REQUESTS="${MEASUREMENT_BURST_REQUESTS:-50}"
RUN_ID="${RUN_ID:-r$(date +%s)}"
RECSYS_TOPIC="recsys_events_${RUN_ID}"
CONTEXT_TOPIC="movielens_context_${RUN_ID}"
SAMPLES_TOPIC="training_samples_${RUN_ID}"
SLATES_TOPIC="training_experiences_${RUN_ID}"
NUM_ITEMS="${NUM_ITEMS:-400}"
NUM_SLATES="${NUM_SLATES:-20000}"
# Embeddings (Item2Vec + user) share the sim's movie_*/user_* ids so Recall/Ranking can score them.
# Set GENERATE_EMBEDDINGS=false for a faster category-only run.
GENERATE_EMBEDDINGS="${GENERATE_EMBEDDINGS:-true}"
RATINGS_CSV="$SIM_ROOT/ratings.csv"
ITEM_EMB_FILE="$SIM_ROOT/item-embedding.txt"
USER_EMB_OUT="$SIM_ROOT/user-embedding"
QUERY_ITEM="${ITEM2VEC_QUERY_ITEM:-movie_1}"

# The floor moves with FEEDBACK_DELAY_SCALE (default 150s * scale, minimum 10s) so compressing
# the producer's feedback tail via FEEDBACK_DELAY_SCALE also compresses how long the drain
# blocks for it — bash has no floating-point arithmetic, hence awk. An explicit
# FEEDBACK_TAIL_SECONDS still overrides this default.
FEEDBACK_DELAY_SCALE="${FEEDBACK_DELAY_SCALE:-1.0}"
FEEDBACK_TAIL_SECONDS="${FEEDBACK_TAIL_SECONDS:-$(awk -v scale="$FEEDBACK_DELAY_SCALE" \
  'BEGIN { v = 150 * scale; if (v < 10) v = 10; printf "%d", v }')}"

# An idle stream produces no micro-batches, so only the arriving feedback itself can close a
# window — the wall-clock arm cannot drain a stopped producer. The wait is therefore tied to
# movie_segment_producer's maximum feedback delay — thumb_up/thumb_down at up to 180s, the
# largest of impression/click/thumb/abandon/order — plus margin so a thumb landing near the true
# maximum doesn't race the window's close (190s * scale): by the time the last impression's
# deadline is reached, feedback is still arriving to close it, and nothing is stranded.
FEEDBACK_JOIN_WAIT_SECONDS="${FEEDBACK_JOIN_WAIT_SECONDS:-$(awk -v scale="$FEEDBACK_DELAY_SCALE" \
  'BEGIN { v = 190 * scale; if (v < 10) v = 10; printf "%d", v }')}"
# The parquet drain must outlast the last order's arrival *and* the window the joiner then holds
# it for, plus one trigger interval for the publishing batch itself.
SAMPLE_DRAIN_SECONDS="${SAMPLE_DRAIN_SECONDS:-$((FEEDBACK_TAIL_SECONDS + FEEDBACK_JOIN_WAIT_SECONDS + 10))}"

# The drain must be able to outlast its own floor: below min_wait every iteration continues,
# so a timeout shorter than the floor kills the job before the floor has elapsed.
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-$(( SAMPLE_DRAIN_SECONDS + 300 > 600 ? SAMPLE_DRAIN_SECONDS + 300 : 600 ))}"

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
    else stable=0; fi
    prev="$count"
  done
}

# Never `kill 0` — that signals the whole process group. Guard each pid. Every *_PID
# variable the script assigns must be listed here, for as long as any one of them can
# be running (CTX/OJ concurrently, then POP, then EXP) — an early exit orphans the
# backgrounded Spark process behind whichever pid this trap forgets to guard.
trap 'for p in "${CTX_PID:-}" "${OJ_PID:-}" "${POP_PID:-}" "${EXP_PID:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null; done' EXIT

redis_cli() { docker compose exec -T redis redis-cli "$@"; }

echo "==> fresh sim dirs under $SIM_ROOT"; rm -rf "$SIM_ROOT"; mkdir -p "$OUT_DIR"

echo "==> resetting infra volumes (avoids stale ZooKeeper broker registration)"; docker compose down -v >/dev/null 2>&1 || true
echo "==> starting Kafka + Redis"; docker compose up -d zookeeper kafka redis
echo "==> waiting for Kafka to be healthy"
for _ in $(seq 1 60); do
  [[ "$(docker compose ps kafka --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && break; sleep 3
done
[[ "$(docker compose ps kafka --format '{{.Health}}')" == "healthy" ]] || { echo "Kafka not healthy"; exit 1; }

echo "==> clean slate: flush Redis; per-run topics $RECSYS_TOPIC / $CONTEXT_TOPIC"
docker compose exec -T redis redis-cli FLUSHALL >/dev/null 2>&1 || true

# Create them for real. The broker runs with KAFKA_AUTO_CREATE_TOPICS_ENABLE=false and the
# checked-in catalog covers only recsys_events{,.backfill}, so nothing else brings these into
# existence. Without this every producer and job blocks on metadata that never arrives and dies
# with "KafkaTimeoutError: Failed to update metadata" -- the hazard noted below, but hit by the
# producer rather than the jobs.
for topic in "$RECSYS_TOPIC" "$CONTEXT_TOPIC" "$SAMPLES_TOPIC" "$SLATES_TOPIC"; do
  docker compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 >/dev/null
done

echo "==> building Spark job jar"
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
  "ONLINE_JOINER_OUTPUT_TOPIC=$SAMPLES_TOPIC" \
  "FEEDBACK_JOIN_WAIT=$FEEDBACK_JOIN_WAIT_SECONDS seconds"
OJ_PID="$LAST_JOB_PID"

echo "==> producing movie metadata ($CONTEXT_TOPIC) + behavior ($RECSYS_TOPIC)"
NUM_ITEMS="$NUM_ITEMS" NUM_SLATES="$NUM_SLATES" \
RECSYS_TOPIC="$RECSYS_TOPIC" MOVIELENS_CONTEXT_TOPIC="$CONTEXT_TOPIC" \
RATINGS_OUTPUT_PATH="$RATINGS_CSV" \
  python services/python-modeling/movie_segment_producer.py

# Movie metadata → Redis movie:{id}:features  (wait until all NUM_ITEMS keys are written)
drain redis "redis_cli --scan --pattern 'movie:*:features' | wc -l" "$NUM_ITEMS" 0
stop_job "$CTX_PID"; CTX_PID=""
# Engagement → Parquet (stable file count, held open until the feedback tail lands)
drain parquet "find \"$OUT_DIR\" -name '*.parquet' | wc -l" 0 "$SAMPLE_DRAIN_SECONDS"
stop_job "$OJ_PID"; OJ_PID=""

# Clicks → Redis global:item_popularity  (ranking popularity signal; stable member count)
start_job com.demo.task.UserEventStreamingJob pop-ckpt popularity \
  "KAFKA_TOPIC=$RECSYS_TOPIC"
POP_PID="$LAST_JOB_PID"
drain popularity "redis_cli ZCARD global:item_popularity" 0 0
stop_job "$POP_PID"; POP_PID=""

echo
echo "==> SLATE EXPERIENCES (training_samples → slate Parquet for relevance/diversity)"
start_job com.demo.process.ExperienceCollectorStreamingJob exp-ckpt slates \
  "EXPERIENCE_COLLECTOR_OUTPUT_PATH=$SLATE_DIR" \
  "EXPERIENCE_COLLECTOR_INPUT_TOPIC=$SAMPLES_TOPIC" \
  "EXPERIENCE_COLLECTOR_OUTPUT_TOPIC=$SLATES_TOPIC"
EXP_PID="$LAST_JOB_PID"
drain slates "find \"$SLATE_DIR\" -name '*.parquet' | wc -l" 0 0
stop_job "$EXP_PID"; EXP_PID=""

echo
echo "==> CATEGORY REPORT (Parquet engagement ⨝ Redis movie categories)"
SPARK_MAIN_CLASS=com.demo.report.MovieCategoryReportJob \
MOVIE_CATEGORY_INPUT_PATH="$OUT_DIR" REDIS_HOST=localhost \
  ./scripts/run-streaming-job.sh 2>&1 | grep -vE "INFO|WARN|^[0-9]{2}/"

if [[ "$GENERATE_EMBEDDINGS" == "true" ]]; then
  if [[ ! -s "$RATINGS_CSV" || "$(wc -l < "$RATINGS_CSV")" -le 1 ]]; then
    echo "ERROR: no usable positive interactions in $RATINGS_CSV — cannot train embeddings" >&2
    exit 1
  fi
  echo
  echo "==> ITEM2VEC embeddings (i2vEmb:{movieId}) from $RATINGS_CSV"
  RATINGS_INPUT_PATH="$RATINGS_CSV" ITEM2VEC_EMBEDDING_PATH="$ITEM_EMB_FILE" \
  ITEM2VEC_QUERY_ITEM="$QUERY_ITEM" ITEM2VEC_SAVE_TO_REDIS=true \
  REDIS_HOST=localhost REDIS_PORT=6379 \
    ./scripts/run-offline-pipeline.sh 2>&1 | grep -vE "INFO|WARN|^[0-9]{2}/"

  echo
  echo "==> USER embeddings (uEmb:{userId})"
  RATINGS_INPUT_PATH="$RATINGS_CSV" ITEM2VEC_EMBEDDING_PATH="$ITEM_EMB_FILE" \
  USER_EMBEDDING_OUTPUT_PATH="$USER_EMB_OUT" USER_EMBEDDING_SAVE_TO_REDIS=true \
  REDIS_HOST=localhost REDIS_PORT=6379 \
    ./scripts/run-user-embedding-pipeline.sh 2>&1 | grep -vE "INFO|WARN|^[0-9]{2}/"
fi

echo
echo "==> SERVICE BURST (real /metrics latency, freshness, and filter decisions)"
service_pid=""
kill_service() {
  kill "$service_pid" 2>/dev/null || true
  # spring-boot:run forks its own JVM by default, so the process we backgrounded
  # (even post-exec) may not be the one actually bound to the port. Kill by port too.
  lsof -ti tcp:"$SERVICE_PORT" 2>/dev/null | xargs kill 2>/dev/null || true
}
(cd services/java-retrieval-service && \
  JAVA_HOME="${MEASUREMENT_JAVA_HOME:-$JAVA_HOME}" \
  SERVER_PORT="$SERVICE_PORT" REDIS_HOST=localhost \
  exec mvn -q -DskipTests spring-boot:run >"$SIM_ROOT/service.log" 2>&1) &
service_pid=$!
trap kill_service EXIT
for _ in $(seq 1 40); do
  curl -sf "http://localhost:$SERVICE_PORT/metrics" >/dev/null 2>&1 && break; sleep 3
done

if curl -sf "http://localhost:$SERVICE_PORT/metrics" >/dev/null 2>&1; then
  for i in $(seq 1 "$BURST_REQUESTS"); do
    user="user_$(( (i % 10) + 1 ))"
    # /recommend's "recommendations" field is a List<String> of item ids (see
    # HybridRecommendationService.recommend()), not a list of objects — but stay tolerant of an
    # {"item": ...}-shaped entry too, in case that ever changes. Extraction failures print to
    # stderr (not swallowed) so a broken response shape is visible in the sim log.
    item="$(curl -sf "http://localhost:$SERVICE_PORT/recommend/$user?limit=6" \
      | python3 -c '
import json, sys
item = ""
try:
    recs = json.load(sys.stdin).get("recommendations") or []
    first = recs[0] if recs else None
    if isinstance(first, str):
        item = first
    elif isinstance(first, dict):
        item = first.get("item", "")
    if recs and not item:
        print(f"WARN: could not extract item id from recommendation entry: {first!r}", file=sys.stderr)
except Exception as e:
    print(f"WARN: failed to parse /recommend response: {e}", file=sys.stderr)
print(item)
' || true)"
    if [[ -n "$item" && $(( i % 2 )) -eq 0 ]]; then
      curl -sf -X POST "http://localhost:$SERVICE_PORT/feedback" \
        -H 'Content-Type: application/json' \
        -d "{\"user\":\"$user\",\"item\":\"$item\",\"clicked\":true,\"reward\":1.0,\"rating\":4.5,\"dwellMillis\":12000,\"completionRate\":0.75}" \
        >/dev/null 2>&1 || true
    fi
  done
  curl -sf "http://localhost:$SERVICE_PORT/metrics" > "$LIVE_METRICS" 2>/dev/null || true
  echo "   captured $(wc -c < "$LIVE_METRICS" 2>/dev/null || echo 0) bytes of live metrics"
else
  echo "   service did not start (see $SIM_ROOT/service.log) — latency stays N/A"
fi
kill_service
wait "$service_pid" 2>/dev/null || true
trap - EXIT

echo
echo "==> MDP POLICY EVALUATION (uniform vs greedy over the generated ratings)"
# Optional: needs Maven. The ratings.csv written above clears the evaluator's default
# --min-user-ratings 20 / --min-movie-ratings 10 filters; only the tiny bundled
# sampledata/ratings.csv does not. Failure here must not sink a completed simulation, so the
# card simply stays "Not measured".
MDP_CSV="$SIM_ROOT/mdp_eval.csv"
if command -v mvn >/dev/null 2>&1; then
  # Status must come from mvn, not from a pipeline tail, or the failure branch never fires.
  if (cd services/java-retrieval-service && mvn -q compile exec:java \
        -Dexec.mainClass=com.demo.retrieval.evaluation.MovieLensPolicyEvaluation \
        -Dexec.args="--ratings $RATINGS_CSV --output $MDP_CSV") >"$SIM_ROOT/mdp-eval.log" 2>&1; then
    echo "   wrote $MDP_CSV"
  else
    echo "   evaluator failed (see $SIM_ROOT/mdp-eval.log) — MDP card stays Not measured"
    tail -3 "$SIM_ROOT/mdp-eval.log" | sed 's/^/   /'
  fi
else
  echo "   mvn not found — skipping; MDP card stays Not measured"
fi

echo
echo "==> ANALYSIS DASHBOARD (recall + ranking use Redis embeddings/popularity)"
REDIS_HOST=localhost REDIS_PORT=6379 \
  python services/python-modeling/analysis_dashboard_report.py --input "$OUT_DIR" \
    --mdp-csv "$MDP_CSV" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"

echo
echo "==> REACT DASHBOARD SNAPSHOT (seven measurement sections)"
export_args=(--input "$OUT_DIR" --output "frontend/data/dashboard.json")
[[ -s "$MDP_CSV" ]] && export_args+=(--mdp-csv "$MDP_CSV")
[[ -d "$SLATE_DIR" ]] && export_args+=(--experiences "$SLATE_DIR")
[[ -s "$LIVE_METRICS" ]] && export_args+=(--live-metrics "$LIVE_METRICS")
REDIS_HOST=localhost REDIS_PORT=6379 \
  python3 frontend/export_dashboard_json.py "${export_args[@]}" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"
(cd frontend && npm run validate:data)

echo
echo "==> done. CSVs under $SIM_ROOT/report-categories ; dashboard at $SIM_ROOT/report-dashboard/index.html"
echo "    stop infra with: docker compose down"
