#!/usr/bin/env bash
# Movie-category engagement simulation through the REAL pipeline paths:
#   docker (Kafka+Redis) → movie_segment_producer
#     → movielens_context → MovieLensContextCollectorStreamingJob → Redis movie:{id}:features
#     → recsys_events     → OnlineJoinerStreamingJob              → Parquet (engagement)
#   → MovieCategoryReportJob (Scala) joins Parquet engagement with Redis movie categories (l1/l2/l3).
set -euo pipefail
cd "$(dirname "$0")"

SIM_ROOT="${SIM_ROOT:-/tmp/spark-recsys/movie-category-sim}"
OUT_DIR="$SIM_ROOT/training-samples"
SLATE_DIR="$SIM_ROOT/slates"
LIVE_METRICS="$SIM_ROOT/live-metrics.json"
SERVICE_PORT="${RETRIEVAL_SERVICE_PORT:-8080}"
BURST_REQUESTS="${MEASUREMENT_BURST_REQUESTS:-50}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-600}"
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

# Drain until a probe reaches `target` (>0), or — when target=0 — until it is >0 and unchanged for
# 3 reads. Target form is needed for the collector (writes ~NUM_ITEMS keys in one batch with gaps).
drain() {  # $1=label $2=probe $3=target(0=stable)
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
    else stable=0; fi
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

echo "==> resetting infra volumes (avoids stale ZooKeeper broker registration)"; docker compose down -v >/dev/null 2>&1 || true
echo "==> starting Kafka + Redis"; docker compose up -d zookeeper kafka redis
echo "==> waiting for Kafka to be healthy"
for _ in $(seq 1 60); do
  [[ "$(docker compose ps kafka --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && break; sleep 3
done
[[ "$(docker compose ps kafka --format '{{.Health}}')" == "healthy" ]] || { echo "Kafka not healthy"; exit 1; }

echo "==> clean slate: flush Redis; per-run topics $RECSYS_TOPIC / $CONTEXT_TOPIC"
docker compose exec -T redis redis-cli FLUSHALL >/dev/null 2>&1 || true

echo "==> building Spark job jar"
(cd services/spark-streaming-job && sbt -error assembly)

echo "==> producing movie metadata ($CONTEXT_TOPIC) + behavior ($RECSYS_TOPIC)"
NUM_ITEMS="$NUM_ITEMS" NUM_SLATES="$NUM_SLATES" \
RECSYS_TOPIC="$RECSYS_TOPIC" MOVIELENS_CONTEXT_TOPIC="$CONTEXT_TOPIC" \
RATINGS_OUTPUT_PATH="$RATINGS_CSV" \
  python services/python-modeling/movie_segment_producer.py

# Movie metadata → Redis movie:{id}:features  (wait until all NUM_ITEMS keys are written)
run_and_drain com.demo.process.MovieLensContextCollectorStreamingJob ctx-ckpt redis \
  "redis_cli --scan --pattern 'movie:*:features' | wc -l" "$NUM_ITEMS" \
  "MOVIELENS_CONTEXT_INPUT_TOPIC=$CONTEXT_TOPIC"
# Engagement → Parquet (stable file count)
run_and_drain com.demo.process.OnlineJoinerStreamingJob oj-ckpt parquet \
  "find \"$OUT_DIR\" -name '*.parquet' | wc -l" 0 \
  "ONLINE_JOINER_HDFS_OUTPUT_PATH=$OUT_DIR" "ONLINE_JOINER_INPUT_TOPIC=$RECSYS_TOPIC" \
  "ONLINE_JOINER_OUTPUT_TOPIC=$SAMPLES_TOPIC"
# Clicks → Redis global:item_popularity  (ranking popularity signal; stable member count)
run_and_drain com.demo.task.UserEventStreamingJob pop-ckpt popularity \
  "redis_cli ZCARD global:item_popularity" 0 \
  "KAFKA_TOPIC=$RECSYS_TOPIC"

echo
echo "==> SLATE EXPERIENCES (training_samples → slate Parquet for relevance/diversity)"
run_and_drain com.demo.process.ExperienceCollectorStreamingJob exp-ckpt slates \
  "find \"$SLATE_DIR\" -name '*.parquet' | wc -l" 0 \
  "EXPERIENCE_COLLECTOR_OUTPUT_PATH=$SLATE_DIR" \
  "EXPERIENCE_COLLECTOR_INPUT_TOPIC=$SAMPLES_TOPIC" \
  "EXPERIENCE_COLLECTOR_OUTPUT_TOPIC=$SLATES_TOPIC"

echo
echo "==> CATEGORY REPORT (Parquet engagement ⨝ Redis movie categories)"
SPARK_MAIN_CLASS=com.demo.report.MovieCategoryReportJob \
MOVIE_CATEGORY_INPUT_PATH="$OUT_DIR" REDIS_HOST=localhost \
  ./run-streaming-job.sh 2>&1 | grep -vE "INFO|WARN|^[0-9]{2}/"

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
    ./run-offline-pipeline.sh 2>&1 | grep -vE "INFO|WARN|^[0-9]{2}/"

  echo
  echo "==> USER embeddings (uEmb:{userId})"
  RATINGS_INPUT_PATH="$RATINGS_CSV" ITEM2VEC_EMBEDDING_PATH="$ITEM_EMB_FILE" \
  USER_EMBEDDING_OUTPUT_PATH="$USER_EMB_OUT" USER_EMBEDDING_SAVE_TO_REDIS=true \
  REDIS_HOST=localhost REDIS_PORT=6379 \
    ./run-user-embedding-pipeline.sh 2>&1 | grep -vE "INFO|WARN|^[0-9]{2}/"
fi

echo
echo "==> SERVICE BURST (real /metrics latency, freshness, and filter decisions)"
service_pid=""
(cd services/java-retrieval-service && \
  JAVA_HOME="${MEASUREMENT_JAVA_HOME:-$JAVA_HOME}" \
  SERVER_PORT="$SERVICE_PORT" REDIS_HOST=localhost \
  mvn -q -DskipTests spring-boot:run >"$SIM_ROOT/service.log" 2>&1) &
service_pid=$!
for _ in $(seq 1 40); do
  curl -sf "http://localhost:$SERVICE_PORT/metrics" >/dev/null 2>&1 && break; sleep 3
done

if curl -sf "http://localhost:$SERVICE_PORT/metrics" >/dev/null 2>&1; then
  for i in $(seq 1 "$BURST_REQUESTS"); do
    user="user_$(( (i % 10) + 1 ))"
    item="$(curl -sf "http://localhost:$SERVICE_PORT/recommend/$user?limit=6" \
      | python3 -c 'import json,sys; d=json.load(sys.stdin).get("recommendations") or [{}]; print(d[0].get("item",""))' 2>/dev/null || true)"
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
kill "$service_pid" 2>/dev/null || true
wait "$service_pid" 2>/dev/null || true

echo
echo "==> ANALYSIS DASHBOARD (recall + ranking use Redis embeddings/popularity)"
REDIS_HOST=localhost REDIS_PORT=6379 \
  python services/python-modeling/analysis_dashboard_report.py --input "$OUT_DIR" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"

echo
echo "==> REACT DASHBOARD SNAPSHOT (seven measurement sections)"
export_args=(--input "$OUT_DIR" --output ../frontend/data/dashboard.json)
[[ -d "$SLATE_DIR" ]] && export_args+=(--experiences "$SLATE_DIR")
[[ -s "$LIVE_METRICS" ]] && export_args+=(--live-metrics "$LIVE_METRICS")
REDIS_HOST=localhost REDIS_PORT=6379 \
  python3 ../frontend/export_dashboard_json.py "${export_args[@]}" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"
(cd ../frontend && npm run validate:data)

echo
echo "==> done. CSVs under $SIM_ROOT/report-categories ; dashboard at $SIM_ROOT/report-dashboard/index.html"
echo "    stop infra with: docker compose down"
