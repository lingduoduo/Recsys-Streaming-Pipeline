#!/usr/bin/env bash
# Offline embedding pipeline
#
# Flow: ratings.csv ──► ItemSequencePreprocessingJob
#                            └──► Item2VecTrainingJob ──► embedding.txt
#                                                    └──► Redis i2vEmb:{itemId}
#
# Required:
#   RATINGS_INPUT_PATH   Path to ratings CSV (userId,movieId,rating,timestamp)
#
# Optional (embeddings):
#   ITEM2VEC_EMBEDDING_PATH       Output text file  (default: sampledata/item_embedding.txt)
#   ITEM2VEC_QUERY_ITEM           Item ID to print synonyms for at the end (default: 592)
#   ITEM2VEC_MIN_COUNT            Minimum item frequency to train (default: 1)
#   ITEM2VEC_VECTOR_SIZE          Embedding dimensions (default: 10)
#   ITEM2VEC_WINDOW_SIZE          Word2Vec context window (default: 5)
#   ITEM2VEC_NUM_ITERATIONS       Training iterations (default: 10)
#   ITEM2VEC_MIN_RATING           Minimum rating to include (default: 3.5)
#
# Optional (Redis — set ITEM2VEC_SAVE_TO_REDIS=true to enable):
#   ITEM2VEC_SAVE_TO_REDIS        true | false (default: false)
#   REDIS_HOST                    Redis hostname (default: localhost)
#   REDIS_PORT                    Redis port (default: 6379)
#   ITEM2VEC_REDIS_KEY_PREFIX     Key prefix written as {prefix}:{itemId} (default: i2vEmb)
#   ITEM2VEC_REDIS_TTL_SECONDS    TTL in seconds (default: 86400)
#
# Optional (Spark):
#   SPARK_HOME              Path to Spark installation
#   SPARK_MASTER            Spark master URL (default: local[*])
#   SPARK_DRIVER_MEMORY     Driver heap (default: 1g)
#   SPARK_EXECUTOR_MEMORY   Executor heap (default: 2g)
#   SPARK_SQL_SHUFFLE_PARTITIONS  (default: 4)
#
# Example — write to file only:
#   RATINGS_INPUT_PATH=sampledata/ratings.csv ./run-offline-pipeline.sh
#
# Example — write to file + Redis:
#   RATINGS_INPUT_PATH=sampledata/ratings.csv \
#   ITEM2VEC_SAVE_TO_REDIS=true \
#   REDIS_HOST=localhost \
#   ./run-offline-pipeline.sh

set -euo pipefail

cd "$(dirname "$0")"

if [[ -z "${RATINGS_INPUT_PATH:-}" ]]; then
  echo "Error: RATINGS_INPUT_PATH is required." >&2
  echo "Usage: RATINGS_INPUT_PATH=<path/to/ratings.csv> $0" >&2
  exit 1
fi

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || echo "")}"

SPARK_SUBMIT=""
if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT="$(command -v spark-submit)"
else
  echo "spark-submit not found. Set SPARK_HOME or add spark-submit to PATH." >&2
  exit 127
fi

JAR="services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Missing Spark job jar. Run: cd services/spark-streaming-job && sbt assembly" >&2
  exit 127
fi

EMBEDDING_PATH="${ITEM2VEC_EMBEDDING_PATH:-sampledata/item_embedding.txt}"
QUERY_ITEM="${ITEM2VEC_QUERY_ITEM:-592}"

echo "=== Offline embedding pipeline ==="
echo "  Ratings input : $RATINGS_INPUT_PATH"
echo "  Embedding file: $EMBEDDING_PATH"
echo "  Save to Redis : ${ITEM2VEC_SAVE_TO_REDIS:-false}"
if [[ "${ITEM2VEC_SAVE_TO_REDIS:-false}" == "true" ]]; then
  echo "  Redis host    : ${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}"
  echo "  Redis key     : ${ITEM2VEC_REDIS_KEY_PREFIX:-i2vEmb}:{itemId}"
fi
echo ""

exec "$SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --class com.demo.task.Item2VecTrainingJob \
  "$JAR" \
  "$RATINGS_INPUT_PATH" \
  "$EMBEDDING_PATH" \
  "$QUERY_ITEM"
