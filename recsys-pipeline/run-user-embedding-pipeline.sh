#!/usr/bin/env bash
# User embedding pipeline
#
# Flow: ratings.csv + item_embedding.txt ──► UserEmbeddingTrainingJob
#                                                ├──► sampledata/user_embedding.txt
#                                                └──► Redis uEmb:{userId}   (if USER_EMBEDDING_SAVE_TO_REDIS=true)
#
# Required:
#   RATINGS_INPUT_PATH        Path to ratings CSV (userId,movieId,rating,timestamp)
#   ITEM2VEC_EMBEDDING_PATH   Path to item embedding file produced by Item2VecTrainingJob
#
# Optional (model):
#   USER_EMBEDDING_OUTPUT_PATH   Output text file (default: sampledata/user_embedding.txt)
#   USER_EMBEDDING_MIN_RATING    Minimum rating to include (default: 3.5)
#
# Optional (Redis — set USER_EMBEDDING_SAVE_TO_REDIS=true to enable):
#   USER_EMBEDDING_SAVE_TO_REDIS     true | false (default: false)
#   REDIS_HOST                       Redis hostname (default: localhost)
#   REDIS_PORT                       Redis port (default: 6379)
#   USER_EMBEDDING_REDIS_KEY_PREFIX  Key prefix written as {prefix}:{userId} (default: uEmb)
#   USER_EMBEDDING_REDIS_TTL_SECONDS TTL in seconds (default: 86400)
#
# Optional (Spark):
#   SPARK_HOME              Path to Spark installation
#   SPARK_MASTER            Spark master URL (default: local[*])
#   SPARK_DRIVER_MEMORY     Driver heap (default: 1g)
#   SPARK_EXECUTOR_MEMORY   Executor heap (default: 2g)
#   SPARK_SQL_SHUFFLE_PARTITIONS  (default: 4)

set -euo pipefail

cd "$(dirname "$0")"

if [[ -z "${RATINGS_INPUT_PATH:-}" ]]; then
  echo "Error: RATINGS_INPUT_PATH is required." >&2
  echo "Usage: RATINGS_INPUT_PATH=<path> ITEM2VEC_EMBEDDING_PATH=<path> $0" >&2
  exit 1
fi

if [[ -z "${ITEM2VEC_EMBEDDING_PATH:-}" ]]; then
  echo "Error: ITEM2VEC_EMBEDDING_PATH is required." >&2
  echo "Tip: Run ./run-offline-pipeline.sh first to generate item embeddings." >&2
  exit 1
fi

if [[ ! -f "$ITEM2VEC_EMBEDDING_PATH" ]]; then
  echo "Error: Item embedding file not found: $ITEM2VEC_EMBEDDING_PATH" >&2
  echo "Tip: Run ./run-offline-pipeline.sh first to generate item embeddings." >&2
  exit 1
fi

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

OUTPUT_PATH="${USER_EMBEDDING_OUTPUT_PATH:-sampledata/user_embedding.txt}"

echo "=== User embedding pipeline ==="
echo "  Ratings input    : $RATINGS_INPUT_PATH"
echo "  Item embeddings  : $ITEM2VEC_EMBEDDING_PATH"
echo "  Output file      : $OUTPUT_PATH"
echo "  Min rating       : ${USER_EMBEDDING_MIN_RATING:-3.5}"
echo "  Save to Redis    : ${USER_EMBEDDING_SAVE_TO_REDIS:-false}"
if [[ "${USER_EMBEDDING_SAVE_TO_REDIS:-false}" == "true" ]]; then
  echo "  Redis host       : ${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}"
  echo "  Redis key        : ${USER_EMBEDDING_REDIS_KEY_PREFIX:-uEmb}:{userId}"
fi
echo ""

exec "$SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --class com.demo.task.UserEmbeddingTrainingJob \
  "$JAR" \
  "$RATINGS_INPUT_PATH" \
  "$ITEM2VEC_EMBEDDING_PATH" \
  "$OUTPUT_PATH"
