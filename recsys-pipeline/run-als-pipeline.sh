#!/usr/bin/env bash
# ALS embedding pipeline
#
# Flow: ratings.csv ──► AlsEmbeddingTrainingJob
#                            ├──► sampledata/als/userFactors/
#                            ├──► sampledata/als/itemFactors/
#                            ├──► Redis alsUserEmb:{userId}    (if ALS_SAVE_TO_REDIS=true)
#                            └──► Redis alsItemEmb:{movieId}   (if ALS_SAVE_TO_REDIS=true)
#
# Required:
#   RATINGS_INPUT_PATH   Path to ratings CSV (userId,movieId,rating,timestamp)
#
# Optional (model):
#   ALS_EMBEDDING_OUTPUT_PATH   Output directory  (default: sampledata/als)
#   ALS_RANK                    Latent factor dimension (default: 16)
#   ALS_MAX_ITER                ALS iterations (default: 10)
#   ALS_REG_PARAM               Regularisation lambda (default: 0.1)
#
# Optional (Redis — set ALS_SAVE_TO_REDIS=true to enable):
#   ALS_SAVE_TO_REDIS           true | false (default: false)
#   REDIS_HOST                  Redis hostname (default: localhost)
#   REDIS_PORT                  Redis port (default: 6379)
#   ALS_USER_REDIS_KEY_PREFIX   Key prefix for user factors (default: alsUserEmb)
#   ALS_ITEM_REDIS_KEY_PREFIX   Key prefix for item factors (default: alsItemEmb)
#   ALS_REDIS_TTL_SECONDS       TTL in seconds (default: 86400)
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

OUTPUT_PATH="${ALS_EMBEDDING_OUTPUT_PATH:-sampledata/als}"

echo "=== ALS embedding pipeline ==="
echo "  Ratings input : $RATINGS_INPUT_PATH"
echo "  Output path   : $OUTPUT_PATH"
echo "  Rank          : ${ALS_RANK:-16}"
echo "  Max iter      : ${ALS_MAX_ITER:-10}"
echo "  Reg param     : ${ALS_REG_PARAM:-0.1}"
echo "  Save to Redis : ${ALS_SAVE_TO_REDIS:-false}"
if [[ "${ALS_SAVE_TO_REDIS:-false}" == "true" ]]; then
  echo "  Redis host    : ${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}"
  echo "  User key      : ${ALS_USER_REDIS_KEY_PREFIX:-alsUserEmb}:{userId}"
  echo "  Item key      : ${ALS_ITEM_REDIS_KEY_PREFIX:-alsItemEmb}:{movieId}"
fi
echo ""

exec "$SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --class com.demo.task.AlsEmbeddingTrainingJob \
  "$JAR" \
  "$RATINGS_INPUT_PATH" \
  "$OUTPUT_PATH"
