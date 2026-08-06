#!/usr/bin/env bash
# Behavioral user-profile batch pipeline
#
# Builds a versioned Parquet snapshot and publishes run-scoped Redis keys before
# atomically advancing user-profile:v1:active-run.

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${USER_PROFILE_INPUT_PATH:-}" ]]; then
  echo "Error: USER_PROFILE_INPUT_PATH is required." >&2
  echo "Usage: USER_PROFILE_INPUT_PATH=<profile-events-parquet> $0" >&2
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

OUTPUT_PATH="${USER_PROFILE_OUTPUT_PATH:-sampledata/user_profiles}"

echo "=== Behavioral user-profile pipeline ==="
echo "  Input path          : $USER_PROFILE_INPUT_PATH"
echo "  Output path         : $OUTPUT_PATH"
echo "  Redis host          : ${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}"
echo "  Redis key prefix    : ${USER_PROFILE_REDIS_KEY_PREFIX:-user-profile:v1}"
echo "  Redis profile TTL   : ${USER_PROFILE_REDIS_TTL_SECONDS:-86400}"
echo "  Source lookback     : ${USER_PROFILE_SOURCE_LOOKBACK_SECONDS:-2592000} seconds"
echo "  Half life           : ${USER_PROFILE_HALF_LIFE_SECONDS:-604800} seconds"
echo "  Preference limits   : genres=${USER_PROFILE_MAX_GENRES:-10}, tags=${USER_PROFILE_MAX_TAGS:-20}"
echo ""

exec "$SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --class com.demo.profile.UserBehaviorProfileBatchJob \
  "$JAR" \
  "$USER_PROFILE_INPUT_PATH" \
  "$OUTPUT_PATH"
