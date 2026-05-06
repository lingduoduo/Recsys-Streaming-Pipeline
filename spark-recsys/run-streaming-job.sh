#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || echo "")}"
SPARK_HOME="${SPARK_HOME:-}"
SPARK_SUBMIT=""
if [[ -n "$SPARK_HOME" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
fi

if [[ -z "$SPARK_SUBMIT" ]]; then
  if command -v spark-submit >/dev/null 2>&1; then
    SPARK_SUBMIT="$(command -v spark-submit)"
  else
    echo "spark-submit not found. Set SPARK_HOME or add spark-submit to PATH." >&2
    exit 127
  fi
fi

if [[ ! -f spark-streaming-job/target/scala-2.12/spark-recsys-job.jar ]]; then
  echo "Missing Spark job jar. Run: cd spark-streaming-job && sbt assembly" >&2
  exit 127
fi

exec "$SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --class com.demo.streaming.UserEventStreamingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
