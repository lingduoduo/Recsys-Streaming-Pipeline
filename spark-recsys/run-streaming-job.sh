#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17)}"
SPARK_HOME="${SPARK_HOME:-/Users/linghuang/opt/spark-3.5.1-bin-hadoop3}"
SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"

if [[ ! -x "$SPARK_SUBMIT" ]]; then
  if command -v spark-submit >/dev/null 2>&1; then
    SPARK_SUBMIT="$(command -v spark-submit)"
  else
    echo "spark-submit was not found. Install Apache Spark 3.5.x or set SPARK_HOME." >&2
    exit 127
  fi
fi

if [[ ! -f spark-streaming-job/target/scala-2.12/spark-recsys-job.jar ]]; then
  echo "Missing Spark job jar. Run: cd spark-streaming-job && sbt assembly" >&2
  exit 127
fi

exec "$SPARK_SUBMIT" \
  --class com.demo.streaming.UserEventStreamingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
