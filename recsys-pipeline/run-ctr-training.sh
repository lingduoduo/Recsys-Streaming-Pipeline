#!/usr/bin/env bash
# CTR/ranking batch trainer
#
# Reads the OnlineJoiner Parquet training-samples store and trains a
# click-probability model (offline only; no serving changes).
#
# Flow: training-samples/date=*/  ──► CtrRankingModelTrainingJob
#                                        ├──► Spark ML model dir
#                                        └──► metrics.json (AUC / PR-AUC / logloss)
#
# Env vars:
#   CTR_INPUT_PATH           default /tmp/spark-recsys/training-samples
#   CTR_MODEL_OUTPUT_PATH    default /tmp/spark-recsys/ctr-model
#   CTR_METRICS_OUTPUT_PATH  default <model>/metrics.json
#   CTR_HOLDOUT_DAYS         default 1
#   CTR_ALGORITHM            logreg | gbt   (default logreg)
#   CTR_LABEL_MODE           positive | click  (default positive)
#   CTR_NUM_FEATURES         default 262144
#   SPARK_HOME, SPARK_MASTER (default local[*])
#
# Example:
#   CTR_INPUT_PATH=/tmp/spark-recsys/training-samples ./run-ctr-training.sh
set -euo pipefail

cd "$(dirname "$0")"

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || echo "")}"

SPARK_SUBMIT=""
if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT="$(command -v spark-submit)"
else
  echo "Error: spark-submit not found. Set SPARK_HOME or add spark-submit to PATH." >&2
  exit 1
fi

JAR="services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Missing Spark job jar. Run: cd services/spark-streaming-job && sbt assembly" >&2
  exit 127
fi

"$SPARK_SUBMIT" \
  --class com.demo.task.CtrRankingModelTrainingJob \
  --master "${SPARK_MASTER:-local[*]}" \
  "$JAR"
