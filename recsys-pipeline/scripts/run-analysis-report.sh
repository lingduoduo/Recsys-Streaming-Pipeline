#!/usr/bin/env bash
# Finite Scala reports; all remaining arguments are passed unchanged to the job.
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1:-}" in
  recall) MAIN_CLASS=com.demo.report.RecallEvaluationReportJob ;;
  ranking) MAIN_CLASS=com.demo.report.RankingEvaluationReportJob ;;
  ope) MAIN_CLASS=com.demo.report.OffPolicyEvaluationReportJob ;;
  dashboard) MAIN_CLASS=com.demo.report.AnalysisDashboardExportJob ;;
  --help|-h)
    echo "Usage: $0 {recall|ranking|ope|dashboard} [job options]"
    echo "Use <report> --help for job options. Requires the Spark 3.5.1 assembly."
    exit 0 ;;
  *) echo "Usage: $0 {recall|ranking|ope|dashboard} [job options]" >&2; exit 2 ;;
esac
shift

if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  REPORT_SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  REPORT_SPARK_SUBMIT="$(command -v spark-submit)"
else
  echo "spark-submit not found. Set SPARK_HOME to Spark 3.5.1." >&2
  exit 127
fi

REPORT_JAR=services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
if [[ ! -s "$REPORT_JAR" ]]; then
  echo "Missing Spark job jar. Run: (cd services/spark-streaming-job && sbt assembly)" >&2
  exit 127
fi

exec "$REPORT_SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --class "$MAIN_CLASS" "$REPORT_JAR" "$@"
