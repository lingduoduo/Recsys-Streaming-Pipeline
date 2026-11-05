package com.demo.report

import org.apache.spark.sql.SparkSession
import ReportSupport._

object OffPolicyEvaluationReportJob {
  private def formatted(value: Any, signed: Boolean = false): String = {
    if (value == null) "N/A"
    else if (signed) f"${number(value)}%+.4f"
    else f"${number(value)}%.4f"
  }

  private def interval(row: Record, low: String, high: String, signed: Boolean = false): String = {
    if (row(low) == null || row(high) == null) "N/A"
    else s"[${formatted(row(low), signed)}, ${formatted(row(high), signed)}]"
  }

  def main(args: Array[String]): Unit = {
    val o = options(args)
    require(o.keySet.subsetOf(Set("key", "parquet", "output", "limit", "bootstrap-samples", "bootstrap-seed", "help")), "unknown option")
    if (o.contains("help")) {
      println("--key replay:recommendations --parquet PATH --output CSV --limit -1 --bootstrap-samples 1000 --bootstrap-seed 20260716")
      return
    }
    val samples = o.getOrElse("bootstrap-samples", "1000").toInt
    require(samples >= 0, "--bootstrap-samples must be nonnegative")
    val spark = SparkSession.builder.appName("OffPolicyEvaluationReport").getOrCreate()
    try {
      val events = o.get("parquet").map(records(spark, _)).getOrElse(replay(
        sys.env.getOrElse("REDIS_HOST", "localhost"),
        sys.env.getOrElse("REDIS_PORT", "6379").toInt,
        o.getOrElse("key", "replay:recommendations"), o.getOrElse("limit", "-1").toInt))
      val report = EvaluationReports.ope(events, samples, o.getOrElse("bootstrap-seed", "20260716").toLong)
      require(report.nonEmpty, "no feedback-completed replay events (with reward) — nothing to evaluate")
      val rows = seq(report("rows")).map(obj)
      val calibration = obj(report("calibration"))
      println(s"reward estimator calibration: auc=${text(calibration("auc"), "N/A")} " +
        s"mse=${text(calibration("mse"), "N/A")} (n_test=${calibration("n_test")})")
      println(report("headline"))
      println("95% event-bootstrap CIs conditional on fixed reward model; JVM seeded draws differ from NumPy PCG64")
      rows.foreach { row =>
        println(f"  ${text(row("policy"))}%-16s value=${formatted(row("value"))} " +
          s"95% CI=${interval(row, "value_ci_low", "value_ci_high")} " +
          s"lift=${formatted(row("lift_vs_logging"), signed = true)} " +
          s"95% lift CI=${interval(row, "lift_ci_low", "lift_ci_high", signed = true)} n=${row("n_events")}")
      }
      o.get("output").foreach { output =>
        writeCsv(output, Seq("policy", "value", "lift_vs_logging", "n_events", "estimator_auc", "estimator_mse",
          "value_ci_low", "value_ci_high", "lift_ci_low", "lift_ci_high"), rows)
        println(s"wrote $output")
      }
    } finally spark.stop()
  }
}
