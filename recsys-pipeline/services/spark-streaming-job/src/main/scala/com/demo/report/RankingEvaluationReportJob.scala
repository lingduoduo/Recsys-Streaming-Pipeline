package com.demo.report

import org.apache.spark.sql.SparkSession
import ReportSupport._

object RankingEvaluationReportJob {
  def main(args: Array[String]): Unit = {
    val o = options(args)
    require(o.keySet.subsetOf(Set("input", "outdir", "help")), "unknown option")
    if (o.contains("help")) {
      println("--input PATH --outdir PATH (Redis: REDIS_HOST / REDIS_PORT)")
      return
    }
    val input = o.getOrElse("input", "/tmp/spark-recsys/movie-category-sim/training-samples")
    val host = sys.env.getOrElse("REDIS_HOST", "localhost")
    val port = sys.env.getOrElse("REDIS_PORT", "6379").toInt
    val spark = SparkSession.builder.appName("RankingEvaluationReport").getOrCreate()
    try {
      def optional[A](fallback: A)(fetch: => A): A = try fetch catch {
        case scala.util.control.NonFatal(e) =>
          println("[warn] Redis fetch failed: " + e.getMessage)
          fallback
      }
      val rows = EvaluationReports.ranking(
        records(spark, input),
        optional(Map.empty[String, Double])(popularity(host, port)),
        optional(Map.empty[String, Seq[Double]])(embeddings(host, port, "uEmb")),
        optional(Map.empty[String, Seq[Double]])(embeddings(host, port, "i2vEmb")))
      val output = o.getOrElse("outdir", s"$input/../report-ranking-eval") + "/ranking_eval.csv"
      writeCsv(output, Seq("signal", "n", "positives", "coverage", "auc", "logloss"), rows)
      rows.foreach { row =>
        val auc = text(row("auc"), "N/A")
        val loss = text(row("logloss"), "N/A")
        println(f"  ${text(row("signal"))}%-11s auc=$auc logloss=$loss " +
          s"(n=${row("n")}, pos=${row("positives")}, coverage=${row("coverage")})")
      }
      println(s"wrote $output")
    } finally spark.stop()
  }
}
