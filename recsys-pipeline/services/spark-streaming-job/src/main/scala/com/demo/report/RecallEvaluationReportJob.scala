package com.demo.report

import org.apache.spark.sql.SparkSession
import ReportSupport._

object RecallEvaluationReportJob {
  def main(args: Array[String]): Unit = {
    val o = options(args)
    require(o.keySet.subsetOf(Set("input", "outdir", "ks", "help")), "unknown option")
    if (o.contains("help")) {
      println("--input PATH --outdir PATH --ks 5,10,20 (Redis: REDIS_HOST / REDIS_PORT)")
      return
    }
    val input = o.getOrElse("input", "/tmp/spark-recsys/movie-category-sim/training-samples")
    val host = sys.env.getOrElse("REDIS_HOST", "localhost")
    val port = sys.env.getOrElse("REDIS_PORT", "6379").toInt
    val spark = SparkSession.builder.appName("RecallEvaluationReport").getOrCreate()
    try {
      val movies = catalog(host, port)
      if (movies.isEmpty) {
        println("no movie corpus in Redis (movie:*:features) — cannot evaluate; exiting")
        return
      }
      val rows = EvaluationReports.recall(
        records(spark, input), movies, embeddings(host, port, "i2vEmb"),
        o.getOrElse("ks", "5,10,20").split(",").toSeq.map(_.toInt))
      val output = o.getOrElse("outdir", s"$input/../report-recall-eval") + "/recall_eval.csv"
      writeCsv(output, Seq("method", "k", "recall_at_k", "hitrate_at_k", "users_evaluated", "instances"), rows)
      rows.foreach { row =>
        println(f"  ${text(row("method"))}%-9s @${number(row("k")).toInt}%-3d " +
          f"recall=${number(row("recall_at_k"))}%.4f hitrate=${number(row("hitrate_at_k"))}%.4f " +
          s"(users=${row("users_evaluated")}, instances=${row("instances")})")
      }
      println(s"wrote $output")
    } finally spark.stop()
  }
}
