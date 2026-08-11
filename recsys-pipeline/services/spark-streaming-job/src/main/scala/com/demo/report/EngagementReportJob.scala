package com.demo.report

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Engagement time-series report (pure-Spark port of `engagement_report_pyspark.py`).
  *
  * Reads the date-partitioned `training_samples` Parquet (written by
  * [[com.demo.process.OnlineJoinerStreamingJob]]) and computes CTR = avg(clicked) by date /
  * hour-of-day / day-of-week, writing one CSV per breakdown.
  *
  * (The Python version carried two unimplemented "future scope" stubs — STL decomposition and
  * changepoint detection — that would run `statsmodels`/`ruptures` on the tiny collected
  * aggregate. Those belong to the Python stats ecosystem, not Spark, so they are not part of this
  * collection job.)
  *
  * Run through the project's pinned Spark:
  *   "$SPARK_HOME/bin/spark-submit" --class com.demo.report.EngagementReportJob \
  *     spark-recsys-job.jar <parquet-input> [<outdir>]
  */
object EngagementReportJob {

  def main(args: Array[String]): Unit = {
    val input = Env.argOrEnv(args, 0, "ENGAGEMENT_REPORT_INPUT_PATH")
      .getOrElse("/tmp/spark-recsys/engagement-sim/training-samples")
    val outdir = Env.argOrEnv(args, 1, "ENGAGEMENT_REPORT_OUTPUT_PATH")
      .getOrElse(s"$input/../report-engagement")

    val lookbackDays = Env.int("ENGAGEMENT_REPORT_LOOKBACK_DAYS", 30)

    val spark = SparkSessions.create("EngagementReportJob")
    try {
      val df = ReportWindow.withinLookback(spark.read.parquet(input), lookbackDays).cache()
      val d = daily(df); val h = byHour(df); val w = byDow(df)
      writeCsv(d, s"$outdir/ctr_daily")
      writeCsv(h, s"$outdir/ctr_by_hour")
      writeCsv(w, s"$outdir/ctr_by_dow")

      println("=== daily CTR ===");          d.show(50, truncate = false)
      println("=== CTR by hour ===");        h.show(24, truncate = false)
      println("=== CTR by day-of-week ==="); w.show(7, truncate = false)
      println(s"wrote CSVs under $outdir")
    } finally {
      spark.stop()
    }
  }

  /** Daily CTR + impression count, ordered by day. */
  def daily(df: DataFrame): DataFrame =
    df.groupBy(to_date(col("impression_time")).as("day"))
      .agg(avg("clicked").as("ctr"), count(lit(1)).as("impressions"))
      .orderBy("day")

  /** CTR by hour-of-day (0-23). */
  def byHour(df: DataFrame): DataFrame =
    df.groupBy(hour(col("impression_time")).as("hour"))
      .agg(avg("clicked").as("ctr"))
      .orderBy("hour")

  /** CTR by day-of-week. `dow_num` is Spark's 1=Sun..7=Sat; `dow` is the short name. */
  def byDow(df: DataFrame): DataFrame =
    df.groupBy(dayofweek(col("impression_time")).as("dow_num"),
               date_format(col("impression_time"), "E").as("dow"))
      .agg(avg("clicked").as("ctr"))
      .orderBy("dow_num")

  def writeCsv(df: DataFrame, path: String): Unit =
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
}
