package com.demo.report

import com.demo.util.SparkSessions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Batch report (Spark): read the date-partitioned training_samples Parquet and compute the
  * engagement metric — CTR = avg(clicked) — as a time-series by date, hour-of-day, and
  * day-of-week. This is the Spark equivalent of engagement_report.py's data layer: it runs in
  * the same JVM/Spark stack as the pipeline and scales past a single pandas process. The deeper
  * statistical analyses (STL decomposition, changepoint detection) stay in the Python scaffold.
  *
  * Run:
  *   ENGAGEMENT_INPUT_PATH=/tmp/spark-recsys/engagement-sim/training-samples \
  *   SPARK_MAIN_CLASS=com.demo.report.EngagementReportJob ./run-streaming-job.sh
  */
object EngagementReportJob {

  /** Daily CTR + impression count, ordered by day. */
  def daily(df: DataFrame): DataFrame =
    df.groupBy(to_date(col("impression_time")).as("day"))
      .agg(avg(col("clicked")).as("ctr"), count(lit(1)).as("impressions"))
      .orderBy("day")

  /** CTR by hour-of-day (0–23). */
  def byHour(df: DataFrame): DataFrame =
    df.groupBy(hour(col("impression_time")).as("hour"))
      .agg(avg(col("clicked")).as("ctr"))
      .orderBy("hour")

  /** CTR by day-of-week. `dow_num` is Spark's 1=Sun…7=Sat; `dow` is the short name. */
  def byDayOfWeek(df: DataFrame): DataFrame =
    df.groupBy(
        dayofweek(col("impression_time")).as("dow_num"),
        date_format(col("impression_time"), "EEE").as("dow"))
      .agg(avg(col("clicked")).as("ctr"))
      .orderBy("dow_num")

  def writeCsv(df: DataFrame, path: String): Unit =
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)

  def main(args: Array[String]): Unit = {
    val input = sys.env.getOrElse("ENGAGEMENT_INPUT_PATH",
      args.headOption.getOrElse("/tmp/spark-recsys/engagement-sim/training-samples"))
    val outDir = sys.env.getOrElse("ENGAGEMENT_REPORT_PATH",
      args.lift(1).getOrElse(s"$input/../report-spark"))

    val spark: SparkSession = SparkSessions.create("EngagementReportJob")
    val df = spark.read.parquet(input).cache()

    val d = daily(df); val h = byHour(df); val w = byDayOfWeek(df)
    writeCsv(d, s"$outDir/ctr_daily")
    writeCsv(h, s"$outDir/ctr_by_hour")
    writeCsv(w, s"$outDir/ctr_by_dow")

    println("=== daily CTR ===");          d.show(50, truncate = false)
    println("=== CTR by hour ===");        h.show(24, truncate = false)
    println("=== CTR by day-of-week ==="); w.show(7,  truncate = false)
    println(s"wrote CSVs under $outDir")
    spark.stop()
  }
}
