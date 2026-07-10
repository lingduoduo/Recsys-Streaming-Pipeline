package com.demo.report

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** Query analysis report (pure-Spark port of `query_analysis_report.py`) — most-common queries +
  * short-vs-long engagement.
  *
  * A "query" is the search intent of a recommended impression, represented as its movie's
  * genre-combo string (e.g. "Sci-Fi Action"). Reads the `training_samples` Parquet whose
  * `genres: array<string>` column is written by [[com.demo.process.OnlineJoinerStreamingJob]]'s
  * catalog path — so, like the keyword/relevance reports, this relies on the Parquet column and
  * does not read Redis.
  *
  * Analyses:
  *   1. Most common queries — top genre-combo queries by impressions (users/sessions/CTR/CVR).
  *   2. Short (≤10 chars) vs long (>10 chars) query engagement — CTR / CVR / avg_rating per bucket.
  *
  * Run through the project's pinned Spark:
  *   "$SPARK_HOME/bin/spark-submit" --class com.demo.report.QueryAnalysisReportJob \
  *     spark-recsys-job.jar <parquet-input> [<outdir>]
  */
object QueryAnalysisReportJob {

  val ShortMaxChars = 10

  def main(args: Array[String]): Unit = {
    val input = Env.argOrEnv(args, 0, "QUERY_ANALYSIS_INPUT_PATH")
      .getOrElse("/tmp/spark-recsys/movie-category-sim/training-samples")
    val outdir = Env.argOrEnv(args, 1, "QUERY_ANALYSIS_OUTPUT_PATH")
      .getOrElse(s"$input/../report-queries")
    val top = math.max(1, Env.int("QUERY_ANALYSIS_TOP", 20))

    val spark = SparkSessions.create("QueryAnalysisReportJob")
    try {
      val df = withQuery(ensureGenres(spark.read.parquet(input))).cache()

      def emit(name: String, frame: DataFrame, n: Int): Unit = {
        println(s"=== $name ===")
        frame.show(n, truncate = false)
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$outdir/$name")
      }

      emit("top_queries", mostCommonQueries(df).limit(top), top) // analysis 1
      emit("by_query_length", lengthEngagement(df), 10)          // analysis 2

      println(s"wrote query-analysis CSVs under $outdir")
    } finally {
      spark.stop()
    }
  }

  def ensureGenres(df: DataFrame): DataFrame = {
    require(df.columns.contains("genres"),
      "input Parquet has no `genres` column — produce it via OnlineJoinerStreamingJob's catalog path")
    df
  }

  /** Add `query` (genre-combo, 'unknown' if empty), its char length and length bucket. */
  def withQuery(df: DataFrame): DataFrame = {
    val combo = concat_ws(" ", col("genres"))
    df.withColumn("query", when(combo === "", lit("unknown")).otherwise(combo))
      .withColumn("query_len", length(col("query")))
      .withColumn("query_length",
        when(length(col("query")) <= ShortMaxChars, lit("short (<=10)")).otherwise(lit("long (>10)")))
  }

  def mostCommonQueries(df: DataFrame): DataFrame = {
    val aggs = engagementAggs :+ sum("label").as("rating_sum")
    val grouped = df.groupBy("query", "query_len").agg(aggs.head, aggs.tail: _*)
    withRates(grouped).orderBy(col("impressions").desc, col("query"))
  }

  def lengthEngagement(df: DataFrame): DataFrame = {
    val aggs = engagementAggs ++ Seq(
      countDistinct("query").as("distinct_queries"),
      sum("label").as("rating_sum"))
    val grouped = df.groupBy("query_length").agg(aggs.head, aggs.tail: _*)
    withRates(grouped).orderBy("query_length")
  }

  private def engagementAggs: Seq[Column] = Seq(
    count(lit(1)).as("impressions"),
    countDistinct("user_id").as("users"),
    countDistinct("session_id").as("sessions"),
    sum("clicked").as("clicks"),
    sum("ordered").as("orders"))

  private def withRates(grouped: DataFrame): DataFrame =
    grouped
      .withColumn("ctr", round(col("clicks") / col("impressions"), 4))
      .withColumn("cvr", round(col("orders") / col("impressions"), 4))
      .withColumn("avg_rating", round(col("rating_sum") / col("impressions"), 4))
      .drop("rating_sum")
}
