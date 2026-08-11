package com.demo.report

import com.demo.util.{Env, MovieCategories, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Keyword analysis report (pure-Spark port of `keyword_analysis_report.py`) — two analyses
  * over the engagement stream.
  *
  * Reads the `training_samples` Parquet (user_id, session_id, item_id, clicked, `genres`),
  * written by [[com.demo.process.OnlineJoinerStreamingJob]] whose catalog path always carries a
  * top-level `genres: array<string>` column. Per movie: keyword = first genre, subkeyword =
  * second genre; category l1 = genre family, l2 = primary genre, l3 = genre×decade (via
  * [[com.demo.util.MovieCategories]]).
  *
  * A "query" is a (user, session); its keywords are the genres of the movies the user **clicked**,
  * so `query_clicks` sums `clicked`.
  *
  * Analyses:
  *   1. Keyword / subkeyword distribution — movie impressions, distinct movies, query clicks.
  *   2. Category top keywords — within each category value (l1/l2/l3), genres exploded and ranked
  *      by movie impressions, with query clicks beside them.
  *
  * Unlike the Python version this relies on the Parquet `genres` column (always present for
  * OnlineJoiner output) and does not read Redis. `release_year` is absent from that Parquet, so —
  * exactly as in the Python path that sources genres from Parquet — l3's decade is `unknown`.
  *
  * Run through the project's pinned Spark:
  *   "$SPARK_HOME/bin/spark-submit" --class com.demo.report.KeywordAnalysisReportJob \
  *     spark-recsys-job.jar <parquet-input> [<outdir>]
  */
object KeywordAnalysisReportJob {

  val Levels: Seq[String] = Seq("l1", "l2", "l3")

  def main(args: Array[String]): Unit = {
    val input = Env.argOrEnv(args, 0, "KEYWORD_ANALYSIS_INPUT_PATH")
      .getOrElse("/tmp/spark-recsys/movie-category-sim/training-samples")
    val outdir = Env.argOrEnv(args, 1, "KEYWORD_ANALYSIS_OUTPUT_PATH")
      .getOrElse(s"$input/../report-keywords")
    val top = math.max(1, Env.int("KEYWORD_ANALYSIS_TOP", 10))

    val lookbackDays = Env.int("KEYWORD_ANALYSIS_LOOKBACK_DAYS", 30)

    val spark = SparkSessions.create("KeywordAnalysisReportJob")
    try {
      val df = withKeywordsAndCategories(ensureMeta(ReportWindow.withinLookback(spark.read.parquet(input), lookbackDays))).cache()

      def emit(name: String, frame: DataFrame): Unit = {
        println(s"=== $name ===")
        frame.show(top, truncate = false)
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$outdir/$name")
      }

      // Analysis 1 — keyword / subkeyword distribution
      emit("by_keyword", keywordDistribution(df, "keyword"))
      emit("by_subkeyword", keywordDistribution(df, "subkeyword"))

      // Analysis 2 — category top keywords (movies vs queries)
      Levels.foreach { level =>
        emit(s"top_keywords_$level", categoryTopKeywords(df, level).filter(col("rank") <= top))
      }

      println(s"wrote keyword-analysis CSVs under $outdir")
    } finally {
      spark.stop()
    }
  }

  /** Guarantee a `release_year` column (OnlineJoiner Parquet has `genres` but no year). */
  def ensureMeta(df: DataFrame): DataFrame = {
    require(df.columns.contains("genres"),
      "input Parquet has no `genres` column — produce it via OnlineJoinerStreamingJob's catalog path")
    if (df.columns.contains("release_year")) df
    else df.withColumn("release_year", lit(null).cast(IntegerType))
  }

  /** Add keyword/subkeyword and l1/l2/l3, derived from the `genres` array + `release_year`. */
  def withKeywordsAndCategories(df: DataFrame): DataFrame = {
    val genresStr = concat_ws(",", col("genres"))
    val yearStr = col("release_year").cast(StringType)
    val primaryUdf = udf((g: String) => MovieCategories.primaryGenre(g))
    val secondaryUdf = udf((g: String) => MovieCategories.secondaryGenre(g))
    val l1Udf = udf((g: String) => MovieCategories.l1(g))
    val l3Udf = udf((g: String, y: String) => MovieCategories.l3(g, y))
    df.withColumn("keyword", primaryUdf(genresStr))
      .withColumn("subkeyword", secondaryUdf(genresStr))
      .withColumn("l1", l1Udf(genresStr))
      .withColumn("l2", primaryUdf(genresStr)) // l2 == primary genre
      .withColumn("l3", l3Udf(genresStr, yearStr))
  }

  /** Distribution of `dim` (keyword|subkeyword) over movies (impressions / distinct movies) and
    * queries (clicks). */
  def keywordDistribution(df: DataFrame, dim: String): DataFrame =
    df.groupBy(dim)
      .agg(
        count(lit(1)).as("movie_impressions"),
        countDistinct("item_id").as("distinct_movies"),
        sum("clicked").as("query_clicks"))
      .orderBy(col("movie_impressions").desc)

  /** Within each category value, rank exploded genres by movie impressions (with query clicks). */
  def categoryTopKeywords(df: DataFrame, level: String): DataFrame = {
    val exploded = df.select(col(level), explode(col("genres")).as("keyword"),
      col("item_id"), col("clicked"))
    val grouped = exploded.groupBy(col(level), col("keyword"))
      .agg(
        count(lit(1)).as("movie_impressions"),
        sum("clicked").as("query_clicks"))
    val rank = Window.partitionBy(level).orderBy(col("movie_impressions").desc, col("keyword"))
    grouped.withColumn("rank", row_number().over(rank)).orderBy(col(level), col("rank"))
  }
}
