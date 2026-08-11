package com.demo.report

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** Relevance analysis report (pure-Spark port of `relevance_analysis_report.py`) —
  * relevance-state distribution + query/genre relevance.
  *
  * Relevance is the graded engagement label of a recommended impression:
  *   label 0.0 → impression_only, 1.0 → clicked, 2.0 → ordered.
  * A "query" is the impression's genre-combo intent (`concat_ws(" ", genres)`).
  *
  * Reads the `training_samples` Parquet whose `genres: array<string>` column is written by
  * [[com.demo.process.OnlineJoinerStreamingJob]]'s catalog path — so, like the keyword report,
  * this relies on the Parquet column and does not read Redis.
  *
  * Analyses:
  *   1. Relevance-state distribution — per state: score, impressions, proportion.
  *   2. Query / genre relevance — mean score + per-state shares, by query and by genre (exploded).
  *
  * Run through the project's pinned Spark:
  *   "$SPARK_HOME/bin/spark-submit" --class com.demo.report.RelevanceAnalysisReportJob \
  *     spark-recsys-job.jar <parquet-input> [<outdir>]
  */
object RelevanceAnalysisReportJob {

  def main(args: Array[String]): Unit = {
    val input = Env.argOrEnv(args, 0, "RELEVANCE_ANALYSIS_INPUT_PATH")
      .getOrElse("/tmp/spark-recsys/movie-category-sim/training-samples")
    val outdir = Env.argOrEnv(args, 1, "RELEVANCE_ANALYSIS_OUTPUT_PATH")
      .getOrElse(s"$input/../report-relevance")
    val top = math.max(1, Env.int("RELEVANCE_ANALYSIS_TOP", 20))

    val lookbackDays = Env.int("RELEVANCE_ANALYSIS_LOOKBACK_DAYS", 30)

    val spark = SparkSessions.create("RelevanceAnalysisReportJob")
    try {
      val df = withRelevance(ensureGenres(ReportWindow.withinLookback(spark.read.parquet(input), lookbackDays))).cache()

      def emit(name: String, frame: DataFrame, n: Int): Unit = {
        println(s"=== $name ===")
        frame.show(n, truncate = false)
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$outdir/$name")
      }

      emit("by_state", stateDistribution(df), 10)      // analysis 1
      emit("by_query", byQuery(df).limit(top), top)    // analysis 2 (query)
      emit("by_genre", byGenre(df), 30)                // analysis 2 (movie genres)

      println(s"wrote relevance-analysis CSVs under $outdir")
    } finally {
      spark.stop()
    }
  }

  def ensureGenres(df: DataFrame): DataFrame = {
    require(df.columns.contains("genres"),
      "input Parquet has no `genres` column — produce it via OnlineJoinerStreamingJob's catalog path")
    df
  }

  /** Add `relevance_state` (from label) and `query` (genre-combo intent). */
  def withRelevance(df: DataFrame): DataFrame = {
    val combo = concat_ws(" ", col("genres"))
    df.withColumn("relevance_state",
        when(col("label") >= 2.0, lit("ordered"))
          .when(col("label") >= 1.0, lit("clicked"))
          .otherwise(lit("impression_only")))
      .withColumn("query", when(combo === "", lit("unknown")).otherwise(combo))
  }

  def stateDistribution(df: DataFrame): DataFrame = {
    val total = df.count()
    val denom = if (total > 0) total.toDouble else 1.0
    df.groupBy("relevance_state")
      .agg(first("label").as("score"), count(lit(1)).as("impressions"))
      .withColumn("proportion", round(col("impressions") / lit(denom), 4))
      .orderBy("score")
  }

  def byQuery(df: DataFrame): DataFrame = relevanceBreakdown(df, "query")

  def byGenre(df: DataFrame): DataFrame = {
    val exploded = df.select(explode(col("genres")).as("genre"), col("label"), col("relevance_state"))
    relevanceBreakdown(exploded, "genre")
  }

  /** Per `dim`: impressions, mean score, and the share of each relevance state. */
  private def relevanceBreakdown(df: DataFrame, dim: String): DataFrame =
    df.groupBy(dim)
      .agg(
        count(lit(1)).as("impressions"),
        round(avg("label"), 4).as("mean_score"),
        share("impression_only").as("impression_only_share"),
        share("clicked").as("clicked_share"),
        share("ordered").as("ordered_share"))
      .orderBy(col("mean_score").desc, col("impressions").desc)

  private def share(state: String): Column =
    round(avg(when(col("relevance_state") === state, 1.0).otherwise(0.0)), 4)
}
