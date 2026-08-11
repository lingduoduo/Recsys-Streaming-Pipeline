package com.demo.report

import com.demo.engine.RedisPool
import com.demo.util.{Env, MovieCategories, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/** Movie-category engagement report (pure-Spark port of `movie_category_report.py`).
  *
  * Joins engagement with movie categories across the two real pipeline paths:
  *   • engagement: `training_samples` Parquet (per impression: clicked, ordered, item_id),
  *     written by [[com.demo.process.OnlineJoinerStreamingJob]].
  *   • categories: Redis `movie:{id}:features` (genres, releaseYear), written by
  *     [[com.demo.process.MovieLensContextCollectorStreamingJob]].
  *
  * For each level (l1 family / l2 primary genre / l3 genre×decade) reports impressions
  * (sample size), CTR, order_rate, clicks_per_item, and CTR lift vs overall. Categories are
  * fetched only for the item_ids present in the samples (the join is inner, so scanning all of
  * Redis would add nothing).
  *
  * Run through the project's pinned Spark:
  *   REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit" --class com.demo.report.MovieCategoryReportJob \
  *     spark-recsys-job.jar <parquet-input> [<outdir>]
  */
object MovieCategoryReportJob {

  val Levels: Seq[String] = Seq("l1", "l2", "l3")

  private val CategorySchema: StructType =
    StructType(Seq("item_id", "l1", "l2", "l3").map(StructField(_, StringType)))

  def main(args: Array[String]): Unit = {
    val input = Env.argOrEnv(args, 0, "MOVIE_CATEGORY_INPUT_PATH")
      .getOrElse("/tmp/spark-recsys/movie-category-sim/training-samples")
    val outdir = Env.argOrEnv(args, 1, "MOVIE_CATEGORY_OUTPUT_PATH")
      .getOrElse(s"$input/../report-categories")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = Env.int("REDIS_PORT", 6379)
    val redisPoolMaxTotal = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))
    val lookbackDays = Env.int("MOVIE_CATEGORY_LOOKBACK_DAYS", 30)

    val spark = SparkSessions.create("MovieCategoryReportJob")
    try {
      val df = withinLookback(spark.read.parquet(input), lookbackDays).cache()
      val overallCtr = round4(df.agg(avg("clicked")).first().getDouble(0))
      println(s"overall CTR = $overallCtr  (impressions=${df.count()})\n")

      val ids = df.select("item_id").distinct().collect().map(_.getString(0))
      val features = fetchMovieFeatures(ids, redisHost, redisPort, redisPoolMaxTotal)
      if (features.isEmpty) {
        println("no movie features in Redis — nothing to break down by; exiting")
        return
      }

      val joined = perItemEngagement(df).join(categoriesDf(spark, features), "item_id")
      Levels.foreach { level =>
        val m = categoryMetrics(joined, level, overallCtr)
        println(s"=== engagement by $level (CTR desc; lift vs overall) ===")
        m.show(30, truncate = false)
        m.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$outdir/by_$level")
      }
      println(s"wrote per-category CSVs under $outdir")
    } finally {
      spark.stop()
    }
  }

  /** Restrict to the most recent `lookbackDays` partition dates; unbounded when not positive.
    *
    * `training_samples` is partitioned by `date`, and this report read all of it, so its cost grew
    * with total history rather than with the window being reported on. The window is anchored to
    * the newest date present rather than the wall clock, so a report over historical data stays
    * deterministic and re-runnable.
    */
  def withinLookback(df: DataFrame, lookbackDays: Int): DataFrame =
    if (lookbackDays <= 0 || !df.columns.contains("date")) df
    else {
      val newest = df.agg(max(to_date(col("date")))).first()
      if (newest.isNullAt(0)) df
      else df.filter(to_date(col("date")) > date_sub(lit(newest.getDate(0)), lookbackDays))
    }

  def perItemEngagement(df: DataFrame): DataFrame =
    df.groupBy("item_id").agg(
      count(lit(1)).as("impressions"),
      sum("clicked").as("clicks"),
      sum("ordered").as("orders"))

  def categoryMetrics(joined: DataFrame, level: String, overallCtr: Double): DataFrame =
    joined.groupBy(level).agg(
        sum("impressions").as("impressions"),
        sum("clicks").as("clicks"),
        sum("orders").as("orders"),
        countDistinct("item_id").as("items"))
      .withColumn("ctr", round(col("clicks") / col("impressions"), 4))
      .withColumn("order_rate", round(col("orders") / col("impressions"), 4))
      .withColumn("clicks_per_item", round(col("clicks") / col("items"), 2))
      .withColumn("ctr_lift_pct",
        round((col("ctr") - lit(overallCtr)) / lit(overallCtr) * 100, 1))
      .orderBy(col("ctr").desc)

  /** Build (item_id, l1, l2, l3) rows from the Redis `movie:{id}:features` hashes. */
  def categoriesDf(spark: SparkSession, features: Map[String, Map[String, String]]): DataFrame = {
    val rows = features.toSeq.map { case (id, h) =>
      val genres = h.getOrElse("genres", "")
      val year = h.getOrElse("releaseYear", "")
      org.apache.spark.sql.Row(id, MovieCategories.l1(genres), MovieCategories.l2(genres),
        MovieCategories.l3(genres, year))
    }
    spark.createDataFrame(rows.asJava, CategorySchema)
  }

  /** Driver-side Redis HGETALL of `movie:{id}:features`; missing keys omitted. */
  def fetchMovieFeatures(ids: Array[String], host: String, port: Int, poolMax: Int): Map[String, Map[String, String]] = {
    if (ids.isEmpty) return Map.empty
    val jedis = RedisPool.get(host, port, poolMax).getResource
    try {
      ids.flatMap { id =>
        val h = jedis.hgetAll(s"movie:$id:features")
        if (h == null || h.isEmpty) None else Some(id -> h.asScala.toMap)
      }.toMap
    } finally jedis.close()
  }

  private def round4(v: Double): Double = math.round(v * 10000.0) / 10000.0
}
