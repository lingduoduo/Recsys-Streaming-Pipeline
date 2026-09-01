package com.demo.report

import com.demo.engine.RedisPool
import com.demo.util.{Env, MovieCategories, SparkSessions}
import org.apache.spark.sql.DataFrame
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
      val df = ReportWindow.withinLookback(spark.read.parquet(input), lookbackDays).cache()
      val overallCtr = round4(df.agg(avg("clicked")).first().getDouble(0))
      println(s"overall CTR = $overallCtr  (impressions=${df.count()})\n")

      val ids = df.select("item_id").distinct()
      val features = fetchMovieFeaturesDf(ids, redisHost, redisPort, redisPoolMaxTotal).cache()
      if (features.isEmpty) {
        println("no movie features in Redis — nothing to break down by; exiting")
        return
      }

      val joined = perItemEngagement(df).join(features, "item_id")
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

  /** Pure: one already-fetched Redis hash → one (item_id, l1, l2, l3) row. Single source of
    * truth for the per-row derivation, used by the executor-side `fetchMovieFeaturesDf` below. */
  def categoryRow(id: String, h: Map[String, String]): org.apache.spark.sql.Row = {
    val genres = h.getOrElse("genres", "")
    val year = h.getOrElse("releaseYear", "")
    org.apache.spark.sql.Row(id, MovieCategories.l1(genres), MovieCategories.l2(genres),
      MovieCategories.l3(genres, year))
  }

  /** Pure: a raw (possibly null/empty) Redis hash → at most one row. "Missing keys omitted"
    * lives here so it is testable without Redis, and is enforced identically by both fetch paths. */
  def categoryRowOrNone(id: String, h: java.util.Map[String, String]): Option[org.apache.spark.sql.Row] =
    if (h == null || h.isEmpty) None else Some(categoryRow(id, h.asScala.toMap))

  private val RedisPipelineBatchSize = 500

  /** Executor-side, pipelined replacement for the driver-side fetch-then-collect pair
    * previously used by `main`: reads `movie:{id}:features` in parallel across
    * partitions, one pooled Jedis connection per partition (`RedisPool` — one JedisPool per
    * executor JVM), batching HGETALLs through `jedis.pipelined()` so N items cost O(partitions)
    * round trips instead of N sequential ones on the driver. Missing/empty hashes are omitted,
    * exactly like the driver-side path.
    */
  def fetchMovieFeaturesDf(ids: DataFrame, host: String, port: Int, poolMax: Int): DataFrame = {
    val rowRdd = ids.rdd.mapPartitions { partitionRows =>
      val partitionIds = partitionRows.map(_.getString(0)).toList
      if (partitionIds.isEmpty) Iterator.empty
      else {
        val jedis = RedisPool.get(host, port, poolMax).getResource
        try {
          // Eagerly build the full result before returning: mapPartitions hands Spark a lazy
          // iterator, and closing `jedis` in `finally` would run before Spark ever consumes a
          // lazy iterator, killing the connection mid-read. Materializing into `results` here
          // means every Redis call happens inside this try, before close() runs — so it is safe
          // to close right after.
          val results = scala.collection.mutable.ArrayBuffer.empty[org.apache.spark.sql.Row]
          partitionIds.grouped(RedisPipelineBatchSize).foreach { chunk =>
            val pipeline = jedis.pipelined()
            val pending = chunk.map(id => id -> pipeline.hgetAll(s"movie:$id:features"))
            pipeline.sync()
            pending.foreach { case (id, response) =>
              categoryRowOrNone(id, response.get()).foreach(results += _)
            }
          }
          results.iterator
        } finally jedis.close()
      }
    }
    ids.sparkSession.createDataFrame(rowRdd, CategorySchema)
  }

  private def round4(v: Double): Double = math.round(v * 10000.0) / 10000.0
}
