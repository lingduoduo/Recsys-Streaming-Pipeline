package com.demo.report

import com.demo.engine.RedisPool
import com.demo.util.{Env, SegmentFeatures, SparkSessions}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/** MovieLens-aligned user-segment engagement report (pure-Spark port of
  * `movielens_segment_report.py`).
  *
  * Joins engagement with demographics across the two real pipeline paths:
  *   • engagement: `training_samples` Parquet (clicked, ordered, user_id, context_features),
  *     written by [[com.demo.process.OnlineJoinerStreamingJob]].
  *   • demographics: Redis `user:{id}:features` (gender, occupation, age, zipCode, avgRating,
  *     ratingCount), written by [[com.demo.process.MovieLensContextCollectorStreamingJob]].
  *
  * Per segment: impressions, CTR, order_rate, clicks/user, CTR lift vs overall. `platform` comes
  * from the typed Parquet `device` column, falling back to the legacy `context_features["platform"]`
  * / `context_features["device"]` map keys for Parquet written before schema v2 (no Redis). The
  * demographic
  * dimensions (age_band, gender, occupation, geo) come from the Redis join, fetched only for the
  * user_ids present in the samples; age→age_band and zip→geo are derived with
  * [[com.demo.util.SegmentFeatures]]. When Redis has no demographics the report degrades to the
  * platform-only breakdown.
  *
  * Run through the project's pinned Spark:
  *   REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit" --class com.demo.report.SegmentReportJob \
  *     spark-recsys-job.jar <parquet-input> [<outdir>]
  */
object SegmentReportJob {

  val DemoDims: Seq[String] = Seq("age_band", "gender", "occupation", "geo")

  private val DemographicsSchema: StructType = StructType(Seq(
    StructField("user_id", StringType),
    StructField("gender", StringType),
    StructField("occupation", StringType),
    StructField("age_band", StringType),
    StructField("geo", StringType),
    StructField("user_avg_rating", DoubleType),
    StructField("user_rating_count", LongType)))

  def main(args: Array[String]): Unit = {
    val input = Env.argOrEnv(args, 0, "SEGMENT_REPORT_INPUT_PATH")
      .getOrElse("/tmp/spark-recsys/movielens-segment-sim/training-samples")
    val outdir = Env.argOrEnv(args, 1, "SEGMENT_REPORT_OUTPUT_PATH")
      .getOrElse(s"$input/../report-segments")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = Env.int("REDIS_PORT", 6379)
    val redisPoolMaxTotal = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))

    val lookbackDays = Env.int("SEGMENT_REPORT_LOOKBACK_DAYS", 30)

    val spark = SparkSessions.create("SegmentReportJob")
    try {
      val df = ReportWindow.withinLookback(spark.read.parquet(input), lookbackDays).cache()
      val overallCtr = round4(df.agg(avg("clicked")).first().getDouble(0))
      println(s"overall CTR = $overallCtr  (impressions=${df.count()})\n")

      def emit(name: String, frame: DataFrame): Unit = {
        println(s"=== engagement by $name (CTR desc; lift vs overall) ===")
        frame.show(truncate = false)
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$outdir/by_$name")
      }

      emit("device", deviceMetrics(df, overallCtr)) // event-context, from Parquet

      val ids = df.select("user_id").distinct()
      val demographics = fetchDemographicsDf(ids, redisHost, redisPort, redisPoolMaxTotal).cache()
      if (!demographics.isEmpty) {
        val joined = perUserEngagement(df).join(demographics, "user_id")
        DemoDims.foreach(dim => emit(dim, demographicMetrics(joined, dim, overallCtr)))
      } else {
        println("no demographics in Redis — skipped age_band/gender/occupation/geo")
      }
      println(s"wrote per-segment CSVs under $outdir")
    } finally {
      spark.stop()
    }
  }

  def perUserEngagement(df: DataFrame): DataFrame =
    df.groupBy("user_id").agg(
      count(lit(1)).as("impressions"),
      sum("clicked").as("clicks"),
      sum("ordered").as("orders"))

  /** Device breakdown from the typed `device` column, falling back to the legacy
    * `context_features["platform"]` / `context_features["device"]` map keys for Parquet
    * written before schema v2 promoted `device` out of the map.
    *
    * The segment, its column, and the `by_device` CSV directory are all named for the event
    * field they describe. They were called `platform` while the map key was, which left the
    * report contradicting both the event schema and the governance dimension. */
  def deviceMetrics(df: DataFrame, overallCtr: Double): DataFrame = {
    val typedDevice = if (df.columns.contains("device")) col("device") else lit(null).cast("string")
    val legacyDevice =
      if (df.columns.contains("context_features"))
        coalesce(col("context_features").getItem("platform"), col("context_features").getItem("device"))
      else lit(null).cast("string")
    val seg = df.select(
      coalesce(typedDevice, legacyDevice).as("device"),
      col("clicked"), col("ordered"), col("user_id"))
    val grouped = seg.groupBy("device").agg(
      count(lit(1)).as("impressions"),
      sum("clicked").as("clicks"),
      sum("ordered").as("orders"),
      countDistinct("user_id").as("users"))
    finalizeMetrics(grouped, overallCtr)
  }

  def demographicMetrics(joined: DataFrame, dim: String, overallCtr: Double): DataFrame = {
    val grouped = joined.groupBy(dim).agg(
      sum("impressions").as("impressions"),
      sum("clicks").as("clicks"),
      sum("orders").as("orders"),
      countDistinct("user_id").as("users"),
      // explicit-feedback avg rating, weighted by each user's rating count
      sum(col("user_avg_rating") * col("user_rating_count")).as("_rsum"),
      sum("user_rating_count").as("_rn"))
    finalizeMetrics(grouped, overallCtr)
      .withColumn("avg_rating", when(col("_rn") > 0, round(col("_rsum") / col("_rn"), 3)))
      .drop("_rsum", "_rn")
  }

  private def finalizeMetrics(grouped: DataFrame, overallCtr: Double): DataFrame =
    grouped
      .withColumn("ctr", round(col("clicks") / col("impressions"), 4))
      .withColumn("order_rate", round(col("orders") / col("impressions"), 4))
      .withColumn("clicks_per_user", round(col("clicks") / col("users"), 3))
      .withColumn("ctr_lift_pct",
        round((col("ctr") - lit(overallCtr)) / lit(overallCtr) * 100, 1))
      .orderBy(col("ctr").desc)

  /** Pure: one already-fetched Redis hash → one demographics row. Single source of truth for
    * the per-row derivation, shared by the driver-side Map path (`demographicsDf`, kept for
    * `fetchDemographics` callers) and the executor-side `fetchDemographicsDf` below. */
  def demographicsRow(id: String, h: Map[String, String]): Row =
    Row(id, h.get("gender").orNull, h.get("occupation").orNull,
      SegmentFeatures.deriveAgeBand(h.getOrElse("age", "")),
      SegmentFeatures.deriveGeo(h.getOrElse("zipCode", "")),
      parseDouble(h.getOrElse("avgRating", "")),
      parseLong(h.getOrElse("ratingCount", "")))

  /** Pure: a raw (possibly null/empty) Redis hash → at most one row. "Missing keys omitted"
    * lives here so it is testable without Redis, and is enforced identically by both fetch paths. */
  def demographicsRowOrNone(id: String, h: java.util.Map[String, String]): Option[Row] =
    if (h == null || h.isEmpty) None else Some(demographicsRow(id, h.asScala.toMap))

  /** Build the demographics DataFrame from the Redis `user:{id}:features` hashes. */
  def demographicsDf(spark: SparkSession, features: Map[String, Map[String, String]]): DataFrame = {
    val rows = features.toSeq.map { case (id, h) => demographicsRow(id, h) }
    spark.createDataFrame(rows.asJava, DemographicsSchema)
  }

  /** Driver-side Redis HGETALL of `user:{id}:features`; missing keys omitted. */
  def fetchDemographics(ids: Array[String], host: String, port: Int, poolMax: Int): Map[String, Map[String, String]] = {
    if (ids.isEmpty) return Map.empty
    val jedis = RedisPool.get(host, port, poolMax).getResource
    try {
      ids.flatMap { id =>
        val h = jedis.hgetAll(s"user:$id:features")
        if (h == null || h.isEmpty) None else Some(id -> h.asScala.toMap)
      }.toMap
    } finally jedis.close()
  }

  private val RedisPipelineBatchSize = 500

  /** Executor-side, pipelined replacement for the driver-side `fetchDemographics` +
    * `demographicsDf` pair used by `main`: reads `user:{id}:features` in parallel across
    * partitions, one pooled Jedis connection per partition (`RedisPool` — one JedisPool per
    * executor JVM), batching HGETALLs through `jedis.pipelined()` so N users cost O(partitions)
    * round trips instead of N sequential ones on the driver. Missing/empty hashes are omitted,
    * exactly like the driver-side path.
    */
  def fetchDemographicsDf(ids: DataFrame, host: String, port: Int, poolMax: Int): DataFrame = {
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
          val results = scala.collection.mutable.ArrayBuffer.empty[Row]
          partitionIds.grouped(RedisPipelineBatchSize).foreach { chunk =>
            val pipeline = jedis.pipelined()
            val pending = chunk.map(id => id -> pipeline.hgetAll(s"user:$id:features"))
            pipeline.sync()
            pending.foreach { case (id, response) =>
              demographicsRowOrNone(id, response.get()).foreach(results += _)
            }
          }
          results.iterator
        } finally jedis.close()
      }
    }
    ids.sparkSession.createDataFrame(rowRdd, DemographicsSchema)
  }

  private def parseDouble(s: String): Double = scala.util.Try(s.toDouble).getOrElse(0.0)
  private def parseLong(s: String): Long = scala.util.Try(s.toDouble.toLong).getOrElse(0L)
  private def round4(v: Double): Double = math.round(v * 10000.0) / 10000.0
}
