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
  * straight from the Parquet `context_features` map (always emitted, no Redis). The demographic
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

      emit("platform", platformMetrics(df, overallCtr)) // event-context, from Parquet

      val ids = df.select("user_id").distinct().collect().map(_.getString(0))
      val demographics = fetchDemographics(ids, redisHost, redisPort, redisPoolMaxTotal)
      if (demographics.nonEmpty) {
        val joined = perUserEngagement(df).join(demographicsDf(spark, demographics), "user_id")
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

  /** Platform breakdown straight from the Parquet `context_features` map (no Redis). */
  def platformMetrics(df: DataFrame, overallCtr: Double): DataFrame = {
    val seg = df.select(
      col("context_features").getItem("platform").as("platform"),
      col("clicked"), col("ordered"), col("user_id"))
    val grouped = seg.groupBy("platform").agg(
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

  /** Build the demographics DataFrame from the Redis `user:{id}:features` hashes. */
  def demographicsDf(spark: SparkSession, features: Map[String, Map[String, String]]): DataFrame = {
    val rows = features.toSeq.map { case (id, h) =>
      Row(id, h.get("gender").orNull, h.get("occupation").orNull,
        SegmentFeatures.deriveAgeBand(h.getOrElse("age", "")),
        SegmentFeatures.deriveGeo(h.getOrElse("zipCode", "")),
        parseDouble(h.getOrElse("avgRating", "")),
        parseLong(h.getOrElse("ratingCount", "")))
    }
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

  private def parseDouble(s: String): Double = scala.util.Try(s.toDouble).getOrElse(0.0)
  private def parseLong(s: String): Long = scala.util.Try(s.toDouble.toLong).getOrElse(0L)
  private def round4(v: Double): Double = math.round(v * 10000.0) / 10000.0
}
