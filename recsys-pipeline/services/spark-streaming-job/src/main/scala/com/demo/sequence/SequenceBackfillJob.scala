package com.demo.sequence

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** One-shot backfill of the columnar sequence store from the historical ratings CSV.
  * Runs in Overwrite mode, so it is idempotent and skips the read-merge phase entirely. */
object SequenceBackfillJob {

  private val RatingsSchema = StructType(Seq(
    StructField("userId", StringType),
    StructField("movieId", StringType),
    StructField("rating", DoubleType),
    StructField("timestamp", LongType)
  ))

  def main(args: Array[String]): Unit = {
    val ratingsPath = Env.requiredArgOrEnv(args, 0, "RATINGS_INPUT_PATH", "ratings input path")
    val redisHost   = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort   = Env.int("REDIS_PORT", 6379)
    val poolMax     = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))
    val pipelineSz  = math.max(3, Env.int("REDIS_PIPELINE_SIZE", 500))
    val cfg         = SequenceJobConfig.fromEnv()

    val spark = SparkSessions.create("SequenceBackfillJob")
    try {
      SequenceSinks.write(
        SequenceEncoder.toColumnChunks(readRatings(spark, ratingsPath), cfg.bucketWidth),
        cfg, redisHost, redisPort, poolMax, pipelineSz,
        SequenceWriteMode.Overwrite, 0L
      )
    } finally {
      spark.stop()
    }
  }

  /** MovieLens ratings CSV → sequence-store event shape. `timestamp` is in seconds. */
  def readRatings(spark: SparkSession, ratingsPath: String): DataFrame =
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(RatingsSchema)
      .load(ratingsPath)
      .filter(col("userId").isNotNull && col("movieId").isNotNull && col("timestamp").isNotNull)
      .select(
        col("userId").as("user_id"),
        lit(SequenceSchema.KindRating).as("kind"),
        col("movieId").as("item_id"),
        (col("timestamp") * 1000L).cast("long").as("ts"),
        lit("rate").as("action"),
        col("rating").cast("double").as("rating"),
        lit(null).cast("array<string>").as("genres"),
        lit(null).cast("int").as("release_year")
      )
}
