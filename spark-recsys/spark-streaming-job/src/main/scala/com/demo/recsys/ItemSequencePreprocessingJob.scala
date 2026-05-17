package com.demo.recsys

import com.demo.common.{Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ItemSequencePreprocessingJob {
  private val DefaultMinRating = 3.5

  def main(args: Array[String]): Unit = {
    val inputPath = Env.requiredArgOrEnv(args, 0, "RATINGS_INPUT_PATH", "ratings input path")
    val outputPath = Env.argOrEnv(args, 1, "ITEM_SEQUENCES_OUTPUT_PATH")

    val spark = SparkSessions.create("ItemSequencePreprocessingJob")

    try {
      val userSequences = processItemSequenceDataFrame(spark, inputPath)

      outputPath match {
        case Some(path) =>
          userSequences.select("movieIdStr").write.mode("overwrite").text(path)
        case None =>
          userSequences.select("movieIdStr").show(truncate = false)
      }
    } finally {
      spark.stop()
    }
  }

  def processItemSequenceDataFrame(
      sparkSession: SparkSession,
      ratingsPath: String,
      minRating: Double = DefaultMinRating,
      userIdColumn: String = "userId",
      itemIdColumn: String = "movieId",
      ratingColumn: String = "rating",
      timestampColumn: String = "timestamp"
  ): DataFrame = {
    val schema = StructType(Seq(
      StructField(userIdColumn, StringType),
      StructField(itemIdColumn, StringType),
      StructField(ratingColumn, DoubleType),
      StructField(timestampColumn, LongType)
    ))
    val ratingSamples = sparkSession.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(ratingsPath)

    processItemSequenceDataFrame(
      ratingSamples = ratingSamples,
      minRating = minRating,
      userIdColumn = userIdColumn,
      itemIdColumn = itemIdColumn,
      ratingColumn = ratingColumn,
      timestampColumn = timestampColumn
    )
  }

  def processItemSequenceDataFrame(
      ratingSamples: DataFrame,
      minRating: Double,
      userIdColumn: String,
      itemIdColumn: String,
      ratingColumn: String,
      timestampColumn: String
  ): DataFrame = {
    ratingSamples
      .select(
        col(userIdColumn).cast("string").as("userId"),
        col(itemIdColumn).cast("string").as("movieId"),
        col(ratingColumn).cast("double").as("rating"),
        col(timestampColumn).cast("long").as("timestamp")
      )
      .where(
        col("userId").isNotNull &&
          col("movieId").isNotNull &&
          col("timestamp").isNotNull &&
          col("rating") >= lit(minRating)
      )
      .groupBy("userId")
      // sort_array on struct sorts by first field (timestamp) — no UDF needed
      .agg(sort_array(collect_list(struct(col("timestamp"), col("movieId")))).as("sortedPairs"))
      .where(size(col("sortedPairs")) > 1)
      .withColumn("movieIds", transform(col("sortedPairs"), x => x.getField("movieId")))
      .withColumn("movieIdStr", array_join(col("movieIds"), " "))
      .select("userId", "movieIds", "movieIdStr")
  }
}
