package com.demo.recsys

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ItemSequencePreprocessingJob {
  private val DefaultMinRating = 3.5

  def main(args: Array[String]): Unit = {
    val inputPath = args.headOption.orElse(sys.env.get("RATINGS_INPUT_PATH")).getOrElse {
      throw new IllegalArgumentException(
        "Missing ratings input path. Pass it as the first argument or set RATINGS_INPUT_PATH."
      )
    }
    val outputPath = args.lift(1).orElse(sys.env.get("ITEM_SEQUENCES_OUTPUT_PATH"))

    val spark = SparkSession.builder()
      .appName(sys.env.getOrElse("SPARK_APP_NAME", "ItemSequencePreprocessingJob"))
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", sys.env.getOrElse("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
      .getOrCreate()

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

  def processItemSequence(sparkSession: SparkSession): RDD[Seq[String]] = {
    val defaultPath = sys.env
      .get("RATINGS_INPUT_PATH")
      .orElse(Option(getClass.getResource("/webroot/sampledata/ratings.csv")).map(_.getPath))
      .orElse(Option(getClass.getResource("/sampledata/ratings.csv")).map(_.getPath))
      .getOrElse("spark-recsys/sampledata/ratings.csv")

    processItemSequence(sparkSession, defaultPath)
  }

  def processItemSequence(
      sparkSession: SparkSession,
      ratingsPath: String,
      minRating: Double = DefaultMinRating,
      userIdColumn: String = "userId",
      itemIdColumn: String = "movieId",
      ratingColumn: String = "rating",
      timestampColumn: String = "timestamp"
  ): RDD[Seq[String]] = {
    processItemSequenceDataFrame(
      sparkSession = sparkSession,
      ratingsPath = ratingsPath,
      minRating = minRating,
      userIdColumn = userIdColumn,
      itemIdColumn = itemIdColumn,
      ratingColumn = ratingColumn,
      timestampColumn = timestampColumn
    )
      .select("movieIdStr")
      .rdd
      .map(row => row.getAs[String]("movieIdStr").split(" ").toSeq)
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
    val ratingSamples = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
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
