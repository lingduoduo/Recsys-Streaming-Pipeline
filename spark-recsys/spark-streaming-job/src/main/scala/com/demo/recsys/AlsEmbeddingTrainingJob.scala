package com.demo.recsys

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AlsEmbeddingTrainingJob {
  private val DefaultRank = 16
  private val DefaultMaxIter = 10
  private val DefaultRegParam = 0.1

  def main(args: Array[String]): Unit = {
    val ratingsPath = args.headOption.orElse(sys.env.get("RATINGS_INPUT_PATH")).getOrElse {
      throw new IllegalArgumentException(
        "Missing ratings input path. Pass it as the first argument or set RATINGS_INPUT_PATH."
      )
    }
    val outputPath = args
      .lift(1)
      .orElse(sys.env.get("ALS_EMBEDDING_OUTPUT_PATH"))
      .getOrElse("spark-recsys/sampledata/als")

    val spark = SparkSession.builder()
      .appName(sys.env.getOrElse("SPARK_APP_NAME", "AlsEmbeddingTrainingJob"))
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", sys.env.getOrElse("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
      .getOrCreate()

    try {
      val ratings = readRatings(spark, ratingsPath)
      val (userFactors, itemFactors) = trainAlsEmbeddings(
        sparkSession = spark,
        ratings = ratings,
        rank = sys.env.get("ALS_RANK").flatMap(toIntOption).getOrElse(DefaultRank),
        maxIter = sys.env.get("ALS_MAX_ITER").flatMap(toIntOption).getOrElse(DefaultMaxIter),
        regParam = sys.env.get("ALS_REG_PARAM").flatMap(toDoubleOption).getOrElse(DefaultRegParam)
      )

      writeFactors(userFactors, s"$outputPath/userFactors")
      writeFactors(itemFactors, s"$outputPath/itemFactors")
    } finally {
      spark.stop()
    }
  }

  def trainAlsEmbeddings(
      sparkSession: SparkSession,
      ratingsPath: String,
      rank: Int = DefaultRank,
      maxIter: Int = DefaultMaxIter,
      regParam: Double = DefaultRegParam
  ): (DataFrame, DataFrame) = {
    trainAlsEmbeddings(
      sparkSession = sparkSession,
      ratings = readRatings(sparkSession, ratingsPath),
      rank = rank,
      maxIter = maxIter,
      regParam = regParam
    )
  }

  def trainAlsEmbeddings(
      sparkSession: SparkSession,
      ratings: DataFrame,
      rank: Int,
      maxIter: Int,
      regParam: Double
  ): (DataFrame, DataFrame) = {
    import sparkSession.implicits._

    val userIndexer = new StringIndexer()
      .setInputCol("userId")
      .setOutputCol("userIdIndex")
      .setHandleInvalid("skip")
      .fit(ratings)

    val userIndexed = userIndexer.transform(ratings)

    val itemIndexer = new StringIndexer()
      .setInputCol("movieId")
      .setOutputCol("movieIdIndex")
      .setHandleInvalid("skip")
      .fit(userIndexed)

    val indexedRatings = itemIndexer.transform(userIndexed)
      .select(
        col("userIdIndex").cast("int").as("userIdInt"),
        col("movieIdIndex").cast("int").as("movieIdInt"),
        col("rating").cast("float").as("rating")
      )
      .cache()

    val als = new ALS()
      .setUserCol("userIdInt")
      .setItemCol("movieIdInt")
      .setRatingCol("rating")
      .setRank(rank)
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setColdStartStrategy("drop")

    val model = als.fit(indexedRatings)
    indexedRatings.unpersist()

    val userIdMap = userIndexer.labels.zipWithIndex
      .map { case (userId, index) => (index, userId) }
      .toSeq
      .toDF("id", "userId")

    val movieIdMap = itemIndexer.labels.zipWithIndex
      .map { case (movieId, index) => (index, movieId) }
      .toSeq
      .toDF("id", "movieId")

    val userFactors = model.userFactors
      .join(userIdMap, Seq("id"))
      .select(col("userId"), col("features").as("userEmbedding"))

    val itemFactors = model.itemFactors
      .join(movieIdMap, Seq("id"))
      .select(col("movieId"), col("features").as("itemEmbedding"))

    (userFactors, itemFactors)
  }

  private def readRatings(sparkSession: SparkSession, ratingsPath: String): DataFrame =
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(ratingsPath)
      .select(
        col("userId").cast("string").as("userId"),
        col("movieId").cast("string").as("movieId"),
        col("rating").cast("double").as("rating")
      )
      .where(
        col("userId").isNotNull &&
          col("movieId").isNotNull &&
          col("rating").isNotNull
      )

  private def writeFactors(factors: DataFrame, outputPath: String): Unit = {
    val idColumn = factors.columns.head
    val embeddingColumn = factors.columns(1)
    val vectorToString = udf { vector: Seq[Float] => vector.mkString(" ") }

    factors
      .withColumn("embeddingStr", vectorToString(col(embeddingColumn)))
      .select(concat_ws(":", col(idColumn), col("embeddingStr")).as("value"))
      .write
      .mode("overwrite")
      .text(outputPath)
  }

  private def toIntOption(value: String): Option[Int] =
    try Some(value.toInt)
    catch { case _: NumberFormatException => None }

  private def toDoubleOption(value: String): Option[Double] =
    try Some(value.toDouble)
    catch { case _: NumberFormatException => None }
}
