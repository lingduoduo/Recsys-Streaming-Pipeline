package com.demo.recsys

import com.demo.common.{Env, SparkSessions}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AlsEmbeddingTrainingJob {
  private val DefaultRank = 16
  private val DefaultMaxIter = 10
  private val DefaultRegParam = 0.1

  def main(args: Array[String]): Unit = {
    val ratingsPath = Env.requiredArgOrEnv(args, 0, "RATINGS_INPUT_PATH", "ratings input path")
    val outputPath = Env.argOrEnv(args, 1, "ALS_EMBEDDING_OUTPUT_PATH")
      .getOrElse("spark-recsys/sampledata/als")

    val spark = SparkSessions.create("AlsEmbeddingTrainingJob")

    try {
      val ratings = readRatings(spark, ratingsPath)
      val (userFactors, itemFactors) = trainAlsEmbeddings(
        sparkSession = spark,
        ratings = ratings,
        rank = Env.int("ALS_RANK", DefaultRank),
        maxIter = Env.int("ALS_MAX_ITER", DefaultMaxIter),
        regParam = Env.double("ALS_REG_PARAM", DefaultRegParam)
      )

      writeFactors(userFactors, s"$outputPath/userFactors", "userId", "userEmbedding")
      writeFactors(itemFactors, s"$outputPath/itemFactors", "movieId", "itemEmbedding")
    } finally {
      spark.stop()
    }
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

    val userDecoder = new IndexToString()
      .setInputCol("idDouble")
      .setOutputCol("userId")
      .setLabels(userIndexer.labels)

    val movieDecoder = new IndexToString()
      .setInputCol("idDouble")
      .setOutputCol("movieId")
      .setLabels(itemIndexer.labels)

    val userFactors = userDecoder
      .transform(model.userFactors.withColumn("idDouble", col("id").cast(DoubleType)))
      .select(col("userId"), col("features").as("userEmbedding"))

    val itemFactors = movieDecoder
      .transform(model.itemFactors.withColumn("idDouble", col("id").cast(DoubleType)))
      .select(col("movieId"), col("features").as("itemEmbedding"))

    (userFactors, itemFactors)
  }

  private val ratingsSchema = StructType(Seq(
    StructField("userId", StringType),
    StructField("movieId", StringType),
    StructField("rating", DoubleType),
    StructField("timestamp", LongType)
  ))

  private def readRatings(sparkSession: SparkSession, ratingsPath: String): DataFrame =
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .schema(ratingsSchema)
      .load(ratingsPath)
      .select(
        col("userId"),
        col("movieId"),
        col("rating")
      )
      .where(
        col("userId").isNotNull &&
          col("movieId").isNotNull &&
          col("rating").isNotNull
      )

  private def writeFactors(factors: DataFrame, outputPath: String, idCol: String, embeddingCol: String): Unit = {
    val vectorToString = udf { vector: Seq[Float] => vector.mkString(" ") }
    factors
      .withColumn("embeddingStr", vectorToString(col(embeddingCol)))
      .select(concat_ws(":", col(idCol), col("embeddingStr")).as("value"))
      .write
      .mode("overwrite")
      .text(outputPath)
  }
}
