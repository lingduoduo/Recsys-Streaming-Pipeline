package com.demo.task

import com.demo.sink.RedisWriter
import com.demo.util.{Env, RatingsCsv, SparkSessions}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AlsEmbeddingTrainingJob {
  private val DefaultRank            = 16
  private val DefaultMaxIter         = 10
  private val DefaultRegParam        = 0.1
  private val DefaultRedisTtlSeconds = 60 * 60 * 24

  private val vectorToString = udf { v: Seq[Float] => v.mkString(" ") }

  def main(args: Array[String]): Unit = {
    val ratingsPath = Env.requiredArgOrEnv(args, 0, "RATINGS_INPUT_PATH", "ratings input path")
    val outputPath = Env.argOrEnv(args, 1, "ALS_EMBEDDING_OUTPUT_PATH")
      .getOrElse("recsys-pipeline/sampledata/als")

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

      if (Env.boolean("ALS_SAVE_TO_REDIS", default = false)) {
        val redisHost       = sys.env.getOrElse("REDIS_HOST", "localhost")
        val redisPort       = Env.int("REDIS_PORT", 6379)
        val redisTtlSeconds = Env.int("ALS_REDIS_TTL_SECONDS", DefaultRedisTtlSeconds)
        writeFactorsToRedis(userFactors, "userId",  "userEmbedding", redisHost, redisPort,
          sys.env.getOrElse("ALS_USER_REDIS_KEY_PREFIX", "alsUserEmb"), redisTtlSeconds)
        writeFactorsToRedis(itemFactors, "movieId", "itemEmbedding", redisHost, redisPort,
          sys.env.getOrElse("ALS_ITEM_REDIS_KEY_PREFIX", "alsItemEmb"), redisTtlSeconds)
      }
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

  private[task] def writeFactors(
      factors: DataFrame,
      outputPath: String,
      idCol: String,
      embeddingCol: String
  ): Unit = {
    factors
      .withColumn("embeddingStr", vectorToString(col(embeddingCol)))
      .select(concat_ws(":", col(idCol), col("embeddingStr")).as("value"))
      .write
      .mode("overwrite")
      .text(outputPath)
  }

  private def readRatings(sparkSession: SparkSession, ratingsPath: String): DataFrame =
    RatingsCsv.read(sparkSession, ratingsPath)
      .select(col("userId"), col("movieId"), col("rating"))
      .where(
        col("userId").isNotNull &&
          col("movieId").isNotNull &&
          col("rating").isNotNull
      )

  private def writeFactorsToRedis(
      factors: DataFrame,
      idCol: String,
      embeddingCol: String,
      redisHost: String,
      redisPort: Int,
      keyPrefix: String,
      redisTtlSeconds: Int
  ): Unit = {
    factors
      .withColumn("embStr", vectorToString(col(embeddingCol)))
      .foreachPartition { rows: Iterator[Row] =>
        RedisWriter.writeWithPipeline(
          redisHost, redisPort,
          rows.map(r => r.getAs[String](idCol) -> r.getAs[String]("embStr")),
          keyPrefix, redisTtlSeconds
        )
      }
  }
}
