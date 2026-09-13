package com.demo.task

import com.demo.util.{EmbeddingText, Env, RatingsCsv, SparkSessions}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UserEmbeddingTrainingJob {
  private val DefaultMinRating       = 3.5
  private val DefaultRedisTtlSeconds = 60 * 60 * 24

  def main(args: Array[String]): Unit = {
    val ratingsPath = Env.requiredArgOrEnv(args, 0, "RATINGS_INPUT_PATH", "ratings input path")
    val itemEmbeddingPath = Env.requiredArgOrEnv(args, 1, "ITEM2VEC_EMBEDDING_PATH", "item embedding path")
    val userEmbeddingPath = Env.argOrEnv(args, 2, "USER_EMBEDDING_OUTPUT_PATH")
      .getOrElse("recsys-pipeline/sampledata/user_embedding.txt")

    val saveToRedis = Env.boolean("USER_EMBEDDING_SAVE_TO_REDIS", default = false)
    val spark = SparkSessions.create("UserEmbeddingTrainingJob")

    try {
      val raw = trainUserEmbeddings(
        sparkSession = spark,
        ratingsPath = ratingsPath,
        itemEmbeddingPath = itemEmbeddingPath,
        minRating = Env.double("USER_EMBEDDING_MIN_RATING", DefaultMinRating)
      )
      // Cache only when writing to both sinks to avoid recomputing the join+aggregation.
      val userEmbeddings = if (saveToRedis) raw.cache() else raw

      EmbeddingText.writeText(userEmbeddings, "userId", "userEmbedding", userEmbeddingPath)

      if (saveToRedis) {
        EmbeddingText.writeRedis(
          userEmbeddings, "userId", "userEmbedding",
          sys.env.getOrElse("REDIS_HOST", "localhost"), Env.int("REDIS_PORT", 6379),
          sys.env.getOrElse("USER_EMBEDDING_REDIS_KEY_PREFIX", "uEmb"),
          Env.int("USER_EMBEDDING_REDIS_TTL_SECONDS", DefaultRedisTtlSeconds))
        userEmbeddings.unpersist()
      }
    } finally {
      spark.stop()
    }
  }

  def trainUserEmbeddings(
      sparkSession: SparkSession,
      ratingsPath: String,
      itemEmbeddingPath: String,
      minRating: Double = DefaultMinRating
  ): DataFrame = {
    val ratings = readRatings(sparkSession, ratingsPath)
    val itemEmbeddings = EmbeddingText.read(sparkSession, itemEmbeddingPath).withColumnRenamed("id", "movieId")
    trainUserEmbeddings(ratings, itemEmbeddings, minRating)
  }

  def trainUserEmbeddings(
      ratings: DataFrame,
      itemEmbeddings: DataFrame,
      minRating: Double
  ): DataFrame = {
    val userGrouped = ratings.filter(col("rating") >= lit(minRating))
      .join(itemEmbeddings, Seq("movieId"))
      .groupBy("userId")
      .agg(collect_list(col("vector")).as("vecs"))

    val sumVec = aggregate(
      col("vecs"),
      array_repeat(lit(0.0), size(element_at(col("vecs"), 1))),
      (acc: Column, v: Column) => zip_with(acc, v, (a: Column, b: Column) => a + b)
    )
    userGrouped
      .withColumn("userEmbedding", transform(sumVec, (x: Column) => x / size(col("vecs"))))
      .select("userId", "userEmbedding")
  }

  private def readRatings(sparkSession: SparkSession, ratingsPath: String): DataFrame =
    RatingsCsv.read(sparkSession, ratingsPath)
      .select(col("userId"), col("movieId"), col("rating"))
}
