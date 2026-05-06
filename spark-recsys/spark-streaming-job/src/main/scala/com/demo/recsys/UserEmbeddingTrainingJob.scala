package com.demo.recsys

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try

object UserEmbeddingTrainingJob {
  private val DefaultMinRating = 3.5

  case class Rating(userId: String, movieId: String, rating: Double)
  case class ItemEmbedding(movieId: String, vector: Seq[Double])

  def main(args: Array[String]): Unit = {
    val ratingsPath = args.headOption.orElse(sys.env.get("RATINGS_INPUT_PATH")).getOrElse {
      throw new IllegalArgumentException(
        "Missing ratings input path. Pass it as the first argument or set RATINGS_INPUT_PATH."
      )
    }
    val itemEmbeddingPath = args.lift(1).orElse(sys.env.get("ITEM2VEC_EMBEDDING_PATH")).getOrElse {
      throw new IllegalArgumentException(
        "Missing item embedding path. Pass it as the second argument or set ITEM2VEC_EMBEDDING_PATH."
      )
    }
    val userEmbeddingPath = args
      .lift(2)
      .orElse(sys.env.get("USER_EMBEDDING_OUTPUT_PATH"))
      .getOrElse("spark-recsys/sampledata/user_embedding.txt")

    val spark = SparkSession.builder()
      .appName(sys.env.getOrElse("SPARK_APP_NAME", "UserEmbeddingTrainingJob"))
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", sys.env.getOrElse("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
      .getOrCreate()

    try {
      val userEmbeddings = trainUserEmbeddings(
        sparkSession = spark,
        ratingsPath = ratingsPath,
        itemEmbeddingPath = itemEmbeddingPath,
        minRating = sys.env.get("USER_EMBEDDING_MIN_RATING").flatMap(toDoubleOption).getOrElse(DefaultMinRating)
      )

      userEmbeddings
        .select(concat_ws(":", col("userId"), col("userEmbeddingStr")).as("value"))
        .write
        .mode("overwrite")
        .text(userEmbeddingPath)
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
    val itemEmbeddings = readItemEmbeddings(sparkSession, itemEmbeddingPath)

    trainUserEmbeddings(ratings, itemEmbeddings, minRating)
  }

  def trainUserEmbeddings(
      ratings: DataFrame,
      itemEmbeddings: DataFrame,
      minRating: Double
  ): DataFrame = {
    val averageVecs: UserDefinedFunction = udf { vecs: Seq[Seq[Double]] =>
      if (vecs.isEmpty) Seq.empty[Double]
      else {
        val n = vecs.head.length
        val sum = Array.fill[Double](n)(0.0)
        vecs.foreach { vec =>
          var i = 0
          while (i < n) { sum(i) += vec(i); i += 1 }
        }
        val sz = vecs.size.toDouble
        sum.map(_ / sz).toSeq
      }
    }

    val vectorToString: UserDefinedFunction = udf { vec: Seq[Double] =>
      vec.mkString(" ")
    }

    val positiveRatings = ratings.filter(col("rating") >= lit(minRating))
    val joined = positiveRatings.join(itemEmbeddings, Seq("movieId"))

    val userGrouped = joined
      .groupBy("userId")
      .agg(collect_list(col("vector")).as("vecs"))

    userGrouped
      .withColumn("userEmbedding", averageVecs(col("vecs")))
      .withColumn("userEmbeddingStr", vectorToString(col("userEmbedding")))
      .select("userId", "userEmbedding", "userEmbeddingStr")
  }

  private val ratingsSchema = StructType(Seq(
    StructField("userId", StringType),
    StructField("movieId", StringType),
    StructField("rating", DoubleType),
    StructField("timestamp", LongType)
  ))

  private def readRatings(sparkSession: SparkSession, ratingsPath: String): DataFrame = {
    import sparkSession.implicits._

    sparkSession.read
      .format("csv")
      .option("header", "true")
      .schema(ratingsSchema)
      .load(ratingsPath)
      .select(col("userId"), col("movieId"), col("rating"))
      .as[Rating]
      .toDF()
  }

  private def readItemEmbeddings(sparkSession: SparkSession, itemEmbeddingPath: String): DataFrame = {
    import sparkSession.implicits._

    sparkSession.read
      .textFile(itemEmbeddingPath)
      .flatMap { line =>
        val parts = line.split(":", 2)
        if (parts.length != 2 || parts(0).trim.isEmpty || parts(1).trim.isEmpty) {
          None
        } else {
          Try(parts(1).trim.split("\\s+").map(_.toDouble).toSeq).toOption
            .map(vector => ItemEmbedding(parts(0).trim, vector))
        }
      }
      .toDF()
  }

  private def toDoubleOption(value: String): Option[Double] =
    try Some(value.toDouble)
    catch { case _: NumberFormatException => None }
}
