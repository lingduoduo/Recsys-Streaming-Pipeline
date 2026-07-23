package com.demo.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

/** Shared reader for the ratings CSV (`userId,movieId,rating,timestamp`, header row).
  * Returns the full typed DataFrame; callers apply their own `select`/`where`. */
object RatingsCsv {

  val Schema: StructType = StructType(Seq(
    StructField("userId", StringType),
    StructField("movieId", StringType),
    StructField("rating", DoubleType),
    StructField("timestamp", LongType)
  ))

  def read(spark: SparkSession, path: String): DataFrame =
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(Schema)
      .load(path)
}
