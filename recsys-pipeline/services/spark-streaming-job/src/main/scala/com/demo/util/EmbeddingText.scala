package com.demo.util

import com.demo.sink.RedisWriter
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array_join, col, concat_ws, transform}

import scala.util.Try

/** The `id:v1 v2 ...` text contract every embedding job writes and every consumer reads.
  *
  * One line per id; the vector is space-separated numbers. Redis carries the same string under
  * `prefix:id`, which is what the retrieval service's parseVector reads.
  */
object EmbeddingText {

  /** A numeric array column rendered as the space-joined string the contract carries. */
  def vectorString(vector: Column): Column =
    array_join(transform(vector, (x: Column) => x.cast("string")), " ")

  /** (id, vector) rows from a file or a directory of part files; malformed lines are skipped. */
  def read(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    spark.read.textFile(path).flatMap { line =>
      val sep = line.indexOf(':')
      if (sep <= 0) None
      else Try((line.substring(0, sep).trim,
                line.substring(sep + 1).trim.split("\\s+").map(_.toDouble).toSeq)).toOption
    }.toDF("id", "vector")
  }

  def writeText(df: DataFrame, idCol: String, vectorCol: String, path: String): Unit =
    df.select(concat_ws(":", col(idCol), vectorString(col(vectorCol))).as("value"))
      .write.mode("overwrite").text(path)

  def writeRedis(df: DataFrame, idCol: String, vectorCol: String,
                 redisHost: String, redisPort: Int, keyPrefix: String, ttlSeconds: Int): Unit =
    df.select(col(idCol).as("id"), vectorString(col(vectorCol)).as("value"))
      .foreachPartition { rows: Iterator[Row] =>
        RedisWriter.writeWithPipeline(
          redisHost, redisPort,
          rows.map(r => r.getAs[String]("id") -> r.getAs[String]("value")),
          keyPrefix, ttlSeconds)
      }
}
