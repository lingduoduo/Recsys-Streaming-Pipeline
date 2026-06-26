package com.demo.event

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/** The Kafka-JSON parse mechanic shared by every streaming job:
  * cast the `value` column to a JSON string, apply `from_json`, flatten. */
object EventParsing {

  def fromJson(rawKafka: DataFrame, schema: StructType): DataFrame =
    rawKafka
      .selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")
}
