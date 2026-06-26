package com.demo.event

import org.apache.spark.sql.{Column, DataFrame}
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

  /** Attaches an "ingest" observed metric counting rows where user_id is null (corrupt parses). */
  def observeIngest(df: DataFrame): DataFrame =
    df.observe("ingest", sum(when(col("user_id").isNull, 1L).otherwise(0L)).as("corrupt"))

  /** Watermarked event-id de-duplication. Adds a transient `event_time` column from
    * `eventTime`, sets the watermark, and drops duplicate `event_id`s seen within it. */
  def dedupeWithinWatermark(df: DataFrame, eventTime: Column, delay: String): DataFrame =
    df.withColumn("event_time", eventTime)
      .withWatermark("event_time", delay)
      .dropDuplicatesWithinWatermark("event_id")
}
