package com.demo.event

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** Shared event projections for both derived JSON topics and canonical Avro event frames. */
object EventParsing {

  private lazy val canonicalFieldNames = EventAvroCodec.schema.getFields.asScala.map(_.name)

  def fromJson(rawKafka: DataFrame, schema: StructType): DataFrame =
    rawKafka
      .selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")

  /** Retain only fields from the canonical Avro contract, dropping Kafka lineage before business logic. */
  def canonicalEvents(decoded: DataFrame): DataFrame = {
    val available = decoded.columns.toSet
    decoded.select(canonicalFieldNames.filter(available).map(col): _*)
  }

  /** Watermarked event-id de-duplication. Adds a transient `event_time` column from
    * `eventTime`, sets the watermark, and drops duplicate `event_id`s seen within it. */
  def dedupeWithinWatermark(df: DataFrame, eventTime: Column, delay: String): DataFrame =
    if (df.isStreaming)
      df.withColumn("event_time", eventTime)
        .withWatermark("event_time", delay)
        .dropDuplicatesWithinWatermark("event_id")
    else
      df.withColumn("event_time", eventTime).dropDuplicates("event_id")
}
