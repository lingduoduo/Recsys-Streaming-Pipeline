package com.demo.event

import org.apache.spark.sql.types._

/** Schemas for the unified `recsys_events` payloads, shared by the jobs that
  * consume them. `timestamp_ms` (millis) is primary; `timestamp` (seconds) is
  * legacy compatibility. */
object EventSchemas {

  /** Fields present on every recsys_events record. */
  val baseFields: Seq[StructField] = Seq(
    StructField("user_id", StringType, nullable = false),
    StructField("item_id", StringType, nullable = false),
    StructField("event_type", StringType, nullable = false),
    StructField("timestamp_ms", LongType, nullable = true),
    StructField("timestamp", LongType, nullable = true)
  )

  /** UserEventStreamingJob view: adds event_id. Order matches the original schema. */
  val userEvent: StructType =
    StructType(StructField("event_id", StringType, nullable = true) +: baseFields)

  /** OnlineJoinerStreamingJob view: adds event_id, request_id, position, and feature maps. */
  val joiner: StructType = StructType(
    (StructField("event_id", StringType, nullable = true) +:
      StructField("request_id", StringType, nullable = false) +: baseFields) ++ Seq(
      StructField("position", IntegerType, nullable = true),
      StructField("user_features", MapType(StringType, StringType), nullable = true),
      StructField("item_features", MapType(StringType, StringType), nullable = true),
      StructField("context_features", MapType(StringType, StringType), nullable = true)
    )
  )
}
