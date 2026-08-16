package com.demo.event

import org.apache.spark.sql.types._

/** Schemas for the unified `recsys_events` payloads, shared by the jobs that
  * consume them. `timestamp_ms` (millis) is primary; `timestamp` (seconds) is
  * legacy compatibility. */
object EventSchemas {

  /** Fields present on every recsys_events record.
    *
    * All nullable: `from_json` emits null for a missing or malformed field regardless of what the
    * schema declares, which is exactly why every consumer gates on these explicitly. Declaring
    * them non-null would describe a guarantee nothing enforces, and invite someone to delete
    * those gates. */
  val baseFields: Seq[StructField] = Seq(
    StructField("user_id", StringType, nullable = true),
    StructField("item_id", StringType, nullable = true),
    StructField("event_type", StringType, nullable = true),
    StructField("timestamp_ms", LongType, nullable = true),
    StructField("timestamp", LongType, nullable = true)
  )

  /** UserEventStreamingJob view: adds event_id. Order matches the original schema. */
  val userEvent: StructType =
    StructType(StructField("event_id", StringType, nullable = true) +: baseFields)

  /** OnlineJoinerStreamingJob view: adds event_id, session_id, request_id, position, and feature maps. */
  val joiner: StructType = StructType(
    (StructField("event_id", StringType, nullable = true) +:
      StructField("session_id", StringType, nullable = true) +:
      // Nullable in the Avro contract too (`["null", "string"]`), and the joiner gates on it.
      StructField("request_id", StringType, nullable = true) +: baseFields) ++ Seq(
      StructField("position", IntegerType, nullable = true),
      StructField("user_features", MapType(StringType, StringType), nullable = true),
      StructField("item_features", MapType(StringType, StringType), nullable = true),
      StructField("context_features", MapType(StringType, StringType), nullable = true),
      StructField("model_version", StringType, nullable = true),
      StructField("policy_version", StringType, nullable = true),
      StructField("algorithm_version", StringType, nullable = true),
      StructField("rating", DoubleType, nullable = true),
      StructField("negative_feedback_reason", StringType, nullable = true),
      StructField("dwell_millis", LongType, nullable = true),
      StructField("completion_rate", DoubleType, nullable = true),
      StructField("published_at", LongType, nullable = true),
      StructField("new_release", BooleanType, nullable = true),
      StructField("filter_reason", StringType, nullable = true),
      StructField("unsafe_label", BooleanType, nullable = true)
    )
  )
}
