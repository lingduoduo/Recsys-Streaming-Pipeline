package com.demo.sequence

import com.demo.event.{FieldGate, Gated}
import com.demo.util.{DropMetrics, Reporter}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** Turns a flat events DataFrame into one row per `(user_id, kind, bucket)` partition,
  * with each event attribute packed into its own positionally-aligned column string. */
object SequenceEncoder {

  private val RowsField = "__rows"

  /** Identity fields must not contain a packing separator.
    *
    * `sanitized` strips `,` and `|` from every packed value, which is cosmetic for descriptive
    * fields but not for identity: `user_id` is a Redis key component and `item_id` is what a
    * recommendation resolves to, so stripping can silently merge two distinct entities (`a,b`
    * and `ab`). The packing format cannot change cheaply — every read path would need a decoder
    * and existing data a migration — so these rows are dropped and counted instead.
    */
  def gateIdentifiers(events: DataFrame): Gated =
    FieldGate(events, Seq(
      "separator_in_identifier" ->
        (col("user_id").rlike("[,|]") || col(SequenceSchema.ColItemId).rlike("[,|]"))
    ))

  /** Gate identity fields here rather than at each call site, so no producer can forget it.
    * `batchId` defaults to -1 for the sink lambda, which has no batch in scope. */
  def toColumnChunks(
      events: DataFrame,
      batchId: Long = -1L,
      reporter: Reporter = DropMetrics
  ): DataFrame = {
    val sorted = reporter.report(gateIdentifiers(events), "SequenceEncoder", batchId)
      .withColumn("bucket", SequenceSchema.bucketColumn(col(SequenceSchema.ColTs)))
      .groupBy("user_id", "kind", "bucket")
      // sort_array on a struct sorts by its first field (ts) — no UDF needed, same idiom
      // as ItemSequencePreprocessingJob.
      .agg(
        sort_array(collect_list(struct(
          col(SequenceSchema.ColTs).as("ts"),
          col(SequenceSchema.ColItemId).as("item_id"),
          col(SequenceSchema.ColAction).as("action"),
          col(SequenceSchema.ColRating).as("rating"),
          col(SequenceSchema.ColGenres).as("genres"),
          col(SequenceSchema.ColReleaseYear).as("release_year")
        ))).as(RowsField)
      )

    sorted.select(
      col("user_id"),
      col("kind"),
      col("bucket"),
      packScalar("item_id").as(SequenceSchema.ColItemId),
      packScalar("ts").as(SequenceSchema.ColTs),
      packScalar("action").as(SequenceSchema.ColAction),
      packScalar("rating").as(SequenceSchema.ColRating),
      packGenres.as(SequenceSchema.ColGenres),
      packScalar("release_year").as(SequenceSchema.ColReleaseYear),
      size(col(RowsField)).cast("long").as(SequenceSchema.ColCount)
    )
  }

  /** `array_join` skips nulls entirely, which would shorten the column and shift every
    * later row — so each element is coalesced to "" before joining. */
  private def packScalar(field: String): Column =
    array_join(
      transform(col(RowsField), row => coalesce(sanitized(row.getField(field).cast("string")), lit(""))),
      SequenceSchema.RowSeparator
    )

  private def packGenres: Column =
    array_join(
      transform(
        col(RowsField),
        row => coalesce(
          array_join(
            transform(coalesce(row.getField("genres"), array()), g => sanitized(g)),
            SequenceSchema.ValueSeparator
          ),
          lit("")
        )
      ),
      SequenceSchema.RowSeparator
    )

  private def sanitized(value: Column): Column =
    regexp_replace(value, "[,|]", "")
}
