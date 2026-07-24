package com.demo.sequence

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** Turns a flat events DataFrame into one row per `(user_id, kind, bucket)` partition,
  * with each event attribute packed into its own positionally-aligned column string. */
object SequenceEncoder {

  private val RowsField = "__rows"

  def toColumnChunks(events: DataFrame): DataFrame = {
    val sorted = events
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
