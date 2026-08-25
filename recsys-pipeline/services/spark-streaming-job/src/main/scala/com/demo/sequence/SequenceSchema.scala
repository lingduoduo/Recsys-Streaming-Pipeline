package com.demo.sequence

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

/** Names and shapes for the columnar sequence store. No I/O, no DataFrames beyond
  * `bucketColumn`, so every other component can depend on this without pulling in Redis. */
object SequenceSchema {
  val KindRating   = "rating"
  val KindClick    = "click"
  val KindBehavior = "behavior"

  val ColItemId      = "item_id"
  val ColTs          = "ts"
  val ColAction      = "action"
  val ColRating      = "rating"
  val ColGenres      = "genres"
  val ColReleaseYear = "release_year"
  val ColCount       = "n"

  val Columns: Seq[String] =
    Seq(ColItemId, ColTs, ColAction, ColRating, ColGenres, ColReleaseYear)

  val RowSeparator   = ","
  val ValueSeparator = "|"

  private val DayFormat = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)

  def bucket(tsMillis: Long): String =
    DayFormat.format(Instant.ofEpochMilli(tsMillis))

  /** Spark expression equivalent of `bucket`. Uses DateType arithmetic from the epoch
    * rather than `from_unixtime`, so the result does not depend on the session time zone. */
  def bucketColumn(tsCol: Column): Column =
    date_format(
      date_add(to_date(lit("1970-01-01")), floor(tsCol / 86400000L).cast("int")),
      "yyyyMMdd"
    )

  def key(userId: String, kind: String, bucket: String): String =
    s"seq:$userId:$kind:$bucket"
}
