package com.demo.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{date_add, floor, lit, to_date}

/** Date projections that do not depend on the session time zone.
  *
  * `to_date(from_unixtime(ts))` converts through a local wall-clock string, so the same instant
  * lands in different partitions on differently-configured machines — and `date` keys the CTR
  * train/holdout split in `CtrRankingModelTrainingJob`. `DateType` is days-since-epoch with no
  * zone attached and `date_add` is pure day arithmetic, so deriving the date from the epoch is
  * stable everywhere. Same idiom as `SequenceSchema.bucketColumn`, which documents the rule for
  * milliseconds.
  */
object TimePartitions {

  private val SecondsPerDay = 86400L

  /** UTC calendar date for a column of epoch **seconds**. */
  def utcDate(epochSeconds: Column): Column =
    date_add(to_date(lit("1970-01-01")), floor(epochSeconds / lit(SecondsPerDay)).cast("int"))
}
