package com.demo.report

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_sub, lit, max, to_date}

/** Bounds a report's read of the date-partitioned `training_samples` Parquet.
  *
  * Every report job read the whole archive, so their cost grew with total history rather than with
  * the window being reported on. Because the source is partitioned by `date`, filtering here prunes
  * partitions instead of scanning and discarding.
  */
object ReportWindow {

  /** Restrict to the most recent `lookbackDays` partition dates; unbounded when not positive.
    *
    * The window is anchored to the newest date present in the data, not the wall clock, so a report
    * over historical data stays deterministic: re-running last quarter's report next month gives
    * the same answer.
    *
    * Apply this to the raw `spark.read.parquet(...)` before any projection, so the filter reaches
    * the partition column and later transformations are free to drop it.
    */
  def withinLookback(df: DataFrame, lookbackDays: Int): DataFrame =
    if (lookbackDays <= 0 || !df.columns.contains("date")) df
    else {
      val newest = df.agg(max(to_date(col("date")))).first()
      if (newest.isNullAt(0)) df
      else df.filter(to_date(col("date")) > date_sub(lit(newest.getDate(0)), lookbackDays))
    }
}
