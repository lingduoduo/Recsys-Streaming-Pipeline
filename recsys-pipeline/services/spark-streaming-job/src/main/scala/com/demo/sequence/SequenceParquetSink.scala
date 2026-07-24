package com.demo.sequence

import com.demo.engine.Sink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** The offline mirror of `SequenceRedisSink`. Parquet does its own columnar encoding,
  * so chunks are exploded back to one row per event rather than stored packed. */
class SequenceParquetSink(outputPath: String, mode: SequenceWriteMode) extends Sink {

  def write(batch: DataFrame, batchId: Long): Unit = {
    val writeMode = mode match {
      case SequenceWriteMode.Overwrite => "overwrite"
      case SequenceWriteMode.Append    => "append"
    }
    SequenceParquetSink.explodeChunks(batch)
      .write
      .mode(writeMode)
      .partitionBy("bucket", "kind")
      .parquet(outputPath)
  }
}

object SequenceParquetSink {

  private val IndexField = "__i"

  def explodeChunks(chunks: DataFrame): DataFrame = {
    val indexed = chunks
      .filter(col(SequenceSchema.ColCount) > 0)
      .withColumn(
        IndexField,
        explode(sequence(lit(0), col(SequenceSchema.ColCount).cast("int") - 1))
      )

    val unpacked = SequenceSchema.Columns.foldLeft(indexed) { (df, column) =>
      // split(..., -1) keeps trailing empty elements; the default would drop them
      // and shift every row whose last value is null.
      df.withColumn(column, element_at(split(col(column), ",", -1), col(IndexField) + 1))
    }

    unpacked.select(
      col("user_id"),
      col("kind"),
      col("bucket"),
      col(SequenceSchema.ColItemId),
      col(SequenceSchema.ColTs).cast("long").as(SequenceSchema.ColTs),
      col(SequenceSchema.ColAction),
      col(SequenceSchema.ColRating),
      col(SequenceSchema.ColGenres),
      col(SequenceSchema.ColReleaseYear)
    )
  }
}
