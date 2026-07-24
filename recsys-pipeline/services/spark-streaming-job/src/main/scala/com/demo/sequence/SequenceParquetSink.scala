package com.demo.sequence

import com.demo.engine.Sink
import org.apache.spark.sql.{Column, DataFrame}
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

  private val IndexField      = "__i"
  private val EffectiveNField = "__effectiveN"

  // Mirrors SequenceCodec.unpack, which treats a null/empty packed string as zero
  // elements rather than one: Spark's split("", ",", -1) returns a 1-element array,
  // so that case must be special-cased or a short column would not shrink effectiveN.
  private def splitLength(column: String): Column = {
    val packed = col(column)
    when(packed.isNull || packed === "", lit(0))
      .otherwise(size(split(packed, ",", -1)))
  }

  def explodeChunks(chunks: DataFrame): DataFrame = {
    // Consistency guard: a torn write can leave one packed column shorter than the
    // declared n. Clamp to the shortest column so we degrade to fewer, correct rows
    // instead of emitting NULLs for the missing tail.
    val withEffectiveN = chunks.withColumn(
      EffectiveNField,
      SequenceSchema.Columns.foldLeft(col(SequenceSchema.ColCount).cast("int")) { (acc, column) =>
        least(acc, splitLength(column))
      }
    )

    val indexed = withEffectiveN
      .filter(col(EffectiveNField) > 0)
      .withColumn(
        IndexField,
        explode(sequence(lit(0), col(EffectiveNField) - 1))
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
