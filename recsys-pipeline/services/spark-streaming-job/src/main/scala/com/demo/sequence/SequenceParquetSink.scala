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

  // A packed string always encodes split(packed, ",", -1).length elements -- "" is
  // ONE empty element (e.g. a click event's null rating), never zero; zero elements
  // is expressed only by n == 0. So there is no "" special case here. A SQL-null
  // column means the column is entirely absent (not merely short), so it must not
  // clamp effectiveN at all -- we return n itself and let the other columns decide.
  // This clamp is silent by necessity: explodeChunks is a pure distributed Spark
  // transformation with no per-row driver loop to log from. The logged clamp for
  // this same torn-write case lives on the serving-side reader instead.
  private def splitLength(column: String): Column = {
    val packed = col(column)
    when(packed.isNull, col(SequenceSchema.ColCount).cast("int"))
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
