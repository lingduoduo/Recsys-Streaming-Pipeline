package com.demo.engine

import java.sql.{Date, Timestamp}
import java.time.{Instant, ZoneOffset}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, lit, max, udf}
import org.apache.spark.sql.types.{LongType, StringType}

import scala.util.control.NonFatal

class RawArchiveSink(validRoot: String, deadLetterRoot: String, stateNamespace: String = "default") {

  private val safeStateNamespace = stateNamespace.replaceAll("[^A-Za-z0-9._-]", "_")

  def writeValid(df: DataFrame, batchId: Long): Unit = {
    val utcDate = udf((timestampMs: java.lang.Long) => RawArchiveSink.toUtcDate(timestampMs))
    writeBatch(
      df.withColumn("archived_at", current_timestamp())
        .withColumn("date", utcDate(col("timestamp_ms"))),
      validRoot,
      batchId
    )
  }

  def writeDeadLetters(df: DataFrame, batchId: Long): Unit = {
    val utcDate = udf((timestamp: Timestamp) =>
      if (timestamp == null) null else RawArchiveSink.toUtcDate(timestamp.getTime))
    writeBatch(
      df.withColumn("archived_at", current_timestamp())
        .withColumn("date", utcDate(col("kafka_timestamp"))),
      deadLetterRoot,
      batchId
    )
  }

  /** Apply watermark-bounded event-id state after raw archival and before business stages.
    *
    * The Avro engine applies business stages inside `foreachBatch` so archive failures can abort
    * source progress. A deterministic per-batch snapshot provides the equivalent durable state
    * boundary without scanning the unbounded raw archive. A retry reads the previous snapshot,
    * intentionally ignoring its own snapshot so the same business work is replayed.
    */
  def deduplicateValid(df: DataFrame, batchId: Long, watermarkDelay: String): DataFrame = {
    if (!df.columns.contains("event_id") || !df.columns.contains("timestamp_ms")) return df

    val currentState = df
      .filter(col("event_id").isNotNull && col("timestamp_ms").isNotNull)
      .groupBy("event_id")
      .agg(max(col("timestamp_ms")).cast(LongType).as("timestamp_ms"))
    val previousState = readPreviousDedupeState(df, batchId)
    val combinedState = previousState.unionByName(currentState)
    val latest = combinedState.agg(max(col("timestamp_ms"))).first()

    if (latest.isNullAt(0)) {
      writeDedupeState(currentState, batchId)
      df
    } else {
      val cutoff = RawArchiveSink.watermarkCutoff(latest.getLong(0), watermarkDelay)
      val activePrevious = previousState.filter(col("timestamp_ms") >= lit(cutoff))
      val eligibleCurrent = df.filter(col("timestamp_ms") >= lit(cutoff))
      val unseen = eligibleCurrent.join(activePrevious.select("event_id"), Seq("event_id"), "left_anti")
      val nextState = activePrevious
        .unionByName(currentState.filter(col("timestamp_ms") >= lit(cutoff)))
        .groupBy("event_id")
        .agg(max(col("timestamp_ms")).cast(LongType).as("timestamp_ms"))
      writeDedupeState(nextState, batchId)
      unseen
    }
  }

  /** Best-effort state compaction after every business sink has completed successfully. */
  def completeBusinessBatch(df: DataFrame, batchId: Long): Unit = {
    try {
      val root = dedupeRoot
      val fileSystem = root.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
      if (fileSystem.exists(root)) {
        fileSystem.listStatus(root).iterator
          .filter(_.isDirectory)
          .filter(status => status.getPath.getName.forall(_.isDigit))
          .filter(_.getPath.getName.toLong < batchId)
          .foreach(status => fileSystem.delete(status.getPath, true))
      }
    } catch {
      case NonFatal(_) => ()
    }
  }

  private def dedupeRoot: Path = new Path(validRoot, s"_dedupe/$safeStateNamespace")

  private def readPreviousDedupeState(df: DataFrame, batchId: Long): DataFrame = {
    val empty = df.select(
      col("event_id").cast(StringType).as("event_id"),
      col("timestamp_ms").cast(LongType).as("timestamp_ms")
    ).limit(0)
    if (batchId <= 0L) return empty

    val path = new Path(dedupeRoot, (batchId - 1L).toString)
    val fileSystem = path.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(path) || !RawArchiveSink.hasParquetData(fileSystem, path)) empty
    else df.sparkSession.read.parquet(path.toString)
      .filter(col("event_id").isNotNull)
      .select("event_id", "timestamp_ms")
  }

  private def writeDedupeState(state: DataFrame, batchId: Long): Unit = {
    val marker = state.sparkSession.range(1).select(
      lit(null).cast(StringType).as("event_id"),
      lit(null).cast(LongType).as("timestamp_ms")
    )
    writeDirectory(
      state.unionByName(marker),
      new Path(dedupeRoot, s"_staging/$batchId"),
      new Path(dedupeRoot, batchId.toString),
      partitionByDate = false,
      s"dedupe state batch $batchId"
    )
  }

  private def writeBatch(df: DataFrame, root: String, batchId: Long): Unit = {
    val rootPath = new Path(root)
    writeDirectory(
      df,
      new Path(rootPath, s"_staging/$batchId"),
      new Path(rootPath, s"_batches/$batchId"),
      partitionByDate = true,
      s"archive batch $batchId below $root"
    )
  }

  private def writeDirectory(
      df: DataFrame,
      stagingPath: Path,
      finalPath: Path,
      partitionByDate: Boolean,
      description: String
  ): Unit = {
    val fileSystem = finalPath.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
    if (fileSystem.exists(finalPath)) return
    if (fileSystem.exists(stagingPath)) fileSystem.delete(stagingPath, true)

    val writer = df.write.mode("errorifexists")
    if (partitionByDate) writer.partitionBy("date").parquet(stagingPath.toString)
    else writer.parquet(stagingPath.toString)

    fileSystem.mkdirs(finalPath.getParent)
    if (!fileSystem.rename(stagingPath, finalPath)) {
      if (fileSystem.exists(finalPath)) fileSystem.delete(stagingPath, true)
      else throw new IllegalStateException(s"failed to commit $description")
    }
  }
}

private object RawArchiveSink {
  def toUtcDate(epochMillis: java.lang.Long): Date =
    if (epochMillis == null) null
    else Date.valueOf(Instant.ofEpochMilli(epochMillis).atZone(ZoneOffset.UTC).toLocalDate)

  def watermarkCutoff(latestTimestampMs: Long, watermarkDelay: String): Long = {
    val interval = IntervalUtils.fromIntervalString(watermarkDelay)
    Instant.ofEpochMilli(latestTimestampMs)
      .atZone(ZoneOffset.UTC)
      .minusMonths(interval.months.toLong)
      .minusDays(interval.days.toLong)
      .minusNanos(Math.multiplyExact(interval.microseconds, 1000L))
      .toInstant
      .toEpochMilli
  }

  def hasParquetData(fileSystem: org.apache.hadoop.fs.FileSystem, path: Path): Boolean = {
    val files = fileSystem.listFiles(path, true)
    var found = false
    while (!found && files.hasNext) {
      found = files.next().getPath.getName.endsWith(".parquet")
    }
    found
  }
}
