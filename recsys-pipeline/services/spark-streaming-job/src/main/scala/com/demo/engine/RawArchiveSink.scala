package com.demo.engine

import java.sql.{Date, Timestamp}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.{Instant, ZoneOffset}
import java.util.UUID

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileContext, Options, Path}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, lit, max, udf}
import org.apache.spark.sql.types.{LongType, StringType}

import scala.util.control.NonFatal

/** Durable archive and dedupe state for one Structured Streaming checkpoint owner.
  *
  * `queryIdentity` must remain stable for the lifetime of a checkpoint and must not be shared by
  * concurrently active queries. Commits require a filesystem with atomic, non-overwriting
  * directory rename semantics (for example local/HDFS through Hadoop FileContext).
  */
class RawArchiveSink(validRoot: String, deadLetterRoot: String, val queryIdentity: String) {

  require(Option(queryIdentity).exists(_.trim.nonEmpty), "queryIdentity must not be blank")

  private val queryNamespace = RawArchiveSink.queryNamespace(queryIdentity)

  def stableQueryNamespace: String = queryNamespace

  def sinkContext(sinkIdentity: String, batchId: Long): SinkWriteContext = {
    require(Option(sinkIdentity).exists(_.trim.nonEmpty), "sinkIdentity must not be blank")
    SinkWriteContext(
      queryIdentity,
      queryNamespace,
      sinkIdentity,
      RawArchiveSink.queryNamespace(sinkIdentity),
      batchId)
  }

  def writeValid(df: DataFrame, batchId: Long): Unit = {
    val utcDate = udf((timestampMs: java.lang.Long) => RawArchiveSink.toUtcDate(timestampMs))
    writeBatch(
      df.withColumn("archived_at", current_timestamp())
        .withColumn("date", utcDate(col("timestamp_ms"))),
      validRoot,
      batchId,
      kind = "valid"
    )
  }

  def writeDeadLetters(df: DataFrame, batchId: Long): Unit = {
    val utcDate = udf((timestamp: Timestamp) =>
      if (timestamp == null) null else RawArchiveSink.toUtcDate(timestamp.getTime))
    writeBatch(
      df.withColumn("archived_at", current_timestamp())
        .withColumn("date", utcDate(col("kafka_timestamp"))),
      deadLetterRoot,
      batchId,
      kind = "dead-letter"
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
      // The watermark bounds retained *prior* state. It must not discard a unique row merely
      // because another row in this static micro-batch has a much newer event timestamp.
      val unseen = df.join(activePrevious.select("event_id"), Seq("event_id"), "left_anti")
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
          // A retry of batch N after its business sinks succeeded but before Spark committed the
          // source checkpoint must still reconstruct N from the N-1 snapshot.
          .filter(_.getPath.getName.toLong < batchId - 1L)
          .foreach(status => fileSystem.delete(status.getPath, true))
      }
    } catch {
      case NonFatal(_) => ()
    }
  }

  /** Return true only for a validated per-sink completion marker. */
  def isBusinessSinkComplete(df: DataFrame, sinkIdentity: String, batchId: Long): Boolean = {
    val context = sinkContext(sinkIdentity, batchId)
    val path = businessSinkPath(context)
    val fileSystem = path.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(path)) false
    else {
      validateCommitted(fileSystem, path, businessSinkManifest(context))
      true
    }
  }

  /** Atomically commit one stable query/sink/batch completion marker. */
  def completeBusinessSink(df: DataFrame, sinkIdentity: String, batchId: Long): Unit = {
    val context = sinkContext(sinkIdentity, batchId)
    val finalPath = businessSinkPath(context)
    val configuration = df.sparkSession.sparkContext.hadoopConfiguration
    val fileSystem = finalPath.getFileSystem(configuration)
    val expectedManifest = businessSinkManifest(context)
    if (fileSystem.exists(finalPath)) {
      validateCommitted(fileSystem, finalPath, expectedManifest)
      return
    }

    val attemptsRoot = new Path(finalPath.getParent, "_attempts")
    val attemptPath = new Path(attemptsRoot, UUID.randomUUID().toString)
    try {
      fileSystem.mkdirs(attemptPath)
      val success = fileSystem.create(new Path(attemptPath, "_SUCCESS"), false)
      success.close()
      writeManifest(fileSystem, attemptPath, expectedManifest)
      fileSystem.mkdirs(finalPath.getParent)
      val contextFs = FileContext.getFileContext(fileSystem.getUri, configuration)
      try {
        contextFs.rename(
          fileSystem.makeQualified(attemptPath),
          fileSystem.makeQualified(finalPath),
          Options.Rename.NONE)
      } catch {
        case _: FileAlreadyExistsException => fileSystem.delete(attemptPath, true)
      }
      if (!fileSystem.exists(finalPath))
        throw new IllegalStateException(s"failed to commit business sink ${context.sinkIdentity}")
      validateCommitted(fileSystem, finalPath, expectedManifest)
    } catch {
      case NonFatal(error) =>
        if (fileSystem.exists(attemptPath)) fileSystem.delete(attemptPath, true)
        throw error
    }
  }

  private def queryRoot(root: String): Path =
    new Path(new Path(root), s"_queries/$queryNamespace")

  private def dedupeRoot: Path = new Path(queryRoot(validRoot), "_dedupe")

  private def businessSinkPath(context: SinkWriteContext): Path =
    new Path(
      new Path(queryRoot(validRoot), s"_business_sinks/${context.sinkNamespace}/_batches"),
      context.batchId.toString)

  private def businessSinkManifest(context: SinkWriteContext): String =
    s"version=1\nquery=$queryNamespace\nkind=business-sink\n" +
      s"sink=${context.sinkNamespace}\nbatch_id=${context.batchId}\n"

  private def readPreviousDedupeState(df: DataFrame, batchId: Long): DataFrame = {
    val empty = df.select(
      col("event_id").cast(StringType).as("event_id"),
      col("timestamp_ms").cast(LongType).as("timestamp_ms")
    ).limit(0)
    if (batchId <= 0L) return empty

    val path = new Path(dedupeRoot, (batchId - 1L).toString)
    val fileSystem = path.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(path)) empty
    else {
      validateCommitted(fileSystem, path, manifest("dedupe", batchId - 1L))
      if (!RawArchiveSink.hasParquetData(fileSystem, path)) empty
      else df.sparkSession.read.parquet(path.toString)
        .filter(col("event_id").isNotNull)
        .select("event_id", "timestamp_ms")
    }
  }

  private def writeDedupeState(state: DataFrame, batchId: Long): Unit = {
    val marker = state.sparkSession.range(1).select(
      lit(null).cast(StringType).as("event_id"),
      lit(null).cast(LongType).as("timestamp_ms")
    )
    writeDirectory(
      state.unionByName(marker),
      new Path(dedupeRoot, s"_attempts/$batchId"),
      new Path(dedupeRoot, batchId.toString),
      partitionByDate = false,
      s"dedupe state batch $batchId",
      manifest("dedupe", batchId)
    )
  }

  private def writeBatch(df: DataFrame, root: String, batchId: Long, kind: String): Unit = {
    val rootPath = queryRoot(root)
    writeDirectory(
      df,
      new Path(rootPath, s"_attempts/$batchId"),
      new Path(rootPath, s"_batches/$batchId"),
      partitionByDate = true,
      s"$kind archive batch $batchId below $root",
      manifest(kind, batchId)
    )
  }

  private def writeDirectory(
      df: DataFrame,
      attemptsRoot: Path,
      finalPath: Path,
      partitionByDate: Boolean,
      description: String,
      expectedManifest: String
  ): Unit = {
    val configuration = df.sparkSession.sparkContext.hadoopConfiguration
    val fileSystem = finalPath.getFileSystem(configuration)
    if (fileSystem.exists(finalPath)) {
      validateCommitted(fileSystem, finalPath, expectedManifest)
      return
    }

    val attemptPath = new Path(attemptsRoot, UUID.randomUUID().toString)
    try {
      val writer = df.write.mode("errorifexists")
      if (partitionByDate) writer.partitionBy("date").parquet(attemptPath.toString)
      else writer.parquet(attemptPath.toString)
      writeManifest(fileSystem, attemptPath, expectedManifest)

      fileSystem.mkdirs(finalPath.getParent)
      val context = FileContext.getFileContext(fileSystem.getUri, configuration)
      try {
        context.rename(
          fileSystem.makeQualified(attemptPath),
          fileSystem.makeQualified(finalPath),
          Options.Rename.NONE
        )
      } catch {
        case _: FileAlreadyExistsException =>
          // Only this UUID attempt belongs to us; leave all sibling attempts untouched.
          fileSystem.delete(attemptPath, true)
      }

      if (!fileSystem.exists(finalPath))
        throw new IllegalStateException(s"failed to commit $description")
      validateCommitted(fileSystem, finalPath, expectedManifest)
    } catch {
      case NonFatal(error) =>
        // Cleanup is restricted to the UUID generated by this invocation.
        if (fileSystem.exists(attemptPath)) fileSystem.delete(attemptPath, true)
        throw error
    }
  }

  private def manifest(kind: String, batchId: Long): String =
    s"version=1\nquery=$queryNamespace\nkind=$kind\nbatch_id=$batchId\n"

  private def writeManifest(
      fileSystem: org.apache.hadoop.fs.FileSystem,
      directory: Path,
      contents: String
  ): Unit = {
    val output = fileSystem.create(new Path(directory, RawArchiveSink.CommitMarker), false)
    try output.write(contents.getBytes(StandardCharsets.UTF_8))
    finally output.close()
  }

  private def validateCommitted(
      fileSystem: org.apache.hadoop.fs.FileSystem,
      directory: Path,
      expectedManifest: String
  ): Unit = {
    val marker = new Path(directory, RawArchiveSink.CommitMarker)
    val success = new Path(directory, "_SUCCESS")
    if (!fileSystem.exists(marker) || !fileSystem.exists(success))
      throw new IllegalStateException(s"uncommitted or incomplete directory $directory")

    val input = fileSystem.open(marker)
    val actual = try {
      val source = scala.io.Source.fromInputStream(input, StandardCharsets.UTF_8.name())
      try source.mkString
      finally source.close()
    } finally {
      // Source.close closes the stream; Hadoop close is idempotent and keeps this safe if Source
      // construction or reading failed.
      input.close()
    }
    if (actual != expectedManifest)
      throw new IllegalStateException(s"commit identity mismatch for $directory")
  }
}

private object RawArchiveSink {
  val CommitMarker = "_COMMITTED"

  def queryNamespace(queryIdentity: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(queryIdentity.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

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
