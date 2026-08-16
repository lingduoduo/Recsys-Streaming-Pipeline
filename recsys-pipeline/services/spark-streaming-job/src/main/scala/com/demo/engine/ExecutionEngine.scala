package com.demo.engine

import com.demo.event.DecodedEventFrames
import com.demo.util.BatchMetricsListener
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.storage.StorageLevel

import scala.util.control.NonFatal

object ExecutionEngine {

  private val RetryBackoffMillis = 200L

  /** Run `op`; on failure retry up to `maxRetries` more times, then rethrow. */
  def withRetry(maxRetries: Int)(op: => Unit): Unit = {
    var attempt = 0
    var done = false
    while (!done) {
      try { op; done = true }
      catch {
        case NonFatal(e) =>
          if (attempt >= maxRetries) throw e
          attempt += 1
          Thread.sleep(RetryBackoffMillis)
      }
    }
  }

  /** Fold batch stages, persist, write each sink under retry, always unpersist. */
  def processBatch(
      batch: DataFrame, batchId: Long,
      batchStages: Seq[BatchStage], sinks: Seq[Sink], maxRetries: Int
  ): Unit = {
    val records = batchStages.foldLeft(batch)((df, s) => s(df, batchId))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      sinks.foreach(sink => withRetry(maxRetries)(sink.write(records, batchId)))
    } finally {
      records.unpersist()
    }
  }

  /** Decode one source micro-batch, durably archive both outcomes, then process only valid rows. */
  def processDecodedBatch(
      batch: DataFrame,
      batchId: Long,
      decode: DataFrame => DecodedEventFrames,
      archive: RawArchiveSink,
      streamingStages: Seq[Stage],
      batchStages: Seq[BatchStage],
      sinks: Seq[Sink],
      maxRetries: Int,
      watermarkDelay: String = "10 minutes"
  ): Unit = {
    val durableSinks = requireDurableSinks(sinks)
    val decoded = decode(batch)
    val valid = decoded.valid.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val deadLetters = decoded.deadLetters.persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      withRetry(maxRetries)(archive.writeValid(valid, batchId))
      withRetry(maxRetries)(archive.writeDeadLetters(deadLetters, batchId))
      val unseenValid = archive.deduplicateValid(valid, batchId, watermarkDelay)
      val stagedValid = streamingStages.foldLeft(unseenValid)((df, stage) => stage(df))
      processDurableBatch(stagedValid, batchId, batchStages, durableSinks, archive, maxRetries)
      archive.completeBusinessBatch(valid, batchId)
    } finally {
      valid.unpersist()
      deadLetters.unpersist()
    }
  }

  private def requireDurableSinks(sinks: Seq[Sink]): Seq[DurableSink] = {
    val durable = sinks.map {
      case sink: DurableSink if Option(sink.sinkIdentity).exists(_.trim.nonEmpty) => sink
      case sink: DurableSink =>
        throw new IllegalArgumentException(
          s"Avro business sink ${sink.getClass.getName} has a blank stable identity")
      case sink =>
        throw new IllegalArgumentException(
          s"Avro business sink ${sink.getClass.getName} has no durable retry contract")
    }
    val duplicates = durable.groupBy(_.sinkIdentity).collect {
      case (identity, values) if values.size > 1 => identity
    }.toSeq.sorted
    if (duplicates.nonEmpty)
      throw new IllegalArgumentException(
        s"Avro business sink identities must be unique: ${duplicates.mkString(", ")}")
    durable
  }

  private def processDurableBatch(
      batch: DataFrame,
      batchId: Long,
      batchStages: Seq[BatchStage],
      sinks: Seq[DurableSink],
      archive: RawArchiveSink,
      maxRetries: Int
  ): Unit = {
    val records = batchStages.foldLeft(batch)((df, stage) => stage(df, batchId))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      sinks.foreach { sink =>
        val context = archive.sinkContext(sink.sinkIdentity, batchId)
        withRetry(maxRetries) {
          if (!archive.isBusinessSinkComplete(records, sink.sinkIdentity, batchId)) {
            sink.writeDurably(records, context)
            archive.completeBusinessSink(records, sink.sinkIdentity, batchId)
          }
        }
      }
    } finally records.unpersist()
  }

  /** Wire source -> streaming stages -> foreachBatch(processBatch) -> start/await. */
  def run(
      spark: SparkSession, cfg: EngineConfig, source: Source,
      streamingStages: Seq[Stage], batchStages: Seq[BatchStage], sinks: Seq[Sink]
  ): Unit = {
    // Per-batch throughput metrics (rows/rps/batchMs) for every engine adopter.
    BatchMetricsListener.register(spark)
    val streamed = streamingStages.foldLeft(source.read(spark, cfg))((df, s) => s(df))
    streamed.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        processBatch(batch, batchId, batchStages, sinks, cfg.sinkMaxRetries)
      }
      .option("checkpointLocation", cfg.checkpointLocation)
      .trigger(Trigger.ProcessingTime(cfg.triggerInterval))
      .start()
      .awaitTermination()
  }

  /** Avro source path: decode and archive inside foreachBatch so archive failure prevents progress. */
  def run(
      spark: SparkSession,
      cfg: EngineConfig,
      source: Source,
      decode: DataFrame => DecodedEventFrames,
      archive: RawArchiveSink,
      streamingStages: Seq[Stage],
      batchStages: Seq[BatchStage],
      sinks: Seq[Sink]
  ): Unit = {
    BatchMetricsListener.register(spark)
    source.read(spark, cfg).writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        processDecodedBatch(
          batch, batchId, decode, archive, streamingStages, batchStages, sinks,
          cfg.sinkMaxRetries, cfg.watermarkDelay)
      }
      .option("checkpointLocation", cfg.checkpointLocation)
      .trigger(Trigger.ProcessingTime(cfg.triggerInterval))
      .start()
      .awaitTermination()
  }
}
