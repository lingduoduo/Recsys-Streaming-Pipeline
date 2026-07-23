package com.demo.engine

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

  /** Wire source -> streaming stages -> foreachBatch(processBatch) -> start/await. */
  def run(
      spark: SparkSession, cfg: EngineConfig, source: Source,
      streamingStages: Seq[Stage], batchStages: Seq[BatchStage], sinks: Seq[Sink]
  ): Unit = {
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
}
