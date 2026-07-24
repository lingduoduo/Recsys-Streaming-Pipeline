package com.demo.sequence

import com.demo.util.Env
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/** Shared sequence-store knobs, read once per job. */
final case class SequenceJobConfig(
    bucketWidth: String,
    lookbackDays: Int,
    maxRowsPerBucket: Int,
    parquetPath: Option[String]
) {
  def ttlSeconds: Int = lookbackDays * 24 * 3600
}

object SequenceJobConfig {
  def fromEnv(): SequenceJobConfig = SequenceJobConfig(
    bucketWidth      = sys.env.getOrElse("SEQ_BUCKET_WIDTH", "day"),
    lookbackDays     = math.max(1, Env.int("SEQ_LOOKBACK_DAYS", 90)),
    maxRowsPerBucket = math.max(1, Env.int("SEQ_MAX_ROWS_PER_BUCKET", 500)),
    parquetPath      = sys.env.get("SEQ_PARQUET_PATH").filter(_.nonEmpty)
  )
}

/** Fans one chunk DataFrame out to the Redis and Parquet sinks. Shared by both
  * streaming producers and the backfill job. */
object SequenceSinks {

  def write(
      chunks: DataFrame,
      cfg: SequenceJobConfig,
      redisHost: String,
      redisPort: Int,
      poolMax: Int,
      pipelineSize: Int,
      mode: SequenceWriteMode,
      batchId: Long,
      writeRedis: Boolean = true
  ): Unit = {
    // chunks is consumed by up to two sinks; without persist Spark recomputes the groupBy.
    chunks.persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      if (writeRedis) {
        new SequenceRedisSink(
          redisHost, redisPort, poolMax, pipelineSize,
          cfg.ttlSeconds, cfg.maxRowsPerBucket, mode
        ).write(chunks, batchId)
      }
      cfg.parquetPath.foreach { path =>
        new SequenceParquetSink(path, mode).write(chunks, batchId)
      }
    } finally {
      chunks.unpersist()
    }
  }
}
