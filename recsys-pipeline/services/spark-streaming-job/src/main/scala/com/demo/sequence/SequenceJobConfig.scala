package com.demo.sequence

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/** Shared sequence-store knobs, read once per job. */
final case class SequenceJobConfig(
    lookbackDays: Int,
    maxRowsPerBucket: Int,
    parquetPath: Option[String]
) {
  def ttlSeconds: Int = lookbackDays * 24 * 3600
}

object SequenceJobConfig {

  private def intFromMap(env: Map[String, String], key: String, default: Int): Int =
    env.get(key).flatMap(v => try Some(v.toInt) catch { case _: NumberFormatException => None }).getOrElse(default)

  /** Pure: builds the config from an explicit environment map. */
  def from(env: Map[String, String]): SequenceJobConfig = SequenceJobConfig(
    lookbackDays     = math.max(1, intFromMap(env, "SEQ_LOOKBACK_DAYS", 90)),
    maxRowsPerBucket = math.max(1, intFromMap(env, "SEQ_MAX_ROWS_PER_BUCKET", 500)),
    parquetPath      = env.get("SEQ_PARQUET_PATH").filter(_.nonEmpty)
  )

  /** Reads the real process environment. */
  def fromEnv(): SequenceJobConfig = from(sys.env)
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
