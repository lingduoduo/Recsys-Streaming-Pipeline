package com.demo.sequence

import com.demo.engine.{DurableSink, SinkWriteContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/** Shared sequence-store knobs, read once per job.
  *
  * `writeEnabled` is the producer-side kill switch, mirroring `recsys.sequence.mode` on the
  * serving side: with it off, a sequence-store Redis problem can be taken off both streaming
  * jobs' critical path at restart instead of by a code change. It gates the Redis write only —
  * the optional Parquet mirror is unaffected, since a Redis outage is no reason to stop it.
  */
final case class SequenceJobConfig(
    lookbackDays: Int,
    maxRowsPerBucket: Int,
    parquetPath: Option[String],
    ledgerRetentionBatches: Int = 2,
    writeEnabled: Boolean = true
) {
  def ttlSeconds: Int = lookbackDays * 24 * 3600
}

object SequenceJobConfig {

  private def intFromMap(env: Map[String, String], key: String, default: Int): Int =
    env.get(key).flatMap(v => try Some(v.toInt) catch { case _: NumberFormatException => None }).getOrElse(default)

  /** Only an explicit `true`/`false` counts. A typo must not silently stop the sequence write. */
  private def booleanFromMap(env: Map[String, String], key: String, default: Boolean): Boolean =
    env.get(key).map(_.trim.toLowerCase) match {
      case Some("true")  => true
      case Some("false") => false
      case _             => default
    }

  /** Pure: builds the config from an explicit environment map. */
  def from(env: Map[String, String]): SequenceJobConfig = SequenceJobConfig(
    lookbackDays     = math.max(1, intFromMap(env, "SEQ_LOOKBACK_DAYS", 90)),
    maxRowsPerBucket = math.max(1, intFromMap(env, "SEQ_MAX_ROWS_PER_BUCKET", 500)),
    parquetPath      = env.get("SEQ_PARQUET_PATH").filter(_.nonEmpty),
    ledgerRetentionBatches = math.max(2, intFromMap(env, "REDIS_LEDGER_RETENTION_BATCHES", 2)),
    writeEnabled     = booleanFromMap(env, "SEQ_WRITE_ENABLED", default = true)
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
      if (writeRedis && cfg.writeEnabled) {
        new SequenceRedisSink(
          redisHost, redisPort, poolMax, pipelineSize,
          cfg.ttlSeconds, cfg.maxRowsPerBucket, mode, cfg.ledgerRetentionBatches
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

/** Composite durable sink used by the Avro user-event job. Redis effects have an atomic
  * per-key ledger and the optional Parquet mirror uses a deterministic batch commit. This lets
  * the whole composite safely retry when either inner write fails after the other succeeded.
  */
class SequenceBusinessSink(
    cfg: SequenceJobConfig,
    redisHost: String,
    redisPort: Int,
    poolMax: Int,
    pipelineSize: Int,
    mode: SequenceWriteMode,
    toChunks: DataFrame => DataFrame,
    override val sinkIdentity: String
) extends DurableSink {
  require(Option(sinkIdentity).exists(_.trim.nonEmpty), "sequence sink identity must not be blank")

  override def write(batch: DataFrame, batchId: Long): Unit =
    SequenceSinks.write(
      toChunks(batch), cfg, redisHost, redisPort, poolMax, pipelineSize, mode, batchId)

  override def writeDurably(batch: DataFrame, context: SinkWriteContext): Unit = {
    val chunks = toChunks(batch).persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      // This path calls the Redis sink directly rather than through SequenceSinks.write, so it
      // carries its own copy of the kill-switch gate. Gating only one of the two would leave
      // the Avro user-event job — the main producer — still writing.
      if (cfg.writeEnabled) {
        new SequenceRedisSink(
          redisHost, redisPort, poolMax, pipelineSize,
          cfg.ttlSeconds, cfg.maxRowsPerBucket, mode, cfg.ledgerRetentionBatches
        ).writeDurably(chunks, context)
      }
      cfg.parquetPath.foreach { path =>
        new SequenceParquetSink(path, mode).writeDurably(chunks, context)
      }
    } finally chunks.unpersist()
  }
}
