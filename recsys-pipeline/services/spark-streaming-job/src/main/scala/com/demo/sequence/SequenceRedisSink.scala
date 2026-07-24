package com.demo.sequence

import com.demo.engine.{RedisPool, RedisSink, Sink}
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisException

import scala.collection.JavaConverters._

sealed trait SequenceWriteMode
object SequenceWriteMode {
  /** Streaming producers: read the existing bucket and concatenate. */
  case object Append extends SequenceWriteMode
  /** Backfill: replace the bucket outright, so re-runs are idempotent and skip the read. */
  case object Overwrite extends SequenceWriteMode
}

/** Persists column chunks to Redis, one HASH per `(user, kind, bucket)` partition. */
class SequenceRedisSink(
    host: String,
    port: Int,
    poolMax: Int,
    pipelineSize: Int,
    ttlSeconds: Int,
    maxRowsPerBucket: Int,
    mode: SequenceWriteMode
) extends Sink {

  def write(batch: DataFrame, batchId: Long): Unit = {
    // Copy to locals so the partition closure doesn't capture `this`.
    val h = host; val pt = port; val mx = poolMax; val ps = pipelineSize
    val ttl = ttlSeconds; val cap = maxRowsPerBucket; val m = mode
    val fields = (SequenceSchema.Columns :+ SequenceSchema.ColCount).toArray

    batch.foreachPartition { rows: Iterator[Row] =>
      val log = LoggerFactory.getLogger(classOf[SequenceRedisSink])
      val jedis = RedisPool.get(h, pt, mx).getResource
      try {
        // Phase 1 — direct, NON-pipelined reads. Issuing a read inside an open pipeline
        // makes it consume buffered pipeline replies and corrupts the connection.
        val writes = scala.collection.mutable.ArrayBuffer.empty[(String, java.util.Map[String, String])]
        var total = 0
        var skipped = 0
        rows.foreach { row =>
          total += 1
          var key = "<unresolved-key>"
          try {
            key = SequenceSchema.key(
              row.getAs[String]("user_id"), row.getAs[String]("kind"), row.getAs[String]("bucket")
            )
            val fresh = SequenceRedisSink.chunkFields(row)
            val existing = m match {
              case SequenceWriteMode.Overwrite => Map.empty[String, String]
              case SequenceWriteMode.Append =>
                val values = jedis.hmget(key, fields: _*).asScala
                fields.zip(values).collect { case (f, v) if v != null => f -> v }.toMap
            }
            writes += key -> SequenceRedisSink.resolve(existing, fresh, cap, m).asJava
          } catch {
            case e: Exception if SequenceRedisSink.isFatal(e) => throw e
            case e: Exception =>
              skipped += 1
              log.warn("Skipping sequence chunk for key {}: {}", Array[AnyRef](key, e.getMessage): _*)
          }
        }
        if (skipped > 0) {
          log.warn("Skipped {} of {} rows in partition due to per-row errors", skipped, total)
        }

        // Phase 2 — all writes through one pipeline, no reads interleaved.
        if (writes.nonEmpty) {
          val p = jedis.pipelined()
          RedisSink.foreachWithFlush(writes.iterator, ps) { case (key, f) =>
            p.hset(key, f)
            p.expire(key, ttl)
          }(() => p.sync())
        }
      } finally {
        jedis.close()
      }
    }
  }
}

object SequenceRedisSink {

  /** Infrastructure failures must fail the batch so Spark retries; per-row data
    * problems are skipped. */
  def isFatal(e: Throwable): Boolean = e.isInstanceOf[JedisException]

  /** Row → field map. Pure, so the column extraction is testable without Redis. */
  def chunkFields(row: Row): Map[String, String] = {
    val data = SequenceSchema.Columns.map { column =>
      column -> Option(row.getAs[String](column)).getOrElse("")
    }.toMap
    data + (SequenceSchema.ColCount -> row.getAs[Long](SequenceSchema.ColCount).toString)
  }

  /** Mode dispatch. Pure, so both write modes are testable without Redis. */
  def resolve(
      existing: Map[String, String],
      fresh: Map[String, String],
      maxRows: Int,
      mode: SequenceWriteMode
  ): Map[String, String] = mode match {
    case SequenceWriteMode.Overwrite => SequenceCodec.merge(Map.empty, fresh, maxRows)
    case SequenceWriteMode.Append    => SequenceCodec.merge(existing, fresh, maxRows)
  }
}
