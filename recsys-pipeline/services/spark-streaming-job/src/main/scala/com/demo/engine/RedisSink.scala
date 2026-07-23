package com.demo.engine

import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.Pipeline

/** Persists a DataFrame to Redis: borrows a pooled Jedis per partition, pipelines
  * commands with a bounded flush, and delegates each row's command to `writeRow`. */
class RedisSink(host: String, port: Int, poolMax: Int, pipelineSize: Int,
                writeRow: (Pipeline, Row) => Unit) extends Sink {

  def write(batch: DataFrame, batchId: Long): Unit = {
    // Copy fields to locals so the partition closure doesn't capture `this`.
    val h = host; val pt = port; val mx = poolMax; val ps = pipelineSize; val wr = writeRow
    batch.foreachPartition { rows: Iterator[Row] =>
      val jedis = RedisPool.get(h, pt, mx).getResource
      try {
        val p = jedis.pipelined()
        RedisSink.foreachWithFlush(rows, ps)(r => wr(p, r))(() => p.sync())
      } finally {
        jedis.close()
      }
    }
  }
}

object RedisSink {
  /** Apply `onRow` to each element; `flush` after every `pipelineSize` elements and
    * once more at the end if any remain. Pure — the unit-test seam. */
  def foreachWithFlush[A](rows: Iterator[A], pipelineSize: Int)
                         (onRow: A => Unit)(flush: () => Unit): Unit = {
    var pending = 0
    rows.foreach { r =>
      onRow(r)
      pending += 1
      if (pending >= pipelineSize) { flush(); pending = 0 }
    }
    if (pending > 0) flush()
  }
}
