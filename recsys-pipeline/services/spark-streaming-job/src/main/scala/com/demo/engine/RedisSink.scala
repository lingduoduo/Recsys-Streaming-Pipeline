package com.demo.engine

import java.util

import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.Pipeline

import scala.collection.JavaConverters._

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

/** Atomic, per-effect Redis ledgers for retrying a partially acknowledged micro-batch.
  * Ledger hashes deliberately have no TTL: they must remain valid for as long as the
  * corresponding Structured Streaming checkpoint can replay its batch id.
  */
object RedisBatchLedger {
  type Eval = (String, util.List[String], util.List[String]) => Object

  private val IncrementScript =
    "if redis.call('HSETNX', KEYS[1], ARGV[1], '1') == 1 then " +
      "redis.call('ZINCRBY', KEYS[2], ARGV[2], ARGV[3]); return 1 else return 0 end"

  private val HashScript =
    "if redis.call('HSETNX', KEYS[1], ARGV[1], '1') == 1 then " +
      "for i = 3, #ARGV, 2 do redis.call('HSET', KEYS[2], ARGV[i], ARGV[i + 1]) end; " +
      "redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2])); return 1 else return 0 end"

  def ledgerKey(context: SinkWriteContext, purpose: String): String = {
    require(Option(purpose).exists(_.trim.nonEmpty), "Redis ledger purpose must not be blank")
    s"recsys:batch-ledger:${context.queryNamespace}:${context.sinkNamespace}:${context.batchId}:$purpose"
  }

  def incrementOnce(
      eval: Eval,
      ledgerKey: String,
      sortedSetKey: String,
      effectId: String,
      amount: Double,
      member: String
  ): Unit = {
    eval(
      IncrementScript,
      Seq(ledgerKey, sortedSetKey).asJava,
      Seq(effectId, amount.toString, member).asJava)
    ()
  }

  def hashOnce(
      eval: Eval,
      ledgerKey: String,
      hashKey: String,
      effectId: String,
      ttlSeconds: Int,
      fields: Map[String, String]
  ): Unit = {
    val fieldArguments = fields.toSeq.sortBy(_._1).flatMap { case (field, value) => Seq(field, value) }
    eval(
      HashScript,
      Seq(ledgerKey, hashKey).asJava,
      (Seq(effectId, ttlSeconds.toString) ++ fieldArguments).asJava)
    ()
  }
}

/** User-event popularity sink. Every item increment and its batch-ledger marker execute in one
  * Redis Lua script, so retrying after a lost response cannot increment that item twice.
  */
class RedisPopularitySink(
    host: String,
    port: Int,
    poolMax: Int,
    popularityKey: String = "global:item_popularity"
) extends DurableSink {
  require(Option(popularityKey).exists(_.trim.nonEmpty), "Redis popularity key must not be blank")

  override val sinkIdentity: String = s"redis-popularity:$popularityKey"

  private def counts(batch: DataFrame): DataFrame = batch.groupBy("item_id").count()

  override def write(batch: DataFrame, batchId: Long): Unit = {
    val h = host; val pt = port; val mx = poolMax; val key = popularityKey
    counts(batch).foreachPartition { rows: Iterator[Row] =>
      val jedis = RedisPool.get(h, pt, mx).getResource
      try rows.foreach { row =>
        jedis.zincrby(key, row.getAs[Long]("count").toDouble, row.getAs[String]("item_id"))
      }
      finally jedis.close()
    }
  }

  override def writeDurably(batch: DataFrame, context: SinkWriteContext): Unit = {
    val h = host; val pt = port; val mx = poolMax; val key = popularityKey
    val ledger = RedisBatchLedger.ledgerKey(context, "popularity")
    counts(batch).foreachPartition { rows: Iterator[Row] =>
      val jedis = RedisPool.get(h, pt, mx).getResource
      try rows.foreach { row =>
        val itemId = row.getAs[String]("item_id")
        RedisBatchLedger.incrementOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger,
          key,
          itemId,
          row.getAs[Long]("count").toDouble,
          itemId)
      }
      finally jedis.close()
    }
  }
}
