package com.demo.engine

import java.util
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

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
  * Each item has one deterministic hash. Batch fields older than the configured recovery window
  * are pruned only while applying a later batch; the minimum window retains N and N-1.
  */
object RedisBatchLedger {
  type Eval = (String, util.List[String], util.List[String]) => Object

  private val IncrementScript =
    "local lt=redis.call('TYPE',KEYS[1]).ok; local tt=redis.call('TYPE',KEYS[2]).ok; " +
      "if lt~='none' and lt~='hash' then return redis.error_reply('ledger key must be a hash') end; " +
      "if tt~='none' and tt~='zset' then return redis.error_reply('target key must be a zset') end; " +
      "local batch=tonumber(ARGV[1]); local window=tonumber(ARGV[2]); local amount=tonumber(ARGV[3]); " +
      "if not batch or not window or window<2 or not amount then return redis.error_reply('invalid ledger arguments') end; " +
      "if redis.call('HEXISTS',KEYS[1],ARGV[1])==1 then return 0 end; " +
      "for _,f in ipairs(redis.call('HKEYS',KEYS[1])) do local b=tonumber(f); " +
      "if b and b < batch-window+1 then redis.call('HDEL',KEYS[1],f) end end; " +
      "redis.call('ZINCRBY',KEYS[2],ARGV[3],ARGV[4]); " +
      "redis.call('HSET',KEYS[1],ARGV[1],'1'); return 1"

  private val HashScript =
    "local lt=redis.call('TYPE',KEYS[1]).ok; local tt=redis.call('TYPE',KEYS[2]).ok; " +
      "if lt~='none' and lt~='hash' then return redis.error_reply('ledger key must be a hash') end; " +
      "if tt~='none' and tt~='hash' then return redis.error_reply('target key must be a hash') end; " +
      "local batch=tonumber(ARGV[1]); local window=tonumber(ARGV[2]); local ttl=tonumber(ARGV[3]); " +
      "if not batch or not window or window<2 or not ttl or ttl<=0 or (#ARGV-3)%2~=0 then " +
      "return redis.error_reply('invalid ledger arguments') end; " +
      "if redis.call('HEXISTS',KEYS[1],ARGV[1])==1 then return 0 end; " +
      "for _,f in ipairs(redis.call('HKEYS',KEYS[1])) do local b=tonumber(f); " +
      "if b and b < batch-window+1 then redis.call('HDEL',KEYS[1],f) end end; " +
      "for i=4,#ARGV,2 do redis.call('HSET',KEYS[2],ARGV[i],ARGV[i+1]) end; " +
      "redis.call('EXPIRE',KEYS[2],ttl); redis.call('HSET',KEYS[1],ARGV[1],'1'); return 1"

  def ledgerKey(context: SinkWriteContext, purpose: String, effectId: String): String = {
    require(Option(purpose).exists(_.trim.nonEmpty), "Redis ledger purpose must not be blank")
    require(Option(effectId).exists(_.trim.nonEmpty), "Redis ledger effect must not be blank")
    val effectHash = MessageDigest.getInstance("SHA-256")
      .digest(effectId.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x").mkString
    s"recsys:batch-ledger:${context.queryNamespace}:${context.sinkNamespace}:$purpose:$effectHash"
  }

  def incrementOnce(
      eval: Eval,
      ledgerKey: String,
      sortedSetKey: String,
      effectId: String,
      amount: Double,
      member: String,
      batchId: Long,
      retainedBatchWindow: Int = 2
  ): Unit = {
    eval(
      IncrementScript,
      Seq(ledgerKey, sortedSetKey).asJava,
      Seq(batchId.toString, retainedBatchWindow.toString, amount.toString, member).asJava)
    ()
  }

  def hashOnce(
      eval: Eval,
      ledgerKey: String,
      hashKey: String,
      effectId: String,
      ttlSeconds: Int,
      fields: Map[String, String],
      batchId: Long,
      retainedBatchWindow: Int = 2
  ): Unit = {
    val fieldArguments = fields.toSeq.sortBy(_._1).flatMap { case (field, value) => Seq(field, value) }
    eval(
      HashScript,
      Seq(ledgerKey, hashKey).asJava,
      (Seq(batchId.toString, retainedBatchWindow.toString, ttlSeconds.toString) ++ fieldArguments).asJava)
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
    popularityKey: String = "global:item_popularity",
    ledgerRetentionBatches: Int = 2
) extends DurableSink {
  require(Option(popularityKey).exists(_.trim.nonEmpty), "Redis popularity key must not be blank")
  require(ledgerRetentionBatches >= 2, "Redis ledger retention must preserve batches N and N-1")

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
    val retention = ledgerRetentionBatches
    counts(batch).foreachPartition { rows: Iterator[Row] =>
      val jedis = RedisPool.get(h, pt, mx).getResource
      try rows.foreach { row =>
        val itemId = row.getAs[String]("item_id")
        val ledger = RedisBatchLedger.ledgerKey(context, "popularity", itemId)
        RedisBatchLedger.incrementOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger,
          key,
          itemId,
          row.getAs[Long]("count").toDouble,
          itemId,
          context.batchId,
          retention)
      }
      finally jedis.close()
    }
  }
}
