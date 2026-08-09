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

/** Atomic, bounded Redis ledgers for retrying a partially acknowledged micro-batch.
  *
  * One hash records the effects of one query/sink/batch, while a stable state hash carries the
  * monotonic committed-batch watermark and a sorted-set index names the retained batch hashes.
  * Effect scripts fence work below the committed recovery floor. Pruning happens only in
  * `completeBatch`, after the caller has durably completed every partition of the batch.
  */
object RedisBatchLedger {
  type Eval = (String, util.List[String], util.List[String]) => Object

  private val IncrementScript =
    "local lt=redis.call('TYPE',KEYS[1]).ok; local st=redis.call('TYPE',KEYS[2]).ok; " +
      "local it=redis.call('TYPE',KEYS[3]).ok; local tt=redis.call('TYPE',KEYS[4]).ok; " +
      "if lt~='none' and lt~='hash' then return redis.error_reply('ledger key must be a hash') end; " +
      "if st~='none' and st~='hash' then return redis.error_reply('ledger state key must be a hash') end; " +
      "if it~='none' and it~='zset' then return redis.error_reply('ledger index key must be a zset') end; " +
      "if tt~='none' and tt~='zset' then return redis.error_reply('target key must be a zset') end; " +
      "local batch=tonumber(ARGV[1]); local window=tonumber(ARGV[2]); local amount=tonumber(ARGV[4]); " +
      "if not batch or not window or window<2 or ARGV[3]=='' or not amount or ARGV[5]=='' then " +
      "return redis.error_reply('invalid ledger arguments') end; " +
      "local raw=redis.call('HGET',KEYS[2],'committed_batch'); local watermark=tonumber(raw); " +
      "if raw and not watermark then return redis.error_reply('committed batch watermark must be numeric') end; " +
      "if watermark and batch < watermark-window+1 then return -1 end; " +
      "if redis.call('HEXISTS',KEYS[1],ARGV[3])==1 then return 0 end; " +
      "redis.call('ZINCRBY',KEYS[4],ARGV[4],ARGV[5]); " +
      "redis.call('HSET',KEYS[1],ARGV[3],'1'); redis.call('ZADD',KEYS[3],batch,KEYS[1]); return 1"

  private val HashScript =
    "local lt=redis.call('TYPE',KEYS[1]).ok; local st=redis.call('TYPE',KEYS[2]).ok; " +
      "local it=redis.call('TYPE',KEYS[3]).ok; local tt=redis.call('TYPE',KEYS[4]).ok; " +
      "if lt~='none' and lt~='hash' then return redis.error_reply('ledger key must be a hash') end; " +
      "if st~='none' and st~='hash' then return redis.error_reply('ledger state key must be a hash') end; " +
      "if it~='none' and it~='zset' then return redis.error_reply('ledger index key must be a zset') end; " +
      "if tt~='none' and tt~='hash' then return redis.error_reply('target key must be a hash') end; " +
      "local batch=tonumber(ARGV[1]); local window=tonumber(ARGV[2]); local ttl=tonumber(ARGV[3]); " +
      "if not batch or not window or window<2 or not ttl or ttl<=0 or ARGV[4]=='' or (#ARGV-4)%2~=0 then " +
      "return redis.error_reply('invalid ledger arguments') end; " +
      "local raw=redis.call('HGET',KEYS[2],'committed_batch'); local watermark=tonumber(raw); " +
      "if raw and not watermark then return redis.error_reply('committed batch watermark must be numeric') end; " +
      "if watermark and batch < watermark-window+1 then return -1 end; " +
      "if redis.call('HEXISTS',KEYS[1],ARGV[4])==1 then return 0 end; " +
      "for i=5,#ARGV,2 do redis.call('HSET',KEYS[4],ARGV[i],ARGV[i+1]) end; " +
      "redis.call('EXPIRE',KEYS[4],ttl); redis.call('HSET',KEYS[1],ARGV[4],'1'); " +
      "redis.call('EXPIRE',KEYS[1],ttl); redis.call('ZADD',KEYS[3],batch,KEYS[1]); " +
      "redis.call('EXPIRE',KEYS[3],ttl); return 1"

  private val CompleteScript =
    "local st=redis.call('TYPE',KEYS[1]).ok; local it=redis.call('TYPE',KEYS[2]).ok; " +
      "if st~='none' and st~='hash' then return redis.error_reply('ledger state key must be a hash') end; " +
      "if it~='none' and it~='zset' then return redis.error_reply('ledger index key must be a zset') end; " +
      "local batch=tonumber(ARGV[1]); local window=tonumber(ARGV[2]); local ttl=tonumber(ARGV[3]); " +
      "if not batch or not window or window<2 or not ttl or ttl<0 then return redis.error_reply('invalid ledger arguments') end; " +
      "local raw=redis.call('HGET',KEYS[1],'committed_batch'); local watermark=tonumber(raw); " +
      "if raw and not watermark then return redis.error_reply('committed batch watermark must be numeric') end; " +
      "if watermark and batch < watermark-window+1 then return -1 end; " +
      "local committed=batch; if watermark and watermark>batch then committed=watermark end; " +
      "redis.call('HSET',KEYS[1],'committed_batch',committed); local floor=committed-window+1; " +
      "for _,key in ipairs(redis.call('ZRANGEBYSCORE',KEYS[2],'-inf',floor-1)) do " +
      "redis.call('DEL',key); redis.call('ZREM',KEYS[2],key) end; " +
      "if ttl>0 then redis.call('EXPIRE',KEYS[1],ttl); " +
      "if redis.call('EXISTS',KEYS[2])==1 then redis.call('EXPIRE',KEYS[2],ttl) end end; " +
      "return committed"

  def namespace(context: SinkWriteContext, purpose: String): String = {
    require(Option(purpose).exists(_.trim.nonEmpty), "Redis ledger purpose must not be blank")
    s"recsys:batch-ledger:${context.queryNamespace}:${context.sinkNamespace}:$purpose:"
  }

  def ledgerKey(context: SinkWriteContext, purpose: String): String =
    s"${namespace(context, purpose)}batch:${context.batchId}"

  def stateKey(context: SinkWriteContext, purpose: String): String =
    s"${namespace(context, purpose)}state"

  def indexKey(context: SinkWriteContext, purpose: String): String =
    s"${namespace(context, purpose)}index"

  private def asLong(result: Object): Long = result match {
    case number: java.lang.Number => number.longValue()
    case other => throw new IllegalStateException(s"Redis ledger script returned non-numeric result $other")
  }

  def incrementOnce(
      eval: Eval,
      ledgerKey: String,
      stateKey: String,
      indexKey: String,
      sortedSetKey: String,
      effectId: String,
      amount: Double,
      member: String,
      batchId: Long,
      retainedBatchWindow: Int = 2
  ): Long =
    asLong(eval(
      IncrementScript,
      Seq(ledgerKey, stateKey, indexKey, sortedSetKey).asJava,
      Seq(batchId.toString, retainedBatchWindow.toString, effectId, amount.toString, member).asJava))

  def hashOnce(
      eval: Eval,
      ledgerKey: String,
      stateKey: String,
      indexKey: String,
      hashKey: String,
      effectId: String,
      ttlSeconds: Int,
      fields: Map[String, String],
      batchId: Long,
      retainedBatchWindow: Int = 2
  ): Long = {
    val fieldArguments = fields.toSeq.sortBy(_._1).flatMap { case (field, value) => Seq(field, value) }
    asLong(eval(
      HashScript,
      Seq(ledgerKey, stateKey, indexKey, hashKey).asJava,
      (Seq(batchId.toString, retainedBatchWindow.toString, ttlSeconds.toString, effectId) ++
        fieldArguments).asJava))
  }

  def completeBatch(
      eval: Eval,
      stateKey: String,
      indexKey: String,
      batchId: Long,
      retainedBatchWindow: Int = 2,
      ledgerTtlSeconds: Int = 0
  ): Long =
    asLong(eval(
      CompleteScript,
      Seq(stateKey, indexKey).asJava,
      Seq(batchId.toString, retainedBatchWindow.toString, ledgerTtlSeconds.toString).asJava))
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
        val ledger = RedisBatchLedger.ledgerKey(context, "popularity")
        RedisBatchLedger.incrementOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger,
          RedisBatchLedger.stateKey(context, "popularity"),
          RedisBatchLedger.indexKey(context, "popularity"),
          key,
          itemId,
          row.getAs[Long]("count").toDouble,
          itemId,
          context.batchId,
          retention)
      }
      finally jedis.close()
    }
    val jedis = RedisPool.get(h, pt, mx).getResource
    try RedisBatchLedger.completeBatch(
      (script, keys, arguments) => jedis.eval(script, keys, arguments),
      RedisBatchLedger.stateKey(context, "popularity"),
      RedisBatchLedger.indexKey(context, "popularity"),
      context.batchId,
      retention)
    finally jedis.close()
  }
}
