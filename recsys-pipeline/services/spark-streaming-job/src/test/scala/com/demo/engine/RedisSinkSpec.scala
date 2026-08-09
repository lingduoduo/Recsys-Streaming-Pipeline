package com.demo.engine

import java.net.ServerSocket
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException

import scala.collection.JavaConverters._

class RedisSinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var redisProcess: Process = _
  private var redisPort: Int = _

  override protected def beforeAll(): Unit = {
    val socket = new ServerSocket(0)
    redisPort = socket.getLocalPort
    socket.close()
    redisProcess = new ProcessBuilder(
      sys.env.getOrElse("REDIS_SERVER_BINARY", "redis-server"),
      "--port", redisPort.toString, "--bind", "127.0.0.1",
      "--save", "", "--appendonly", "no")
      .redirectErrorStream(true)
      .start()
    val deadline = System.nanoTime() + 5000000000L
    var ready = false
    while (!ready && System.nanoTime() < deadline) {
      try {
        val jedis = new Jedis("127.0.0.1", redisPort)
        try ready = jedis.ping() == "PONG" finally jedis.close()
      } catch { case _: Exception => Thread.sleep(25L) }
    }
    if (!ready) throw new IllegalStateException("test Redis did not start")
  }

  override protected def afterAll(): Unit =
    if (redisProcess != null) redisProcess.destroy()

  private def context(batchId: Long): SinkWriteContext =
    SinkWriteContext(
      "checkpoint://redis", "query-ns", "redis:popularity", "sink-ns", batchId)

  private def eval(jedis: Jedis): RedisBatchLedger.Eval =
    (script, keys, arguments) => jedis.eval(script, keys, arguments)

  private def sequenceContext(batchId: Long): SinkWriteContext =
    SinkWriteContext(
      "checkpoint://sequence", "query-seq", "sequence:user-events", "sink-seq", batchId)

  private def hash(
      jedis: Jedis,
      batchId: Long,
      target: String,
      effectId: String,
      ttlSeconds: Int,
      fields: Map[String, String]
  ): Long = {
    val writeContext = sequenceContext(batchId)
    RedisBatchLedger.hashOnce(
      eval(jedis),
      RedisBatchLedger.ledgerKey(writeContext, "sequence"),
      RedisBatchLedger.stateKey(writeContext, "sequence"),
      RedisBatchLedger.indexKey(writeContext, "sequence"),
      target,
      effectId,
      ttlSeconds,
      fields,
      batchId)
  }

  private def awaitPttlAtMost(jedis: Jedis, key: String, upperBoundMillis: Long): Long = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L)
    var remaining = jedis.pttl(key)
    while (remaining > upperBoundMillis && System.nanoTime() < deadline) {
      Thread.sleep(10L)
      remaining = jedis.pttl(key)
    }
    withClue(s"PTTL for $key") {
      remaining should be > 0L
      remaining should be <= upperBoundMillis
    }
    remaining
  }

  private def increment(
      jedis: Jedis,
      batchId: Long,
      effectId: String,
      amount: Double = 1.0,
      retainedBatchWindow: Int = 2
  ): Long = {
    val writeContext = context(batchId)
    RedisBatchLedger.incrementOnce(
      eval(jedis),
      RedisBatchLedger.ledgerKey(writeContext, "popularity"),
      RedisBatchLedger.stateKey(writeContext, "popularity"),
      RedisBatchLedger.indexKey(writeContext, "popularity"),
      "global:item_popularity",
      effectId,
      amount,
      effectId,
      batchId,
      retainedBatchWindow)
  }

  private def complete(
      jedis: Jedis,
      batchId: Long,
      retainedBatchWindow: Int = 2,
      ledgerTtlSeconds: Int = 0
  ): Long = {
    val writeContext = context(batchId)
    RedisBatchLedger.completeBatch(
      eval(jedis),
      RedisBatchLedger.stateKey(writeContext, "popularity"),
      RedisBatchLedger.indexKey(writeContext, "popularity"),
      batchId,
      retainedBatchWindow,
      ledgerTtlSeconds)
  }

  "foreachWithFlush" should "apply onRow to every element and flush on the cadence" in {
    def run(n: Int, size: Int): (Int, Int) = {
      var rows = 0
      var flushes = 0
      RedisSink.foreachWithFlush((1 to n).iterator, size)(_ => rows += 1)(() => flushes += 1)
      (rows, flushes)
    }
    run(3, 2) shouldBe (3, 2)  // flush after 2, final flush for the 3rd
    run(4, 2) shouldBe (4, 2)  // exact multiple, no extra final flush
    run(1, 2) shouldBe (1, 1)  // one pending -> final flush
    run(0, 2) shouldBe (0, 0)  // nothing to do
  }

  "RedisBatchLedger.incrementOnce" should "not repeat an acknowledged increment after interruption" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val writeContext = context(8L)
      val ledger = RedisBatchLedger.ledgerKey(writeContext, "popularity")
      var failAfterFirstApply = true
      val lostResponse: RedisBatchLedger.Eval = (script, keys, arguments) => {
        val result = jedis.eval(script, keys, arguments)
        if (failAfterFirstApply) {
          failAfterFirstApply = false
          throw new RuntimeException("connection lost after Redis applied the script")
        }
        result
      }

      intercept[RuntimeException] {
        RedisBatchLedger.incrementOnce(
          lostResponse,
          ledger,
          RedisBatchLedger.stateKey(writeContext, "popularity"),
          RedisBatchLedger.indexKey(writeContext, "popularity"),
          "global:item_popularity",
          "item-1",
          3.0,
          "item-1",
          8L)
      }
      increment(jedis, 8L, "item-1", 3.0) shouldBe 0L

      jedis.zscore("global:item_popularity", "item-1") shouldBe 3.0
      jedis.hkeys(ledger).asScala should contain only "item-1"
    } finally jedis.close()
  }

  it should "leave no ledger marker when a wrong-type popularity target rejects the effect" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val writeContext = context(8L)
      val ledger = RedisBatchLedger.ledgerKey(writeContext, "popularity")
      jedis.set("wrong-target", "not-a-zset")

      intercept[JedisDataException] {
        RedisBatchLedger.incrementOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger,
          RedisBatchLedger.stateKey(writeContext, "popularity"),
          RedisBatchLedger.indexKey(writeContext, "popularity"),
          "wrong-target", "item-1", 3.0, "item-1", 8L)
      }

      jedis.exists(ledger) shouldBe false
      jedis.get("wrong-target") shouldBe "not-a-zset"
    } finally jedis.close()
  }

  it should "leave the popularity target unchanged when the ledger has the wrong type" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val writeContext = context(8L)
      val ledger = RedisBatchLedger.ledgerKey(writeContext, "popularity")
      jedis.set(ledger, "not-a-hash")

      intercept[JedisDataException] {
        RedisBatchLedger.incrementOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger,
          RedisBatchLedger.stateKey(writeContext, "popularity"),
          RedisBatchLedger.indexKey(writeContext, "popularity"),
          "global:item_popularity", "item-1", 3.0, "item-1", 8L)
      }

      jedis.zscore("global:item_popularity", "item-1") shouldBe null
      jedis.get(ledger) shouldBe "not-a-hash"
    } finally jedis.close()
  }

  "RedisBatchLedger.hashOnce" should "leave no ledger marker when a wrong-type sequence target rejects the effect" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val writeContext = SinkWriteContext(
        "checkpoint://sequence", "query-ns", "sequence:user-events", "sink-ns", 12L)
      val sequenceKey = "seq:u1:click:20260809"
      val ledger = RedisBatchLedger.ledgerKey(writeContext, "sequence")
      jedis.set(sequenceKey, "not-a-hash")

      intercept[JedisDataException] {
        RedisBatchLedger.hashOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger,
          RedisBatchLedger.stateKey(writeContext, "sequence"),
          RedisBatchLedger.indexKey(writeContext, "sequence"),
          sequenceKey, sequenceKey, 3600, Map("item_id" -> "m1", "n" -> "1"), 12L)
      }

      jedis.exists(ledger) shouldBe false
      jedis.get(sequenceKey) shouldBe "not-a-hash"
    } finally jedis.close()
  }

  it should "advance the fence only after completion and skip delayed work below the recovery horizon" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      increment(jedis, 10L, "batch-10") shouldBe 1L
      complete(jedis, 10L) shouldBe 10L
      val batch10Ledger = RedisBatchLedger.ledgerKey(context(10L), "popularity")

      increment(jedis, 12L, "batch-12") shouldBe 1L
      jedis.exists(batch10Ledger) shouldBe true
      complete(jedis, 12L) shouldBe 12L

      jedis.exists(batch10Ledger) shouldBe false
      increment(jedis, 10L, "delayed-batch-10", amount = 5.0) shouldBe -1L
      increment(jedis, 12L, "batch-12") shouldBe 0L
      jedis.zscore("global:item_popularity", "batch-10") shouldBe 1.0
      jedis.zscore("global:item_popularity", "batch-12") shouldBe 1.0
      jedis.zscore("global:item_popularity", "delayed-batch-10") shouldBe null
      jedis.hget(RedisBatchLedger.stateKey(context(12L), "popularity"), "committed_batch") shouldBe "12"
    } finally jedis.close()
  }

  it should "keep key and field cardinality bounded by the retained batches and their effects" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      (1L to 20L).foreach { batchId =>
        (1 to 3).foreach(effect => increment(jedis, batchId, s"$batchId-$effect"))
        complete(jedis, batchId)
      }

      val namespace = RedisBatchLedger.namespace(context(20L), "popularity")
      jedis.keys(s"$namespace*").asScala shouldBe Set(
        RedisBatchLedger.stateKey(context(20L), "popularity"),
        RedisBatchLedger.indexKey(context(20L), "popularity"),
        RedisBatchLedger.ledgerKey(context(19L), "popularity"),
        RedisBatchLedger.ledgerKey(context(20L), "popularity"))
      jedis.zcard(RedisBatchLedger.indexKey(context(20L), "popularity")) shouldBe 2L
      jedis.hlen(RedisBatchLedger.ledgerKey(context(19L), "popularity")) shouldBe 3L
      jedis.hlen(RedisBatchLedger.ledgerKey(context(20L), "popularity")) shouldBe 3L
    } finally jedis.close()
  }

  it should "serialize concurrent out-of-order calls behind one monotonic committed watermark" in {
    val setup = new Jedis("127.0.0.1", redisPort)
    try setup.flushDB() finally setup.close()
    val ready = new CountDownLatch(2)
    val start = new CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val futures = Seq(13L, 14L).map { batchId =>
        executor.submit(new Callable[(Long, Long)] {
          override def call(): (Long, Long) = {
            val jedis = new Jedis("127.0.0.1", redisPort)
            try {
              ready.countDown()
              start.await(5L, TimeUnit.SECONDS) shouldBe true
              increment(jedis, batchId, s"batch-$batchId") -> complete(jedis, batchId)
            } finally jedis.close()
          }
        })
      }
      ready.await(5L, TimeUnit.SECONDS) shouldBe true
      start.countDown()
      futures.map(_.get(5L, TimeUnit.SECONDS)._1) should contain only (1L)

      val jedis = new Jedis("127.0.0.1", redisPort)
      try {
        jedis.hget(RedisBatchLedger.stateKey(context(14L), "popularity"), "committed_batch") shouldBe "14"
        increment(jedis, 13L, "batch-13") shouldBe 0L
        increment(jedis, 14L, "batch-14") shouldBe 0L
        jedis.zscore("global:item_popularity", "batch-13") shouldBe 1.0
        jedis.zscore("global:item_popularity", "batch-14") shouldBe 1.0
      } finally jedis.close()
    } finally {
      executor.shutdownNow()
    }
  }

  "RedisBatchLedger.hashOnce" should "outlive its target by one retry margin then expire its namespace" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val writeContext = SinkWriteContext(
        "checkpoint://sequence", "query-seq", "sequence:user-events", "sink-seq", 21L)
      val ledger = RedisBatchLedger.ledgerKey(writeContext, "sequence")
      val state = RedisBatchLedger.stateKey(writeContext, "sequence")
      val index = RedisBatchLedger.indexKey(writeContext, "sequence")
      val target = "sequence:u1:click:20260809"

      RedisBatchLedger.hashOnce(
        eval(jedis), ledger, state, index, target, target, 1,
        Map("item_id" -> "m1", "n" -> "1"), 21L) shouldBe 1L
      RedisBatchLedger.completeBatch(eval(jedis), state, index, 21L, 2, 1) shouldBe 21L
      jedis.exists(target) shouldBe true
      jedis.exists(ledger) shouldBe true
      jedis.exists(index) shouldBe true
      jedis.exists(state) shouldBe true

      Thread.sleep(1200L)

      jedis.exists(target) shouldBe false
      jedis.exists(ledger) shouldBe true
      jedis.exists(index) shouldBe true
      jedis.exists(state) shouldBe true

      Thread.sleep(1000L)

      jedis.exists(ledger) shouldBe false
      jedis.exists(index) shouldBe false
      jedis.exists(state) shouldBe false
    } finally jedis.close()
  }

  it should "renew the retained N-1 marker when N crosses its prior Redis expiry boundary" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val target = "sequence:u1:click:20260809"
      val batch30 = sequenceContext(30L)
      val batch31 = sequenceContext(31L)
      val state = RedisBatchLedger.stateKey(batch30, "sequence")
      val index = RedisBatchLedger.indexKey(batch30, "sequence")
      val retainedLedger = RedisBatchLedger.ledgerKey(batch30, "sequence")

      hash(jedis, 30L, target, target, 2, Map("item_id" -> "m1", "n" -> "1")) shouldBe 1L
      RedisBatchLedger.completeBatch(eval(jedis), state, index, 30L, 2, 2) shouldBe 30L
      val oldRemainingMillis = awaitPttlAtMost(jedis, retainedLedger, 900L)

      hash(jedis, 31L, target, target, 2, Map("item_id" -> "m2", "n" -> "2")) shouldBe 1L
      Thread.sleep(oldRemainingMillis + 150L)

      jedis.exists(state) shouldBe true
      jedis.exists(retainedLedger) shouldBe true
      hash(jedis, 30L, target, target, 2, Map("item_id" -> "replayed", "n" -> "999")) shouldBe 0L
      jedis.hget(target, "item_id") shouldBe "m2"
      jedis.hget(target, "n") shouldBe "2"
      jedis.zcard(index) shouldBe 2L
      jedis.exists(RedisBatchLedger.ledgerKey(batch31, "sequence")) shouldBe true
    } finally jedis.close()
  }

  it should "renew sequence state across an expiry boundary before completion" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val writeContext = sequenceContext(40L)
      val state = RedisBatchLedger.stateKey(writeContext, "sequence")
      val ledger = RedisBatchLedger.ledgerKey(writeContext, "sequence")
      val firstTarget = "sequence:u1:click:20260809"
      val secondTarget = "sequence:u2:click:20260809"

      hash(jedis, 40L, firstTarget, firstTarget, 2, Map("item_id" -> "m1")) shouldBe 1L
      jedis.exists(state) shouldBe true
      val oldRemainingMillis = awaitPttlAtMost(jedis, state, 900L)

      hash(jedis, 40L, secondTarget, secondTarget, 2, Map("item_id" -> "m2")) shouldBe 1L
      Thread.sleep(oldRemainingMillis + 150L)

      jedis.exists(state) shouldBe true
      jedis.exists(ledger) shouldBe true
      jedis.hlen(ledger) shouldBe 2L
      RedisBatchLedger.completeBatch(
        eval(jedis), state, RedisBatchLedger.indexKey(writeContext, "sequence"), 40L, 2, 2
      ) shouldBe 40L
    } finally jedis.close()
  }

  it should "fail closed when retained sequence state is missing" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val retained = sequenceContext(50L)
      val older = sequenceContext(49L)
      val target = "sequence:u1:click:20260809"
      val state = RedisBatchLedger.stateKey(retained, "sequence")
      val index = RedisBatchLedger.indexKey(retained, "sequence")

      hash(jedis, 50L, target, target, 30, Map("item_id" -> "m1", "n" -> "1")) shouldBe 1L
      RedisBatchLedger.completeBatch(eval(jedis), state, index, 50L, 2, 30) shouldBe 50L
      jedis.del(state) shouldBe 1L
      jedis.exists(index) shouldBe true
      jedis.exists(target) shouldBe true

      intercept[JedisDataException] {
        hash(jedis, 49L, target, "older-effect", 30, Map("item_id" -> "replayed", "n" -> "999"))
      }

      jedis.hget(target, "item_id") shouldBe "m1"
      jedis.hget(target, "n") shouldBe "1"
      jedis.exists(RedisBatchLedger.ledgerKey(older, "sequence")) shouldBe false
    } finally jedis.close()
  }

  it should "fail closed when the committed watermark field is missing" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val retained = sequenceContext(70L)
      val older = sequenceContext(69L)
      val target = "sequence:u1:click:20260809"
      val state = RedisBatchLedger.stateKey(retained, "sequence")
      val index = RedisBatchLedger.indexKey(retained, "sequence")

      hash(jedis, 70L, target, target, 30, Map("item_id" -> "m1", "n" -> "1")) shouldBe 1L
      RedisBatchLedger.completeBatch(eval(jedis), state, index, 70L, 2, 30) shouldBe 70L
      jedis.hdel(state, "committed_batch") shouldBe 1L

      intercept[JedisDataException] {
        hash(jedis, 69L, target, "older-effect", 30, Map("item_id" -> "replayed", "n" -> "999"))
      }

      jedis.hget(target, "item_id") shouldBe "m1"
      jedis.hget(target, "n") shouldBe "1"
      jedis.exists(RedisBatchLedger.ledgerKey(older, "sequence")) shouldBe false
    } finally jedis.close()
  }

  it should "fail closed before mutation when a retained sequence ledger has the wrong type" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val retained = sequenceContext(60L)
      val next = sequenceContext(61L)
      val retainedLedger = RedisBatchLedger.ledgerKey(retained, "sequence")
      val nextLedger = RedisBatchLedger.ledgerKey(next, "sequence")
      val target = "sequence:u1:click:20260809"
      val state = RedisBatchLedger.stateKey(retained, "sequence")
      val index = RedisBatchLedger.indexKey(retained, "sequence")

      hash(jedis, 60L, target, target, 30, Map("item_id" -> "m1", "n" -> "1")) shouldBe 1L
      RedisBatchLedger.completeBatch(eval(jedis), state, index, 60L, 2, 30) shouldBe 60L
      jedis.del(retainedLedger) shouldBe 1L
      jedis.set(retainedLedger, "not-a-hash") shouldBe "OK"

      intercept[JedisDataException] {
        hash(jedis, 61L, target, "next-effect", 30, Map("item_id" -> "mutated", "n" -> "2"))
      }

      jedis.hget(target, "item_id") shouldBe "m1"
      jedis.hget(target, "n") shouldBe "1"
      jedis.exists(nextLedger) shouldBe false
    } finally jedis.close()
  }
}
