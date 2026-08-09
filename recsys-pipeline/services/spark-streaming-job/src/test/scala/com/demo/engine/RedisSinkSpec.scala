package com.demo.engine

import java.util

import java.net.ServerSocket

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException

import scala.collection.JavaConverters._
import scala.collection.mutable

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
    val context = SinkWriteContext("checkpoint://redis", "query-ns", "redis:popularity", "sink-ns", 8L)
    val applied = mutable.Set.empty[(String, String)]
    val scores = mutable.Map.empty[String, Double].withDefaultValue(0.0)
    var failAfterFirstApply = true
    val eval = (_: String, keys: util.List[String], args: util.List[String]) => {
      val key = keys.asScala.head -> args.asScala.head
      if (applied.add(key)) scores(args.get(3)) += args.get(2).toDouble
      if (failAfterFirstApply) {
        failAfterFirstApply = false
        throw new RuntimeException("connection lost after Redis applied the script")
      }
      java.lang.Long.valueOf(if (applied.contains(key)) 1L else 0L)
    }
    val ledger = RedisBatchLedger.ledgerKey(context, "popularity", "item-1")

    intercept[RuntimeException] {
      RedisBatchLedger.incrementOnce(eval, ledger, "global:item_popularity", "item-1", 3.0, "item-1", 8L)
    }
    RedisBatchLedger.incrementOnce(eval, ledger, "global:item_popularity", "item-1", 3.0, "item-1", 8L)

    scores("item-1") shouldBe 3.0
    applied should contain only (ledger -> "8")
  }

  it should "leave no ledger marker when a wrong-type popularity target rejects the effect" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val context = SinkWriteContext("checkpoint://redis", "query-ns", "redis:popularity", "sink-ns", 8L)
      val ledger = RedisBatchLedger.ledgerKey(context, "popularity", "item-1")
      jedis.set("wrong-target", "not-a-zset")

      intercept[JedisDataException] {
        RedisBatchLedger.incrementOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger, "wrong-target", "item-1", 3.0, "item-1", 8L)
      }

      jedis.exists(ledger) shouldBe false
      jedis.get("wrong-target") shouldBe "not-a-zset"
    } finally jedis.close()
  }

  it should "leave the popularity target unchanged when the ledger has the wrong type" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val context = SinkWriteContext("checkpoint://redis", "query-ns", "redis:popularity", "sink-ns", 8L)
      val ledger = RedisBatchLedger.ledgerKey(context, "popularity", "item-1")
      jedis.set(ledger, "not-a-hash")

      intercept[JedisDataException] {
        RedisBatchLedger.incrementOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger, "global:item_popularity", "item-1", 3.0, "item-1", 8L)
      }

      jedis.zscore("global:item_popularity", "item-1") shouldBe null
      jedis.get(ledger) shouldBe "not-a-hash"
    } finally jedis.close()
  }

  "RedisBatchLedger.hashOnce" should "leave no ledger marker when a wrong-type sequence target rejects the effect" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      val context = SinkWriteContext("checkpoint://sequence", "query-ns", "sequence:user-events", "sink-ns", 12L)
      val sequenceKey = "seq:u1:click:20260809"
      val ledger = RedisBatchLedger.ledgerKey(context, "sequence", sequenceKey)
      jedis.set(sequenceKey, "not-a-hash")

      intercept[JedisDataException] {
        RedisBatchLedger.hashOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger, sequenceKey, sequenceKey, 3600, Map("item_id" -> "m1", "n" -> "1"), 12L)
      }

      jedis.exists(ledger) shouldBe false
      jedis.get(sequenceKey) shouldBe "not-a-hash"
    } finally jedis.close()
  }

  it should "bound a per-item ledger while retaining the current and previous batch" in {
    val jedis = new Jedis("127.0.0.1", redisPort)
    try {
      jedis.flushDB()
      (7L to 10L).foreach { batchId =>
        val context = SinkWriteContext("checkpoint://redis", "query-ns", "redis:popularity", "sink-ns", batchId)
        val ledger = RedisBatchLedger.ledgerKey(context, "popularity", "item-1")
        RedisBatchLedger.incrementOnce(
          (script, keys, arguments) => jedis.eval(script, keys, arguments),
          ledger, "global:item_popularity", "item-1", 1.0, "item-1", batchId,
          retainedBatchWindow = 2)
      }
      val current = SinkWriteContext("checkpoint://redis", "query-ns", "redis:popularity", "sink-ns", 10L)
      val ledger = RedisBatchLedger.ledgerKey(current, "popularity", "item-1")
      jedis.hkeys(ledger).asScala should contain only ("9", "10")
      jedis.zscore("global:item_popularity", "item-1") shouldBe 4.0
    } finally jedis.close()
  }
}
