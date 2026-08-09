package com.demo.engine

import java.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.collection.mutable

class RedisSinkSpec extends AnyFlatSpec with Matchers {

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
      if (applied.add(key)) scores(args.get(2)) += args.get(1).toDouble
      if (failAfterFirstApply) {
        failAfterFirstApply = false
        throw new RuntimeException("connection lost after Redis applied the script")
      }
      java.lang.Long.valueOf(if (applied.contains(key)) 1L else 0L)
    }
    val ledger = RedisBatchLedger.ledgerKey(context, "popularity")

    intercept[RuntimeException] {
      RedisBatchLedger.incrementOnce(eval, ledger, "global:item_popularity", "item-1", 3.0, "item-1")
    }
    RedisBatchLedger.incrementOnce(eval, ledger, "global:item_popularity", "item-1", 3.0, "item-1")

    scores("item-1") shouldBe 3.0
    applied should contain only (ledger -> "item-1")
  }
}
