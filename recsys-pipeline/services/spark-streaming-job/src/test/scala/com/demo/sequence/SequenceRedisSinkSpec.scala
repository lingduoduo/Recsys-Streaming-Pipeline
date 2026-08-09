package com.demo.sequence

import com.demo.SparkTestSupport
import com.demo.engine.{RedisBatchLedger, SinkWriteContext}
import java.util
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisException}

import scala.collection.JavaConverters._
import scala.collection.mutable

class SequenceRedisSinkSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val existing = Map(
    SequenceSchema.ColItemId -> "a,b",
    SequenceSchema.ColTs     -> "1,2",
    SequenceSchema.ColCount  -> "2"
  )
  private val fresh = Map(
    SequenceSchema.ColItemId -> "c",
    SequenceSchema.ColTs     -> "3",
    SequenceSchema.ColCount  -> "1"
  )

  // NOTE: all arguments positional — in Scala 2.12 a positional argument may not follow
  // a named one, so `resolve(existing, fresh, maxRows = 10, Append)` would not compile.
  "resolve" should "merge existing and fresh rows in Append mode" in {
    val resolved = SequenceRedisSink.resolve(existing, fresh, 10, SequenceWriteMode.Append)
    resolved(SequenceSchema.ColItemId) shouldBe "a,b,c"
    resolved(SequenceSchema.ColCount) shouldBe "3"
  }

  it should "ignore existing rows entirely in Overwrite mode" in {
    val resolved = SequenceRedisSink.resolve(existing, fresh, 10, SequenceWriteMode.Overwrite)
    resolved(SequenceSchema.ColItemId) shouldBe "c"
    resolved(SequenceSchema.ColCount) shouldBe "1"
  }

  it should "cap a bucket at maxRows in Append mode" in {
    val resolved = SequenceRedisSink.resolve(existing, fresh, 2, SequenceWriteMode.Append)
    resolved(SequenceSchema.ColItemId) shouldBe "b,c"
    resolved(SequenceSchema.ColCount) shouldBe "2"
  }

  "chunkFields" should "extract every schema column plus n from a chunk Row" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val row = Seq(("u1", "rating", "20260723", "m1,m2", "1,2", "rate,rate", ",4.0", "Drama,", "1995,", 2L))
      .toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")
      .collect()
      .head

    val fields = SequenceRedisSink.chunkFields(row)

    fields(SequenceSchema.ColItemId) shouldBe "m1,m2"
    fields(SequenceSchema.ColRating) shouldBe ",4.0"
    fields(SequenceSchema.ColCount) shouldBe "2"
    fields.keySet shouldBe (SequenceSchema.Columns.toSet + SequenceSchema.ColCount)
  }

  "isFatal" should "be true for a JedisConnectionException" in {
    SequenceRedisSink.isFatal(new JedisConnectionException("boom")) shouldBe true
  }

  it should "be true for a plain JedisException" in {
    SequenceRedisSink.isFatal(new JedisException("boom")) shouldBe true
  }

  it should "be false for a NullPointerException" in {
    SequenceRedisSink.isFatal(new NullPointerException("boom")) shouldBe false
  }

  it should "be false for an IllegalArgumentException" in {
    SequenceRedisSink.isFatal(new IllegalArgumentException("boom")) shouldBe false
  }

  "RedisBatchLedger.hashOnce" should "not append a sequence effect twice after interruption" in {
    val context = SinkWriteContext("checkpoint://sequence", "query-ns", "sequence:user-events", "sink-ns", 12L)
    val applied = mutable.Set.empty[(String, String)]
    val hashes = mutable.Map.empty[String, Map[String, String]]
    var failAfterFirstApply = true
    val eval = (_: String, keys: util.List[String], args: util.List[String]) => {
      val values = args.asScala.toSeq
      val ledgerEffect = keys.get(0) -> values.head
      if (applied.add(ledgerEffect)) hashes(keys.get(1)) = values.drop(2).grouped(2).map {
        case Seq(field, value) => field -> value
      }.toMap
      if (failAfterFirstApply) {
        failAfterFirstApply = false
        throw new RuntimeException("connection lost after Redis applied the script")
      }
      java.lang.Long.valueOf(1L)
    }
    val ledger = RedisBatchLedger.ledgerKey(context, "sequence")
    val fields = Map("item_id" -> "m1", "n" -> "1")

    intercept[RuntimeException] {
      RedisBatchLedger.hashOnce(eval, ledger, "sequence:u1:click:20260723", "u1:click:20260723", 3600, fields)
    }
    RedisBatchLedger.hashOnce(eval, ledger, "sequence:u1:click:20260723", "u1:click:20260723", 3600, fields)

    hashes("sequence:u1:click:20260723") shouldBe fields
    applied should contain only (ledger -> "u1:click:20260723")
  }
}
