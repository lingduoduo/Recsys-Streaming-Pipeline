package com.demo.engine

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Popularity counting only. The Redis effects live in `RedisSinkSpec`, which owns a
  * redis-server process fixture this suite has no use for. */
class RedisPopularitySinkSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val sink = new RedisPopularitySink("localhost", 6379, 1)

  "RedisPopularitySink.counts" should "count clicks and ignore the rest of the behavior stream" in {
    val s = spark; import s.implicits._
    val batch = Seq(
      ("u1", None: Option[String], "search"),
      ("u1", Some("m1"), "result_view"),
      ("u1", Some("m1"), "detail_view"),
      ("u1", Some("m1"), "click"),
      ("u1", Some("m1"), "click"),
      ("u1", Some("m2"), "click")
    ).toDF("user_id", "item_id", "event_type")

    val counts = sink.counts(batch).as[(String, Long)].collect().toMap

    counts shouldBe Map("m1" -> 2L, "m2" -> 1L)
  }

  it should "not count a click whose item went missing" in {
    val s = spark; import s.implicits._
    val batch = Seq(("u1", None: Option[String], "click")).toDF("user_id", "item_id", "event_type")

    sink.counts(batch).count() shouldBe 0L
  }
}
