package com.demo.event

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventParsingSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "EventParsing.fromJson" should "flatten a userEvent JSON value into columns" in {
    val s = spark; import s.implicits._
    val raw = Seq(
      """{"event_id":"e1","user_id":"user_5","item_id":"movie_3","event_type":"click","timestamp_ms":1718400000000}"""
    ).toDF("value")

    val out = EventParsing.fromJson(raw, EventSchemas.userEvent)

    out.columns should contain allOf ("event_id", "user_id", "item_id", "event_type", "timestamp_ms", "timestamp")
    val row = out.collect().head
    row.getAs[String]("user_id") shouldBe "user_5"
    row.getAs[String]("item_id") shouldBe "movie_3"
    row.getAs[Long]("timestamp_ms") shouldBe 1718400000000L
  }

  it should "expose joiner-specific columns (request_id, position, feature maps)" in {
    val s = spark; import s.implicits._
    val raw = Seq(
      """{"request_id":"req_1","user_id":"u1","item_id":"i1","event_type":"impression","timestamp":100,"position":2,"user_features":{"tier":"gold"}}"""
    ).toDF("value")

    val out = EventParsing.fromJson(raw, EventSchemas.joiner)

    out.columns should contain allOf ("request_id", "position", "user_features", "item_features", "context_features")
    out.collect().head.getAs[Int]("position") shouldBe 2
  }
}
