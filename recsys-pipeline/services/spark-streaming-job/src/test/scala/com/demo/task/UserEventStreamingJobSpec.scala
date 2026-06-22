package com.demo.task

import com.demo.SparkTestSupport
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserEventStreamingJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "UserEventStreamingJob.parseEvents" should "parse unified schema with string IDs and timestamp_ms" in {
    val s = spark; import s.implicits._
    val json = Seq(
      """{"event_id":"e1","user_id":"user_5","item_id":"movie_3","event_type":"click","timestamp_ms":1718400000000}""",
      """{"event_id":"e2","user_id":"user_5","item_id":"movie_4","event_type":"impression","timestamp_ms":1718400001000}"""
    ).toDF("value")

    val parsed = UserEventStreamingJob.parseEvents(json)
    val clicks = parsed.filter($"event_type" === "click").collect()
    clicks should have length 1
    clicks.head.getAs[String]("user_id") shouldBe "user_5"
    clicks.head.getAs[String]("item_id") shouldBe "movie_3"
    clicks.head.getAs[Long]("timestamp_ms") shouldBe 1718400000000L
  }

  it should "parse legacy schema with integer timestamp field" in {
    val s = spark; import s.implicits._
    val json = Seq(
      """{"user_id":"user_1","item_id":"item_1","event_type":"click","timestamp":1718400000}"""
    ).toDF("value")

    val parsed = UserEventStreamingJob.parseEvents(json)
    parsed.filter($"event_type" === "click").count() shouldBe 1
  }
}
