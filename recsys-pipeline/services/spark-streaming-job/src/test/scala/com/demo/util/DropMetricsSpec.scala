package com.demo.util

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DropMetricsSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "DropMetrics.format" should "sum dropped from the reasons and keep declared order" in {
    val line = DropMetrics.format("OnlineJoinerStreamingJob", 42L, 4931L,
      Seq("null_request_id" -> 3L, "null_user_id" -> 7L, "null_item_id" -> 48L,
        "null_event_type" -> 0L, "null_timestamp" -> 4L))

    line should include("job=OnlineJoinerStreamingJob")
    line should include("batch=42")
    line should include("kept=4931")
    line should include("dropped=62")
    line should include("null_request_id=3 null_user_id=7 null_item_id=48 " +
      "null_event_type=0 null_timestamp=4")
  }

  it should "still emit every reason on a clean batch" in {
    val line = DropMetrics.format("UserEventStreamingJob", 7L, 100L,
      Seq("null_user_id" -> 0L, "null_item_id" -> 0L))

    // A silent counter is indistinguishable from a broken one, so zeros are emitted too.
    line should include("dropped=0")
    line should include("null_user_id=0 null_item_id=0")
  }

  "DropMetrics.DecodeReasons" should "cover every code EventAvroCodec can reject with" in {
    DropMetrics.DecodeReasons shouldBe
      Seq("invalid_marker", "unknown_fingerprint", "corrupt_payload", "required_field")
  }

  "decodeCounts" should "tally dead letters by code, keeping every declared code" in {
    val s = spark; import s.implicits._
    val deadLetters = Seq("corrupt_payload", "corrupt_payload", "required_field")
      .toDF("error_code")

    DropMetrics.decodeCounts(deadLetters) shouldBe Seq(
      "invalid_marker" -> 0L,
      "unknown_fingerprint" -> 0L,
      "corrupt_payload" -> 2L,
      "required_field" -> 1L)
  }

  it should "report all zeros for a batch with no dead letters" in {
    val s = spark; import s.implicits._
    val empty = Seq.empty[String].toDF("error_code")

    DropMetrics.decodeCounts(empty).map(_._2).sum shouldBe 0L
    DropMetrics.decodeCounts(empty).map(_._1) shouldBe DropMetrics.DecodeReasons
  }
}
