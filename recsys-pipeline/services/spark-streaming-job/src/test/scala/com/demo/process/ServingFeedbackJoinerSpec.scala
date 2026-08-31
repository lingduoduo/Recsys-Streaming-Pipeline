package com.demo.process

import com.demo.SparkTestSupport
import com.demo.event.{EventParsing, EventSchemas}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Closes the gap GrpoEventPublisher was built to close: before it existed, serving never
// published feedback, so every serving-sourced slate landed with clicked=0/label=0.0 no matter
// what the user did. This feeds one impression and one click — shaped exactly like
// GrpoImpressionEvents.build(...) and GrpoFeedbackEvents.build(...) emit them (see
// java-retrieval-service/.../grpo/{GrpoImpressionEvents,GrpoFeedbackEvents}.java) — through the
// real joiner (parseEvents, then buildTrainingSamples) and proves the resulting sample now carries
// clicked=1 and label=1.0, not the 0.0 every serving slate got before.
class ServingFeedbackJoinerSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "the joiner" should "label a serving impression + serving feedback click as clicked with label 1.0" in {
    val s = spark; import s.implicits._

    val impression =
      """{"event_id":"e-imp-1","request_id":"req-1","session_id":"sess_abcd1234","user_id":"u1",
        |"item_id":"m1","event_type":"impression","timestamp_ms":1000,"position":0,
        |"user_features":{"algorithm":"hybrid"},
        |"item_features":{"prediction_score":"0.73","grpo_x":"v1:1.0,0.7,0.4,0.3,0.05,0.0,2.4,1.1,0.0,0.18"},
        |"context_features":{}}""".stripMargin.replaceAll("\n", "")

    // Verbatim shape of GrpoFeedbackEvents.build(...): one event, event_type "click", position 0
    // (not meaningful for feedback — the joiner never reads position off anything but an
    // impression), user_features/item_features/context_features empty.
    val feedback =
      """{"event_id":"e-fb-1","request_id":"req-1","session_id":"sess_efgh5678","user_id":"u1",
        |"item_id":"m1","event_type":"click","timestamp_ms":5000,"position":0,
        |"user_features":{},"item_features":{},"context_features":{}}""".stripMargin.replaceAll("\n", "")

    val kafkaShaped = Seq(impression, feedback).toDF("value")
    val decoded = EventParsing.fromJson(kafkaShaped, EventSchemas.joiner)
    val gated = OnlineJoinerStreamingJob.parseEvents(decoded)
    val samples = OnlineJoinerStreamingJob.buildTrainingSamples(gated.kept)
    val row = samples.select("request_id", "user_id", "item_id", "clicked", "ordered", "label").collect().head

    row.getAs[String]("request_id") shouldBe "req-1"
    row.getAs[String]("user_id") shouldBe "u1"
    row.getAs[String]("item_id") shouldBe "m1"
    row.getAs[Int]("clicked") shouldBe 1
    row.getAs[Int]("ordered") shouldBe 0
    row.getAs[Double]("label") shouldBe 1.0
  }
}
