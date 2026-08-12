package com.demo.process

import java.nio.file.Files

import com.demo.SparkTestSupport
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LateFeedbackJoinSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val Columns = Seq("session_id", "request_id", "user_id", "item_id", "event_type",
    "timestamp", "position", "user_features", "item_features", "context_features")

  private val Empty = Map.empty[String, String]

  /** One event row in the shape the joiner's staged events arrive in. */
  private def event(
      itemId: String,
      eventType: String,
      timestamp: Long,
      requestId: String = "req_1"
  ) = ("sess_1", requestId, "user_1", itemId, eventType, timestamp, 0, Empty, Empty, Empty)

  private def batch(rows: Seq[(String, String, String, String, String, Long, Int,
      Map[String, String], Map[String, String], Map[String, String])]): DataFrame = {
    val session = spark
    import session.implicits._
    // A typed empty Seq still carries the schema, so limit(0) is not needed for the empty case.
    rows.toDF(Columns: _*)
  }

  private def join(wait: String): LateFeedbackJoin =
    new LateFeedbackJoin(
      Files.createTempDirectory("late-feedback-join").toString, "test-namespace", wait)

  private def labels(samples: DataFrame): Seq[(String, Int, Int, Double)] =
    samples.select("item_id", "clicked", "ordered", "label").collect()
      .map(row => (row.getString(0), row.getInt(1), row.getInt(2), row.getDouble(3))).toSeq

  "LateFeedbackJoin" should "join a click to an impression from an earlier batch" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    val first = subject.process(batch(Seq(event("item_1", "impression", 100L))), 0L, startMs)
    val second = subject.process(batch(Seq(event("item_1", "click", 105L))), 1L, startMs + 1000L)
    val third = subject.process(batch(Seq.empty), 2L, startMs + 61000L)

    // Nothing publishes while the window is open.
    first.count() shouldBe 0L
    second.count() shouldBe 0L
    // The wall clock closes the window, and the click lands on its impression.
    labels(third) shouldBe Seq(("item_1", 1, 0, 1.0))
  }

  it should "close a window when event time advances past the deadline" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    subject.process(batch(Seq(event("item_1", "impression", 100L))), 0L, startMs).count() shouldBe 0L
    // req_2's impression at 1000 pushes observed event time past req_1's deadline of 160.
    val published = subject.process(
      batch(Seq(
        event("item_1", "click", 105L),
        event("item_9", "impression", 1000L, requestId = "req_2"))),
      1L, startMs + 1000L)

    // req_1 is due and correctly labelled; req_2's own window (deadline 1060) is still open.
    labels(published) shouldBe Seq(("item_1", 1, 0, 1.0))
  }

  it should "land a click and a later order on one published sample" in {
    val subject = join("120 seconds")
    val startMs = 1000000L

    subject.process(batch(Seq(event("item_1", "impression", 100L))), 0L, startMs).count() shouldBe 0L
    subject.process(batch(Seq(event("item_1", "click", 105L))), 1L, startMs + 5000L).count() shouldBe 0L
    subject.process(batch(Seq(event("item_1", "order", 190L))), 2L, startMs + 90000L).count() shouldBe 0L
    val published = subject.process(batch(Seq.empty), 3L, startMs + 121000L)

    labels(published) shouldBe Seq(("item_1", 1, 1, 2.0))
  }

  it should "publish nothing for feedback whose impression never arrives" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    subject.process(batch(Seq(event("item_1", "click", 105L))), 0L, startMs).count() shouldBe 0L
    subject.process(batch(Seq.empty), 1L, startMs + 61000L).count() shouldBe 0L
  }

  it should "reproduce per-batch behaviour when the wait is zero" in {
    val subject = join("0 seconds")

    val together = subject.process(
      batch(Seq(event("item_1", "impression", 100L), event("item_1", "click", 105L))), 0L, 1000000L)
    val feedbackOnly = subject.process(batch(Seq(event("item_2", "click", 205L))), 1L, 1000000L)

    labels(together) shouldBe Seq(("item_1", 1, 0, 1.0))
    feedbackOnly.count() shouldBe 0L
  }

  it should "publish the same rows when a batch is retried under a later clock" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    subject.process(batch(Seq(event("item_1", "impression", 100L))), 0L, startMs).count() shouldBe 0L
    val attempt = subject.process(batch(Seq(event("item_1", "click", 105L))), 1L, startMs + 1000L)
      .collect().toSeq
    // A retry of batch 1 runs with a clock past the deadline; the committed split still wins.
    val retry = subject.process(batch(Seq(event("item_1", "click", 105L))), 1L, startMs + 61000L)
      .collect().toSeq

    attempt shouldBe Seq.empty[Row]
    retry shouldBe attempt
  }

  "LateFeedbackJoin.waitSeconds" should "parse the watermark interval syntax" in {
    LateFeedbackJoin.waitSeconds("3 minutes") shouldBe 180L
    LateFeedbackJoin.waitSeconds("0 seconds") shouldBe 0L
    LateFeedbackJoin.waitSeconds("2 hours") shouldBe 7200L
  }

  it should "reject a negative or month-based wait" in {
    an[IllegalArgumentException] should be thrownBy LateFeedbackJoin.waitSeconds("-5 seconds")
    an[IllegalArgumentException] should be thrownBy LateFeedbackJoin.waitSeconds("1 month")
  }
}
