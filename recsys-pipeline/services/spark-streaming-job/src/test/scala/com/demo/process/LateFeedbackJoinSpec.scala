package com.demo.process

import java.nio.file.Files

import com.demo.SparkTestSupport
import com.demo.engine.CommitProtocol
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
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

  it should "not flush a pending slate when a later event carries a far-future timestamp" in {
    val subject = join("60 seconds")
    // The impression's timestamp sits in the same range as nowMs (both ~1,000,000) so that an
    // unclamped observed time would clearly clear the deadline, and a clamped one clearly would not.
    val startMs = 1000000L

    subject.process(batch(Seq(event("item_1", "impression", 1000L))), 0L, startMs).count() shouldBe 0L
    // req_2's timestamp is a year ahead, but nowMs has barely moved; observed event time must
    // clamp to nowMs rather than let this skewed row make req_1 due.
    val published = subject.process(
      batch(Seq(event("item_9", "impression", 1000L + 365L * 86400L, requestId = "req_2"))),
      1L, startMs + 1000L)

    published.count() shouldBe 0L
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

  "LateFeedbackJoin.formatOrphans" should "name the batch and both counts" in {
    LateFeedbackJoin.formatOrphans(7L, 2L, 3L) shouldBe
      "[late-feedback] batch=7 orphan_slates=2 orphan_events=3"
  }

  "LateFeedbackJoin" should "count a due slate that never received an impression" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    subject.process(
      batch(Seq(event("item_1", "click", 105L), event("item_1", "order", 110L))), 0L, startMs)
      .count() shouldBe 0L
    subject.orphanCounts shouldBe (0L, 0L)

    subject.process(batch(Seq.empty), 1L, startMs + 61000L).count() shouldBe 0L
    subject.orphanCounts shouldBe (1L, 2L)
  }

  it should "count no orphans when every due slate has its impression" in {
    val subject = join("0 seconds")

    subject.process(
      batch(Seq(event("item_1", "impression", 100L), event("item_1", "click", 105L))), 0L, 1000000L)
      .count() shouldBe 1L
    subject.orphanCounts shouldBe (0L, 0L)
  }

  "readSnapshot" should "fill columns added since a snapshot was written with typed nulls" in {
    val session = spark
    import session.implicits._

    val root = Files.createTempDirectory("late-feedback-schema").toString
    val namespace = "schema-evolution-query"
    val subject = new LateFeedbackJoin(root, namespace, "1 hour")

    // Hand-write batch 0's snapshot in the PRE-UPGRADE shape: every SnapshotColumn except
    // timestamp_ms, which did not exist when that release wrote it. Going through the real commit
    // protocol so the manifest validates exactly as a genuine prior snapshot would.
    val preUpgradeColumns = LateFeedbackJoin.SnapshotColumns.filterNot(_ == "timestamp_ms")
    val pendingRoot = new Path(new Path(new Path(root), s"_queries/$namespace"), "_pending")
    val staged = OnlineJoinerStreamingJob.MeasurementFields
      .foldLeft(batch(Seq(event("item_1", "impression", 100L)))) {
        case (df, (name, dataType)) => df.withColumn(name, lit(null).cast(dataType))
      }
      .withColumn("event_id", lit(null).cast(StringType))
      .select(preUpgradeColumns.map(col): _*)
      .withColumn("first_seen_ms", lit(100L * 1000L))

    CommitProtocol.writeDirectory(
      staged,
      new Path(pendingRoot, "_attempts/0"),
      new Path(pendingRoot, "0"),
      partitionByDate = false,
      "pending slate snapshot 0",
      s"version=2\nquery=$namespace\nkind=pending\nbatch_id=0\n")

    // Batch 1 runs the current code, whose template carries timestamp_ms. Reading batch 0's
    // snapshot must fill the absent column rather than failing the batch.
    val batchOne = batch(Seq(event("item_2", "impression", 200L, requestId = "req_2")))
    noException should be thrownBy subject.process(batchOne, 1L, 200L * 1000L).count()
  }
}
