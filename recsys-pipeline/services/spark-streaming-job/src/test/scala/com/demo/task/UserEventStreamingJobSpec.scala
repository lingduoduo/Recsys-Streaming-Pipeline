package com.demo.task

import com.demo.SparkTestSupport
import org.apache.spark.sql.execution.streaming.MemoryStream
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

  it should "drop duplicate event_id within the watermark across micro-batches" in {
    val s = spark; import s.implicits._
    implicit val sqlCtx = s.sqlContext
    val input = MemoryStream[String]
    val deduped = UserEventStreamingJob.dedupedClicks(input.toDF(), "10 minutes")
    val q = deduped.writeStream.format("memory").queryName("ue_out").outputMode("append").start()
    try {
      val e = """{"event_id":"e1","user_id":"u1","item_id":"i1","event_type":"click","timestamp_ms":1718400000000}"""
      input.addData(e); q.processAllAvailable()
      input.addData(e); q.processAllAvailable()   // identical event_id again
      s.table("ue_out").count() shouldBe 1
    } finally q.stop()
  }

  "UserEventStreamingJob.itemClickCounts" should "count clicks per item" in {
    val s = spark; import s.implicits._
    val batch = Seq("i1", "i1", "i2").toDF("item_id")
    val counts = UserEventStreamingJob.itemClickCounts(batch)
      .collect().map(r => r.getString(0) -> r.getAs[Long]("count")).toMap
    counts shouldBe Map("i1" -> 2L, "i2" -> 1L)
  }

  "buildSequenceEvents" should "project click events into sequence-store shape" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val batch = Seq(
      ("u1", "m1", "click", 1784764801000L),
      ("u1", "m2", "click", 1784764802000L)
    ).toDF("user_id", "item_id", "event_type", "timestamp_ms")

    val rows = UserEventStreamingJob.buildSequenceEvents(batch).orderBy("ts").collect()

    rows.length shouldBe 2
    rows.head.getAs[String]("user_id") shouldBe "u1"
    rows.head.getAs[String]("kind") shouldBe "click"
    rows.head.getAs[String]("item_id") shouldBe "m1"
    rows.head.getAs[Long]("ts") shouldBe 1784764801000L
    rows.head.getAs[String]("action") shouldBe "click"
    rows.head.isNullAt(rows.head.fieldIndex("rating")) shouldBe true
  }

  it should "produce one chunk per user and day through SequenceEncoder" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val batch = Seq(
      ("u1", "m1", "click", 1784764801000L),
      ("u1", "m2", "click", 1784764802000L),
      ("u1", "m3", "click", 1784851201000L)
    ).toDF("user_id", "item_id", "event_type", "timestamp_ms")

    val chunks = com.demo.sequence.SequenceEncoder
      .toColumnChunks(UserEventStreamingJob.buildSequenceEvents(batch), "day")
      .orderBy("bucket")
      .collect()

    chunks.length shouldBe 2
    chunks(0).getAs[String]("bucket") shouldBe "20260723"
    chunks(0).getAs[String]("item_id") shouldBe "m1,m2"
    chunks(1).getAs[String]("bucket") shouldBe "20260724"
    chunks(1).getAs[String]("item_id") shouldBe "m3"
  }
}
