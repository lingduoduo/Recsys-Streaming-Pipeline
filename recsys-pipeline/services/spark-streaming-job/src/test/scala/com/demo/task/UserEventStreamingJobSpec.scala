package com.demo.task

import com.demo.SparkTestSupport
import com.demo.engine.DurableSink
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserEventStreamingJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "UserEventStreamingJob.businessSinks" should "configure only durable sinks for the Avro engine" in {
    val sinks = UserEventStreamingJob.businessSinks(
      redisHost = "localhost",
      redisPort = 6379,
      redisPoolMaxTotal = 1,
      sequenceConfig = com.demo.sequence.SequenceJobConfig(90, 500, None))

    sinks should have length 2
    all(sinks) shouldBe a[DurableSink]
    sinks.map(_.asInstanceOf[DurableSink].sinkIdentity).distinct should have length 2
  }

  "UserEventStreamingJob.parseEvents" should "normalize decoded canonical columns and ignore Kafka lineage" in {
    val s = spark; import s.implicits._
    val decoded = Seq(
      ("e1", "user_5", "movie_3", "click", 1718400000000L, "recsys_events", 10L),
      ("e2", "user_5", "movie_4", "impression", 1718400001000L, "recsys_events", 11L)
    ).toDF("event_id", "user_id", "item_id", "event_type", "timestamp_ms", "kafka_topic", "kafka_offset")

    val parsed = UserEventStreamingJob.parseEvents(decoded).kept
    val clicks = parsed.filter($"event_type" === "click").collect()
    clicks should have length 1
    clicks.head.getAs[String]("user_id") shouldBe "user_5"
    clicks.head.getAs[String]("item_id") shouldBe "movie_3"
    clicks.head.getAs[Long]("timestamp_ms") shouldBe 1718400000000L
    parsed.columns should not contain "kafka_topic"
    parsed.columns should not contain "kafka_offset"
  }

  it should "filter decoded rows missing required business identifiers" in {
    val s = spark; import s.implicits._
    val decoded = Seq(
      ("e1", Some("user_1"), Some("item_1"), "click", 1718400000000L),
      ("e2", None: Option[String], Some("item_2"), "click", 1718400001000L)
    ).toDF("event_id", "user_id", "item_id", "event_type", "timestamp_ms")

    UserEventStreamingJob.parseEvents(decoded).kept.select("event_id").as[String].collect() should contain only "e1"
  }

  it should "drop duplicate event_id within the watermark across micro-batches" in {
    val s = spark; import s.implicits._
    implicit val sqlCtx = s.sqlContext
    val input = MemoryStream[(String, String, String, String, Long)]
    val decoded = input.toDS().toDF("event_id", "user_id", "item_id", "event_type", "timestamp_ms")
    val deduped = UserEventStreamingJob.dedupedClicks(decoded, "10 minutes")
    val q = deduped.writeStream.format("memory").queryName("ue_out").outputMode("append").start()
    try {
      val e = ("e1", "u1", "i1", "click", 1718400000000L)
      input.addData(e); q.processAllAvailable()
      input.addData(e); q.processAllAvailable()   // identical event_id again
      s.table("ue_out").count() shouldBe 1
    } finally q.stop()
  }

  it should "deduplicate decoded clicks when the engine supplies a static micro-batch" in {
    val s = spark
    import s.implicits._
    val decoded = Seq(
      ("e1", "u1", "i1", "click", 1718400000000L),
      ("e1", "u1", "i1", "click", 1718400000000L)
    ).toDF("event_id", "user_id", "item_id", "event_type", "timestamp_ms")

    UserEventStreamingJob.dedupedClicks(decoded, "10 minutes").count() shouldBe 1L
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
      .toColumnChunks(UserEventStreamingJob.buildSequenceEvents(batch))
      .orderBy("bucket")
      .collect()

    chunks.length shouldBe 2
    chunks(0).getAs[String]("bucket") shouldBe "20260723"
    chunks(0).getAs[String]("item_id") shouldBe "m1,m2"
    chunks(1).getAs[String]("bucket") shouldBe "20260724"
    chunks(1).getAs[String]("item_id") shouldBe "m3"
  }
}
