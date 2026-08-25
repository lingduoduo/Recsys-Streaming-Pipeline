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

    sinks should have length 3
    all(sinks) shouldBe a[DurableSink]
    sinks.map(_.asInstanceOf[DurableSink].sinkIdentity).distinct should have length 3
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
    val deduped = UserEventStreamingJob.behavioralEvents(decoded, "10 minutes")
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

    UserEventStreamingJob.behavioralEvents(decoded, "10 minutes").count() shouldBe 1L
  }


  /** The behavioral subset plus one non-behavioral feedback action, as decoded canonical rows. */
  private def behaviorFixture = {
    val s = spark; import s.implicits._
    Seq(
      ("search-ok",  Some("u1"), None: Option[String], "search",      1000L, Some("q1"), Some("space opera"), None: Option[String]),
      ("search-bad", Some("u1"), None: Option[String], "search",      1001L, Some("q1"), Some("   "),         None: Option[String]),
      ("result-bad", Some("u1"), Some("m1"),           "result_view", 1002L, Some("q1"), None: Option[String], None: Option[String]),
      ("detail-ok",  Some("u1"), Some("m1"),           "detail_view", 1003L, None: Option[String], None: Option[String], None: Option[String]),
      ("click-ok",   Some("u1"), Some("m1"),           "click",       1004L, None: Option[String], None: Option[String], None: Option[String]),
      ("click-bad",  Some("u1"), None: Option[String], "click",       1005L, None: Option[String], None: Option[String], None: Option[String]),
      ("thumb-up",   Some("u1"), Some("m1"),           "thumb_up",    1006L, None: Option[String], None: Option[String], None: Option[String])
    ).toDF("event_id", "user_id", "item_id", "event_type", "timestamp_ms",
           "query_id", "query_text", "result_set_id")
  }

  "UserEventStreamingJob.behavioralEvents" should "keep only structurally usable behavioral actions" in {
    val s = spark; import s.implicits._

    val kept = UserEventStreamingJob.behavioralEvents(behaviorFixture, "10 minutes")
      .select("event_id").as[String].collect()

    kept should contain theSameElementsAs Seq("search-ok", "detail-ok", "click-ok")
  }

  "UserEventStreamingJob.normalize" should "name why each behavioral event was rejected" in {
    val s = spark; import s.implicits._

    val reasons = UserEventStreamingJob.normalize(behaviorFixture).tagged
      .select("event_id", "rejection_reason").as[(String, Option[String])].collect().toMap

    reasons shouldBe Map(
      "search-ok"  -> None,
      "search-bad" -> Some("missing_search_query"),
      "result-bad" -> Some("missing_result_identity"),
      "detail-ok"  -> None,
      "click-ok"   -> None,
      "click-bad"  -> Some("missing_behavior_item")
    )
  }

  it should "ignore a non-behavioral feedback action rather than reject it" in {
    val s = spark; import s.implicits._

    UserEventStreamingJob.normalize(behaviorFixture).tagged
      .select("event_id").as[String].collect() should not contain "thumb-up"
  }

  it should "still reject a behavioral event with no user, event id, or timestamp" in {
    val s = spark; import s.implicits._
    val decoded = Seq(
      (Some("e1"), None: Option[String], Some("m1"), "click", Some(1000L)),
      (None: Option[String], Some("u1"), Some("m1"), "click", Some(1001L)),
      (Some("e3"), Some("u1"), Some("m1"), "click", None: Option[Long])
    ).toDF("event_id", "user_id", "item_id", "event_type", "timestamp_ms")

    UserEventStreamingJob.normalize(decoded).kept.count() shouldBe 0L
  }

  "buildBehaviorSequenceEvents" should "project every behavioral action into one sequence kind" in {
    val s = spark; import s.implicits._
    val batch = Seq(
      ("u1", None: Option[String], "search",      1784764801000L),
      ("u1", Some("m1"),           "result_view", 1784764802000L),
      ("u1", Some("m1"),           "detail_view", 1784764803000L),
      ("u1", Some("m1"),           "click",       1784764804000L)
    ).toDF("user_id", "item_id", "event_type", "timestamp_ms")

    val rows = UserEventStreamingJob.buildBehaviorSequenceEvents(batch).orderBy("ts")
      .select("kind", "item_id", "action").as[(String, String, String)].collect()

    rows shouldBe Array(
      ("behavior", "", "search"),
      ("behavior", "m1", "result_view"),
      ("behavior", "m1", "detail_view"),
      ("behavior", "m1", "click")
    )
  }

  it should "encode a behavior chunk in timestamp order with the search sentinel in place" in {
    val s = spark; import s.implicits._
    val batch = Seq(
      ("u1", Some("m1"),           "click",  1784764804000L),
      ("u1", None: Option[String], "search", 1784764801000L)
    ).toDF("user_id", "item_id", "event_type", "timestamp_ms")

    val chunk = com.demo.sequence.SequenceEncoder
      .toColumnChunks(UserEventStreamingJob.buildBehaviorSequenceEvents(batch)).collect()

    chunk.length shouldBe 1
    chunk.head.getAs[String]("kind") shouldBe "behavior"
    chunk.head.getAs[String]("action") shouldBe "search,click"
    chunk.head.getAs[String]("item_id") shouldBe ",m1"
  }

  it should "write a click into both the behavior and the legacy click projection" in {
    val s = spark; import s.implicits._
    val batch = Seq(
      ("u1", Some("m1"),           "click",  1784764804000L),
      ("u1", None: Option[String], "search", 1784764801000L)
    ).toDF("user_id", "item_id", "event_type", "timestamp_ms")

    UserEventStreamingJob.buildBehaviorSequenceEvents(batch).count() shouldBe 2L
    UserEventStreamingJob.buildClickSequenceEvents(batch)
      .select("kind", "item_id").as[(String, String)].collect() shouldBe Array(("click", "m1"))
  }

  "UserEventStreamingJob" should "keep an Avro-encoded search all the way through the gate" in {
    val s = spark; import s.implicits._
    val record = new org.apache.avro.generic.GenericData.Record(com.demo.event.EventAvroCodec.schema)
    record.put("event_id", "search-wire")
    record.put("user_id", "u1")
    record.put("event_type", "search")
    record.put("timestamp_ms", 1784764801000L)
    record.put("query_id", "q1")
    record.put("query_text", "space opera")
    val rawKafka = Seq(
      (com.demo.event.EventAvroCodec.encode(record), "recsys_events", 1, 10L,
        new java.sql.Timestamp(1784764801000L))
    ).toDF("value", "topic", "partition", "offset", "timestamp")

    val decoded = com.demo.event.DecodedEventBatch.decode(rawKafka).valid

    UserEventStreamingJob.behavioralEvents(decoded, "10 minutes")
      .select("event_id").as[String].collect() should contain only "search-wire"
  }

  "UserEventStreamingJob.itemClickCounts" should "count clicks per item" in {
    val s = spark; import s.implicits._
    val batch = Seq("i1", "i1", "i2").toDF("item_id")
    val counts = UserEventStreamingJob.itemClickCounts(batch)
      .collect().map(r => r.getString(0) -> r.getAs[Long]("count")).toMap
    counts shouldBe Map("i1" -> 2L, "i2" -> 1L)
  }

  "buildClickSequenceEvents" should "project click events into sequence-store shape" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val batch = Seq(
      ("u1", "m1", "click", 1784764801000L),
      ("u1", "m2", "click", 1784764802000L)
    ).toDF("user_id", "item_id", "event_type", "timestamp_ms")

    val rows = UserEventStreamingJob.buildClickSequenceEvents(batch).orderBy("ts").collect()

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
      .toColumnChunks(UserEventStreamingJob.buildClickSequenceEvents(batch))
      .orderBy("bucket")
      .collect()

    chunks.length shouldBe 2
    chunks(0).getAs[String]("bucket") shouldBe "20260723"
    chunks(0).getAs[String]("item_id") shouldBe "m1,m2"
    chunks(1).getAs[String]("bucket") shouldBe "20260724"
    chunks(1).getAs[String]("item_id") shouldBe "m3"
  }
}
