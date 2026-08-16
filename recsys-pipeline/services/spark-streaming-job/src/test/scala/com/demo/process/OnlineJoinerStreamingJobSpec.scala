package com.demo.process

import com.demo.event.{EventParsing, EventSchemas}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{coalesce, col, struct, to_json, unix_timestamp}
import org.apache.spark.sql.types.LongType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OnlineJoinerStreamingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("OnlineJoinerStreamingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  private def decodedJson(rawJson: DataFrame): DataFrame =
    EventParsing.fromJson(rawJson, EventSchemas.joiner)
      .withColumn("timestamp_ms", coalesce(col("timestamp_ms"), col("timestamp") * 1000L))

  "buildTrainingSamples" should "join impressions with click and order labels" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("sess_1", "req_1", "user_1", "item_1", "impression", 100L, 0, Map("tier" -> "gold"), Map("genre" -> "drama"), Map("device" -> "ios")),
      ("sess_1", "req_1", "user_1", "item_2", "impression", 100L, 1, Map("tier" -> "gold"), Map("genre" -> "comedy"), Map("device" -> "ios")),
      ("sess_1", "req_1", "user_1", "item_1", "click", 105L, 0, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String]),
      ("sess_1", "req_1", "user_1", "item_2", "order", 120L, 1, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String])
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position", "user_features", "item_features", "context_features")

    val samples = OnlineJoinerStreamingJob.buildTrainingSamples(events)
    val rows = samples
      .select("item_id", "clicked", "ordered", "label")
      .collect()
      .map(row => row.getString(0) -> (row.getInt(1), row.getInt(2), row.getDouble(3)))
      .toMap

    rows("item_1") shouldBe (1, 0, 1.0)
    rows("item_2") shouldBe (0, 1, 2.0)
    // session_id is carried through from the slate's events
    samples.select("session_id").distinct().collect().map(_.getString(0)) shouldBe Array("sess_1")
  }

  it should "keep unclicked impressions as negative samples" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("sess_2", "req_2", "user_1", "item_9", "exposure", 200L, 3, Map.empty[String, String], Map("genre" -> "news"), Map.empty[String, String])
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position", "user_features", "item_features", "context_features")

    val row = OnlineJoinerStreamingJob.buildTrainingSamples(events).first()

    row.getAs[Int]("clicked") shouldBe 0
    row.getAs[Int]("ordered") shouldBe 0
    row.getAs[Double]("label") shouldBe 0.0
  }

  // buildTrainingSamples is batch-local by design and stays that way. LateFeedbackJoin is the
  // layer that spans batches: see LateFeedbackJoinSpec for the end-to-end cross-batch guarantee.
  it should "drop feedback whose impression fell in an earlier batch" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val columns = Seq("session_id", "request_id", "user_id", "item_id", "event_type",
      "timestamp", "position", "user_features", "item_features", "context_features")
    val empty = Map.empty[String, String]

    // Batch 1: the impression alone.
    val firstBatch = Seq(
      ("sess_1", "req_1", "user_1", "item_1", "impression", 100L, 0,
        Map("tier" -> "gold"), Map("genre" -> "drama"), Map("device" -> "ios"))
    ).toDF(columns: _*)

    // Batch 2: the click alone, arriving after batch 1 already published.
    val secondBatch = Seq(
      ("sess_1", "req_1", "user_1", "item_1", "click", 105L, 0, empty, empty, empty)
    ).toDF(columns: _*)

    val first = OnlineJoinerStreamingJob.buildTrainingSamples(firstBatch)
      .select("item_id", "clicked", "ordered", "label").collect()
    val second = OnlineJoinerStreamingJob.buildTrainingSamples(secondBatch).collect()

    // The impression publishes immediately, labelled "not clicked".
    first.length shouldBe 1
    (first.head.getInt(1), first.head.getInt(2), first.head.getDouble(3)) shouldBe (0, 0, 0.0)
    // The click is discarded, and nothing restates the sample published above.
    second shouldBe Array.empty[org.apache.spark.sql.Row]
  }

  it should "still label a click that arrives in the same batch" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val columns = Seq("session_id", "request_id", "user_id", "item_id", "event_type",
      "timestamp", "position", "user_features", "item_features", "context_features")
    val empty = Map.empty[String, String]

    val batch = Seq(
      ("sess_1", "req_1", "user_1", "item_1", "impression", 100L, 0,
        Map("tier" -> "gold"), Map("genre" -> "drama"), Map("device" -> "ios")),
      ("sess_1", "req_1", "user_1", "item_1", "click", 105L, 0, empty, empty, empty)
    ).toDF(columns: _*)

    val rows = OnlineJoinerStreamingJob.buildTrainingSamples(batch)
      .select("clicked", "label").collect()

    rows.length shouldBe 1
    (rows.head.getInt(0), rows.head.getDouble(1)) shouldBe (1, 1.0)
  }

  it should "normalize decoded canonical events and ignore Kafka lineage" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Simulate events arriving with timestamp_ms (millis) and no legacy timestamp.
    // This mirrors what parseEvents produces after normalisation.
    val events = Seq(
      // impression at ms 1718400000000
      ("sess_3", "req_3", "user_5", "movie_3", "impression", 1718400000000L,
        0, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String]),
      // click at ms 1718400005000
      ("sess_3", "req_3", "user_5", "movie_3", "click", 1718400005000L,
        0, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String])
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp_ms",
           "position", "user_features", "item_features", "context_features")
      .withColumn("event_id", org.apache.spark.sql.functions.concat_ws("-", col("event_type"), col("item_id")))
      .withColumn("kafka_topic", org.apache.spark.sql.functions.lit("recsys_events"))
      .withColumn("kafka_offset", org.apache.spark.sql.functions.lit(10L))

    val normalised = OnlineJoinerStreamingJob.parseEvents(events)
    normalised.columns should not contain "kafka_topic"
    normalised.columns should not contain "kafka_offset"

    val rows = OnlineJoinerStreamingJob.buildTrainingSamples(normalised).collect()
    rows should have length 1
    rows.head.getAs[Int]("clicked") shouldBe 1

    val impressionTime = rows.head.getAs[java.sql.Timestamp]("impression_time")
    impressionTime should not be null
    // impression_time should be in year 2024, not 54426
    impressionTime.toLocalDateTime.getYear shouldBe 2024
  }

  it should "preserve Long timestamp and delay fields through the training-sample Kafka contract" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val rawEvents = Seq(
      """{"event_id":"legacy-impression","session_id":"legacy-session","request_id":"legacy-request","user_id":"legacy-user","item_id":"legacy-item","event_type":"impression","timestamp":100,"position":0}""",
      """{"event_id":"legacy-feedback","session_id":"legacy-session","request_id":"legacy-request","user_id":"legacy-user","item_id":"legacy-item","event_type":"click","timestamp":105,"position":0}""",
      """{"event_id":"millis-impression","session_id":"millis-session","request_id":"millis-request","user_id":"millis-user","item_id":"millis-item","event_type":"impression","timestamp_ms":200000,"position":0}""",
      """{"event_id":"millis-feedback","session_id":"millis-session","request_id":"millis-request","user_id":"millis-user","item_id":"millis-item","event_type":"order","timestamp_ms":205000,"position":0}"""
    ).toDF("value")

    val samples = OnlineJoinerStreamingJob.buildTrainingSamples(
      OnlineJoinerStreamingJob.parseEvents(decodedJson(rawEvents)))
    Seq("impression_ts", "last_feedback_ts", "feedback_delay_ms").foreach { field =>
      samples.schema(field).dataType shouldBe LongType
    }

    val kafkaSamples = samples.select(to_json(struct(samples.columns.map(col): _*)).as("value"))
    val decoded = ExperienceCollectorStreamingJob.parseSamples(kafkaSamples)
    Seq("impression_ts", "last_feedback_ts", "feedback_delay_ms").foreach { field =>
      decoded.schema(field).dataType shouldBe LongType
    }
    val delays = decoded.select("request_id", "impression_ts", "last_feedback_ts", "feedback_delay_ms")
      .collect()
      .map(row => row.getAs[String]("request_id") ->
        (row.getAs[Long]("impression_ts"), row.getAs[Long]("last_feedback_ts"), row.getAs[Long]("feedback_delay_ms")))
      .toMap

    delays("legacy-request") shouldBe (100L, 105L, 5000L)
    delays("millis-request") shouldBe (200L, 205L, 5000L)
  }

  it should "attribute all feedback fields to the latest feedback event regardless of input order" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      """{"event_id":"impression","session_id":"session","request_id":"request","user_id":"user","item_id":"item","event_type":"impression","timestamp":100,"position":0,"model_version":"impression-model","policy_version":"impression-policy","algorithm_version":"impression-algorithm","published_at":50,"new_release":true,"filter_reason":"catalog","unsafe_label":false}""",
      """{"event_id":"click","session_id":"session","request_id":"request","user_id":"user","item_id":"item","event_type":"click","timestamp":105,"rating":1.0,"negative_feedback_reason":"click-reason","dwell_millis":1000,"completion_rate":0.1,"model_version":"feedback-model"}""",
      """{"event_id":"order","session_id":"session","request_id":"request","user_id":"user","item_id":"item","event_type":"order","timestamp":110,"rating":5.0,"negative_feedback_reason":"order-reason","dwell_millis":5000,"completion_rate":0.9,"model_version":"later-feedback-model"}"""
    )

    def attribution(input: Seq[String]) =
      OnlineJoinerStreamingJob.buildTrainingSamples(
        OnlineJoinerStreamingJob.parseEvents(decodedJson(input.toDF("value"))))
        .select(
          "model_version", "policy_version", "algorithm_version", "last_feedback_ts", "feedback_delay_ms",
          "rating", "negative_feedback_reason", "dwell_millis", "completion_rate"
        )
        .first()

    val forward = attribution(events)
    val reversed = attribution(events.reverse)
    Seq("model_version", "policy_version", "algorithm_version", "last_feedback_ts", "feedback_delay_ms",
      "rating", "negative_feedback_reason", "dwell_millis", "completion_rate").foreach { field =>
      forward.getAs[Any](field) shouldBe reversed.getAs[Any](field)
    }

    forward.getAs[String]("model_version") shouldBe "impression-model"
    forward.getAs[String]("policy_version") shouldBe "impression-policy"
    forward.getAs[String]("algorithm_version") shouldBe "impression-algorithm"
    forward.getAs[Long]("last_feedback_ts") shouldBe 110L
    forward.getAs[Long]("feedback_delay_ms") shouldBe 10000L
    forward.getAs[Double]("rating") shouldBe 5.0
    forward.getAs[String]("negative_feedback_reason") shouldBe "order-reason"
    forward.getAs[Long]("dwell_millis") shouldBe 5000L
    forward.getAs[Double]("completion_rate") shouldBe 0.9
  }

  it should "keep only the latest feedback event's measurement fields when they are disjoint" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // The click carries engagement signals and the order carries only a rating: exactly the
    // shape a producer emits when it splits measurement fields across the two feedback events.
    // One max_by over the whole struct means the later event wins wholesale, so the click's
    // fields do NOT survive. Producers must repeat them on the later event.
    val impression =
      """{"event_id":"impression","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"impression","timestamp":100,"position":0}"""
    val click =
      """{"event_id":"click","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"click","timestamp":105,"negative_feedback_reason":"not_interested","dwell_millis":9000,"completion_rate":0.08}"""
    val ratingOnlyOrder =
      """{"event_id":"order","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"order","timestamp":110,"rating":4.5}"""

    def sample(events: Seq[String]) =
      OnlineJoinerStreamingJob.buildTrainingSamples(
        OnlineJoinerStreamingJob.parseEvents(decodedJson(events.toDF("value")))).first()

    val row = sample(Seq(impression, click, ratingOnlyOrder))

    row.getAs[Double]("rating") shouldBe 4.5
    row.getAs[AnyRef]("negative_feedback_reason") shouldBe null
    row.getAs[AnyRef]("dwell_millis") shouldBe null
    row.getAs[AnyRef]("completion_rate") shouldBe null
    // The click still counts as a click; only its measurement struct is superseded.
    row.getAs[Int]("clicked") shouldBe 1
    row.getAs[Int]("ordered") shouldBe 1

    // Repeating the click's fields on the order is what preserves them.
    val carryingOrder =
      """{"event_id":"order","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"order","timestamp":110,"rating":4.5,"negative_feedback_reason":"not_interested","dwell_millis":9000,"completion_rate":0.08}"""
    val preserved = sample(Seq(impression, click, carryingOrder))

    preserved.getAs[Double]("rating") shouldBe 4.5
    preserved.getAs[String]("negative_feedback_reason") shouldBe "not_interested"
    preserved.getAs[Long]("dwell_millis") shouldBe 9000L
    preserved.getAs[Double]("completion_rate") shouldBe 0.08
  }

  it should "preserve nullable measurement attribution without changing legacy events" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val raw = Seq(
      """{"event_id":"legacy","session_id":"sess_legacy","request_id":"req_legacy","user_id":"user_legacy","item_id":"item_legacy","event_type":"impression","timestamp":100,"position":0}""",
      """{"event_id":"enriched-impression","session_id":"sess_enriched","request_id":"req_enriched","user_id":"user_enriched","item_id":"item_enriched","event_type":"impression","timestamp":100,"position":1,"model_version":"model-v2","policy_version":"policy-v3","algorithm_version":"algo-v4","published_at":50,"new_release":true,"filter_reason":"muted_genre","unsafe_label":false}""",
      """{"event_id":"enriched-feedback","session_id":"sess_enriched","request_id":"req_enriched","user_id":"user_enriched","item_id":"item_enriched","event_type":"click","timestamp":105,"rating":4.5,"negative_feedback_reason":"not_interested","dwell_millis":12000,"completion_rate":0.75}"""
    ).toDF("value")

    val rows = OnlineJoinerStreamingJob.buildTrainingSamples(
      OnlineJoinerStreamingJob.parseEvents(decodedJson(raw)))
      .select(
        "request_id", "model_version", "policy_version", "algorithm_version", "rating",
        "negative_feedback_reason", "dwell_millis", "completion_rate", "published_at",
        "new_release", "filter_reason", "unsafe_label", "last_feedback_ts", "feedback_delay_ms"
      )
      .collect()
      .map(row => row.getAs[String]("request_id") -> row)
      .toMap

    val legacy = rows("req_legacy")
    Seq(
      "model_version", "policy_version", "algorithm_version", "rating", "negative_feedback_reason",
      "dwell_millis", "completion_rate", "published_at", "new_release", "filter_reason", "unsafe_label",
      "last_feedback_ts", "feedback_delay_ms"
    ).foreach(field => legacy.getAs[AnyRef](field) shouldBe null)

    val enriched = rows("req_enriched")
    enriched.getAs[String]("model_version") shouldBe "model-v2"
    enriched.getAs[String]("policy_version") shouldBe "policy-v3"
    enriched.getAs[String]("algorithm_version") shouldBe "algo-v4"
    enriched.getAs[Double]("rating") shouldBe 4.5
    enriched.getAs[String]("negative_feedback_reason") shouldBe "not_interested"
    enriched.getAs[Long]("dwell_millis") shouldBe 12000L
    enriched.getAs[Double]("completion_rate") shouldBe 0.75
    enriched.getAs[Long]("published_at") shouldBe 50L
    enriched.getAs[Boolean]("new_release") shouldBe true
    enriched.getAs[String]("filter_reason") shouldBe "muted_genre"
    enriched.getAs[Boolean]("unsafe_label") shouldBe false
    enriched.getAs[Long]("last_feedback_ts") shouldBe 105L
    enriched.getAs[Long]("feedback_delay_ms") shouldBe 5000L
  }

  "enrichWithCatalog" should "attach genres/tags by item_id and fill empty arrays for misses" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq("item_1", "item_2").toDF("item_id")
    val catalog = Seq(
      ("item_1", Seq("drama", "crime"), Seq("classic"))
    ).toDF("item_id", "genres", "tags")

    val out = OnlineJoinerStreamingJob.enrichWithCatalog(samples, catalog)
      .collect()
      .map(r => r.getString(r.fieldIndex("item_id")) ->
        (r.getAs[Seq[String]]("genres"), r.getAs[Seq[String]]("tags")))
      .toMap

    out("item_1") shouldBe (Seq("drama", "crime"), Seq("classic"))
    out("item_2") shouldBe (Seq.empty[String], Seq.empty[String])
  }

  "withCatalog" should "add empty genres/tags columns when no catalog is configured" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq("item_1").toDF("item_id")
    val out = OnlineJoinerStreamingJob.withCatalog(samples, None)

    out.columns should contain allOf ("genres", "tags")
    val row = out.first()
    row.getAs[Seq[String]]("genres") shouldBe Seq.empty[String]
    row.getAs[Seq[String]]("tags") shouldBe Seq.empty[String]
  }

  "loadCatalog" should "parse the object-map catalog JSON into (item_id, genres, tags)" in {
    import java.nio.file.{Files, Paths}
    val dir = Files.createTempDirectory("catalog").toFile
    val file = new java.io.File(dir, "catalog.json")
    Files.write(file.toPath,
      """{"item1":{"title":"A","genres":["sci-fi","adventure"],"tags":["space"]},
        | "item2":{"title":"B","genres":["drama"],"tags":[]}}""".stripMargin.getBytes)

    val rows = OnlineJoinerStreamingJob.loadCatalog(spark, file.getAbsolutePath)
      .collect()
      .map(r => r.getString(0) -> r.getAs[Seq[String]]("genres"))
      .toMap

    rows("item1") shouldBe Seq("sci-fi", "adventure")
    rows("item2") shouldBe Seq("drama")
  }

  "a null position" should "survive as null rather than becoming slot 0" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val noFeatures = Map.empty[String, String]
    val events = Seq(
      ("s", "req_np", "u", "item_unknown", "impression", 100L, None: Option[Int],
        noFeatures, noFeatures, noFeatures),
      ("s", "req_np", "u", "item_first", "impression", 100L, Some(0),
        noFeatures, noFeatures, noFeatures)
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position",
      "user_features", "item_features", "context_features")

    val byItem = OnlineJoinerStreamingJob.buildTrainingSamples(events)
      .select("item_id", "position").collect()
      .map(r => r.getString(0) -> (if (r.isNullAt(1)) None else Some(r.getInt(1)))).toMap

    byItem("item_unknown") shouldBe None
    byItem("item_first") shouldBe Some(0)
  }

  "feedback_delay_ms" should "report sub-second and non-round delays exactly" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Impression and feedback with DIFFERENT sub-second remainders. The producers cannot generate
    // this shape — every slate shares one now_ms and feedback lands on whole-second offsets, so
    // truncation to seconds cancels there and hides the defect.
    val impressionMs = 1780356600400L
    val noFeatures = Map.empty[String, String]
    def row(requestId: String, itemId: String, eventType: String, ms: Long) =
      ("s", requestId, "u", itemId, eventType, ms / 1000L, ms, 0,
        noFeatures, noFeatures, noFeatures)

    val events = Seq(
      row("req_ms", "i1", "impression", impressionMs),
      row("req_ms", "i1", "click", impressionMs + 1500L),
      row("req_sub", "i2", "impression", impressionMs),
      row("req_sub", "i2", "click", impressionMs + 400L)
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type",
      "timestamp", "timestamp_ms", "position", "user_features", "item_features", "context_features")

    val delays = OnlineJoinerStreamingJob.buildTrainingSamples(events)
      .select("request_id", "feedback_delay_ms")
      .collect().map(r => r.getString(0) -> r.getAs[Long]("feedback_delay_ms")).toMap

    delays("req_ms") shouldBe 1500L
    delays("req_sub") shouldBe 400L
  }

  it should "fall back to second precision when timestamp_ms is absent" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val noFeatures = Map.empty[String, String]
    val events = Seq(
      ("s", "req_legacy", "u", "i", "impression", 100L, 0, noFeatures, noFeatures, noFeatures),
      ("s", "req_legacy", "u", "i", "click", 110L, 0, noFeatures, noFeatures, noFeatures)
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position",
      "user_features", "item_features", "context_features")

    OnlineJoinerStreamingJob.buildTrainingSamples(events)
      .select("feedback_delay_ms").collect().head.getAs[Long]("feedback_delay_ms") shouldBe 10000L
  }

  "impression_time" should "survive a DST fall-back hour in any session time zone" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // US Eastern falls back 2026-11-01 06:00Z. Both instants format to the same local wall clock
    // "2026-11-01 01:30:00", so a to_timestamp(from_unixtime(...)) round trip collapses them and
    // moves the later one an hour into the past.
    val beforeFallBack = 1793511000L // 05:30Z = 01:30 EDT
    val afterFallBack = 1793514600L // 06:30Z = 01:30 EST
    val noFeatures = Map.empty[String, String]

    val events = Seq(
      ("s", "req_edt", "u", "i1", "impression", beforeFallBack, 0, noFeatures, noFeatures, noFeatures),
      ("s", "req_est", "u", "i2", "impression", afterFallBack, 0, noFeatures, noFeatures, noFeatures)
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position",
      "user_features", "item_features", "context_features")

    def instantsIn(zone: String): Map[String, Long] = {
      val previous = sparkSession.conf.get("spark.sql.session.timeZone")
      sparkSession.conf.set("spark.sql.session.timeZone", zone)
      try
        OnlineJoinerStreamingJob.buildTrainingSamples(events)
          .select(col("request_id"), unix_timestamp(col("impression_time")).as("instant"))
          .collect().map(r => r.getString(0) -> r.getAs[Long]("instant")).toMap
      finally sparkSession.conf.set("spark.sql.session.timeZone", previous)
    }

    val expected = Map("req_edt" -> beforeFallBack, "req_est" -> afterFallBack)
    instantsIn("UTC") shouldBe expected
    instantsIn("America/New_York") shouldBe expected
  }

  "withDatePartition" should "produce the same date in every session time zone" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // 2026-06-01T23:30:00Z: Tokyo is already on 06-02, New York is still on 06-01.
    val lateInTheUtcDay = 1780356600L
    val events = Seq(
      ("s", "req_tz", "user_tz", "item_tz", "impression", lateInTheUtcDay, 0,
        Map.empty[String, String], Map.empty[String, String], Map.empty[String, String])
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position",
      "user_features", "item_features", "context_features")
    val samples = OnlineJoinerStreamingJob.buildTrainingSamples(events)

    val dates = Seq("UTC", "America/New_York", "Asia/Tokyo").map { zone =>
      val previous = sparkSession.conf.get("spark.sql.session.timeZone")
      sparkSession.conf.set("spark.sql.session.timeZone", zone)
      try
        OnlineJoinerStreamingJob.withDatePartition(samples, None)
          .select("date").collect().head.getAs[java.sql.Date]("date").toString
      finally sparkSession.conf.set("spark.sql.session.timeZone", previous)
    }

    dates.distinct shouldBe Seq("2026-06-01")
  }

  "dedupedEvents" should "drop duplicate event_id within the watermark across micro-batches" in {
    val s = spark; import s.implicits._
    implicit val sqlCtx = s.sqlContext
    val input = MemoryStream[String]
    val deduped = OnlineJoinerStreamingJob.dedupedEvents(decodedJson(input.toDF()), "10 minutes")
    val q = deduped.writeStream.format("memory").queryName("oj_out").outputMode("append").start()
    try {
      val e = """{"event_id":"x1","request_id":"r1","user_id":"u1","item_id":"i1","event_type":"impression","timestamp_ms":1718400000000,"position":0}"""
      input.addData(e); q.processAllAvailable()
      input.addData(e); q.processAllAvailable()
      s.table("oj_out").count() shouldBe 1
    } finally q.stop()
  }
}
