package com.demo.process

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{coalesce, col, struct, to_json}
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

  it should "parse unified schema with timestamp_ms field" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Simulate events arriving with timestamp_ms (millis) and no legacy timestamp.
    // This mirrors what parseEvents produces after normalisation.
    val events = Seq(
      // impression at ms 1718400000000
      ("sess_3", "req_3", "user_5", "movie_3", "impression", Some(1718400000000L), None: Option[Long],
        0, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String]),
      // click at ms 1718400005000
      ("sess_3", "req_3", "user_5", "movie_3", "click", Some(1718400005000L), None: Option[Long],
        0, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String])
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp_ms", "timestamp",
           "position", "user_features", "item_features", "context_features")

    // Apply the same normalisation that parseEvents performs
    val normalised = events
      .withColumn("timestamp", coalesce(col("timestamp_ms") / 1000L, col("timestamp")))
      .drop("timestamp_ms")

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

    val samples = OnlineJoinerStreamingJob.buildTrainingSamples(OnlineJoinerStreamingJob.parseEvents(rawEvents))
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
      OnlineJoinerStreamingJob.buildTrainingSamples(OnlineJoinerStreamingJob.parseEvents(input.toDF("value")))
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
      OnlineJoinerStreamingJob.buildTrainingSamples(OnlineJoinerStreamingJob.parseEvents(events.toDF("value"))).first()

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

    val rows = OnlineJoinerStreamingJob.buildTrainingSamples(OnlineJoinerStreamingJob.parseEvents(raw))
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

  "dedupedEvents" should "drop duplicate event_id within the watermark across micro-batches" in {
    val s = spark; import s.implicits._
    implicit val sqlCtx = s.sqlContext
    val input = MemoryStream[String]
    val deduped = OnlineJoinerStreamingJob.dedupedEvents(input.toDF(), "10 minutes")
    val q = deduped.writeStream.format("memory").queryName("oj_out").outputMode("append").start()
    try {
      val e = """{"event_id":"x1","request_id":"r1","user_id":"u1","item_id":"i1","event_type":"impression","timestamp_ms":1718400000000,"position":0}"""
      input.addData(e); q.processAllAvailable()
      input.addData(e); q.processAllAvailable()
      s.table("oj_out").count() shouldBe 1
    } finally q.stop()
  }
}
