package com.demo.process

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExperienceCollectorStreamingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("ExperienceCollectorStreamingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "buildSlates" should "collect item samples into a position-sorted slate" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq(
      ("s2", "sess_1", "req_1", "user_1", "item_2", 1, 100L, 0, 0, 0.0, Map("tier" -> "gold"), Map("genre" -> "comedy"), Map("device" -> "ios")),
      ("s1", "sess_1", "req_1", "user_1", "item_1", 0, 100L, 1, 0, 1.0, Map("tier" -> "gold"), Map("genre" -> "drama"), Map("device" -> "ios"))
    ).toDF("sample_id", "session_id", "request_id", "user_id", "item_id", "position", "impression_ts", "clicked", "ordered", "label", "user_features", "item_features", "context_features")

    val row = ExperienceCollectorStreamingJob.buildSlates(samples).first()
    val items = row.getAs[Seq[org.apache.spark.sql.Row]]("items")

    row.getAs[String]("slate_id") shouldBe "req_1:user_1"
    row.getAs[String]("session_id") shouldBe "sess_1"
    row.getAs[Int]("slate_clicked") shouldBe 1
    row.getAs[Double]("slate_reward") shouldBe 1.0
    row.getAs[Int]("slate_size") shouldBe 2
    items.map(_.getAs[String]("item_id")) shouldBe Seq("item_1", "item_2")
  }

  it should "preserve measurement fields in their ordered item attribution" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val baseSamples = Seq(
      ("s2", "sess_1", "req_1", "user_1", "item_2", 1, 100L, 0, 0, 0.0,
        Map.empty[String, String], Map.empty[String, String], Map.empty[String, String]),
      ("s1", "sess_1", "req_1", "user_1", "item_1", 0, 100L, 1, 0, 1.0,
        Map.empty[String, String], Map.empty[String, String], Map.empty[String, String])
    ).toDF(
      "sample_id", "session_id", "request_id", "user_id", "item_id", "position", "impression_ts",
      "clicked", "ordered", "label", "user_features", "item_features", "context_features"
    )
    val enrichedItem = col("item_id") === "item_1"
    val samples = baseSamples
      .withColumn("model_version", when(enrichedItem, lit("model-v2")))
      .withColumn("policy_version", when(enrichedItem, lit("policy-v3")))
      .withColumn("algorithm_version", when(enrichedItem, lit("algo-v4")))
      .withColumn("rating", when(enrichedItem, lit(4.5)))
      .withColumn("negative_feedback_reason", when(enrichedItem, lit("not_interested")))
      .withColumn("dwell_millis", when(enrichedItem, lit(12000L)))
      .withColumn("completion_rate", when(enrichedItem, lit(0.75)))
      .withColumn("published_at", when(enrichedItem, lit(50L)))
      .withColumn("new_release", when(enrichedItem, lit(true)))
      .withColumn("filter_reason", when(enrichedItem, lit("muted_genre")))
      .withColumn("unsafe_label", when(enrichedItem, lit(false)))
      .withColumn("last_feedback_ts", when(enrichedItem, lit(105L)))
      .withColumn("feedback_delay_ms", when(enrichedItem, lit(5000L)))

    val items = ExperienceCollectorStreamingJob.buildSlates(samples).first()
      .getAs[Seq[org.apache.spark.sql.Row]]("items")

    val legacy = items(1)
    Seq(
      "model_version", "policy_version", "algorithm_version", "rating", "negative_feedback_reason",
      "dwell_millis", "completion_rate", "published_at", "new_release", "filter_reason", "unsafe_label",
      "last_feedback_ts", "feedback_delay_ms"
    ).foreach(field => legacy.getAs[AnyRef](field) shouldBe null)

    val enriched = items.head
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

  it should "not create a Parquet sink when no output path is configured" in {
    ExperienceCollectorStreamingJob.parquetSink("", 1) shouldBe None
  }

  it should "write slates as date-partitioned Parquet when an output path is configured" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq(
      """{"sample_id":"s1","request_id":"r1","user_id":"u1","item_id":"item_1","position":0,"impression_ts":1753000000,"clicked":1,"ordered":0,"label":1.0}""",
      """{"sample_id":"s2","request_id":"r1","user_id":"u1","item_id":"item_2","position":1,"impression_ts":1753000000,"clicked":0,"ordered":0,"label":0.0}"""
    ).toDF("value")
    val slates = ExperienceCollectorStreamingJob.buildSlates(
      ExperienceCollectorStreamingJob.parseSamples(samples))

    val target = java.nio.file.Files.createTempDirectory("slate-sink").resolve("slates").toString
    ExperienceCollectorStreamingJob.parquetSink(target, 1).get.write(slates, 0L)

    val written = sparkSession.read.parquet(target)
    written.count() shouldBe 1
    written.columns should contain allOf ("slate_id", "request_id", "items", "date")
    written.select("items").first().getAs[Seq[org.apache.spark.sql.Row]](0).size shouldBe 2
  }

  "withDatePartition" should "produce the same date in every session time zone" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // 2026-06-01T23:30:00Z: Tokyo is already on 06-02, New York is still on 06-01.
    val slates = Seq((1780356600L, "slate_tz")).toDF("request_ts", "slate_id")

    val dates = Seq("UTC", "America/New_York", "Asia/Tokyo").map { zone =>
      val previous = sparkSession.conf.get("spark.sql.session.timeZone")
      sparkSession.conf.set("spark.sql.session.timeZone", zone)
      try
        ExperienceCollectorStreamingJob.withDatePartition(slates)
          .select("date").collect().head.getAs[java.sql.Date]("date").toString
      finally sparkSession.conf.set("spark.sql.session.timeZone", previous)
    }

    dates.distinct shouldBe Seq("2026-06-01")
  }
}
