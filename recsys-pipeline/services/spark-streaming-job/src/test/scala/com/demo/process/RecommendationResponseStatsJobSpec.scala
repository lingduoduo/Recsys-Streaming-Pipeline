package com.demo.process

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RecommendationResponseStatsJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("RecommendationResponseStatsJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "buildMetricEvents" should "emit global response stats with subscription country and blender tags" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq(
      ("s1", "sess_1", "req_1", "user_1", "item_1", 0, 100L, 1, 0, 1.0, Map("subscription_level" -> "gold", "country_code" -> "US"), Map("type" -> "movie"), Map("AdsBlenderType" -> "balanced")),
      ("s2", "sess_1", "req_1", "user_1", "item_2", 1, 100L, 0, 0, 0.0, Map("subscription_level" -> "gold", "country_code" -> "US"), Map("product_type" -> "ad"), Map("AdsBlenderType" -> "balanced")),
      ("s3", "sess_1", "req_1", "user_1", "item_3", 2, 100L, 0, 0, 0.0, Map("subscription_level" -> "gold", "country_code" -> "US"), Map("type" -> "movie"), Map("AdsBlenderType" -> "balanced"))
    ).toDF("sample_id", "session_id", "request_id", "user_id", "item_id", "position", "impression_ts", "clicked", "ordered", "label", "user_features", "item_features", "context_features")

    val metrics = RecommendationResponseStatsJob.buildMetricEvents(
      ExperienceCollectorStreamingJob.buildSlates(samples)
    ).collect().map { row =>
      val tags = row.getAs[Map[String, String]]("tags")
      (tags, row.getAs[Long]("value"))
    }.toMap

    metrics(Map("type" -> "total", "subscription" -> "gold")) shouldBe 1L
    metrics(Map("type" -> "total", "subscription" -> "gold", "country" -> "us")) shouldBe 1L
    metrics(Map("type" -> "items", "subscription" -> "gold", "blender" -> "balanced")) shouldBe 2L
    metrics(Map("type" -> "ads", "subscription" -> "gold", "blender" -> "balanced")) shouldBe 1L
  }

  it should "emit response guardrail metrics for empty ads" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq(
      ("s1", "sess_2", "req_2", "user_2", "item_1", 0, 100L, 0, 0, 0.0, Map.empty[String, String], Map("type" -> "movie"), Map.empty[String, String])
    ).toDF("sample_id", "session_id", "request_id", "user_id", "item_id", "position", "impression_ts", "clicked", "ordered", "label", "user_features", "item_features", "context_features")

    val metrics = RecommendationResponseStatsJob.buildMetricEvents(
      ExperienceCollectorStreamingJob.buildSlates(samples)
    ).collect().map { row =>
      val tags = row.getAs[Map[String, String]]("tags")
      (tags, row.getAs[Long]("value"))
    }.toMap

    metrics(Map("type" -> "empty_ads", "stage" -> "response", "subscription" -> "none", "blender" -> "default")) shouldBe 1L
  }

  it should "emit non-negative feedback and Kafka ingest delay measurements with bounded tags" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val rawKafka = Seq(
      ("""{"slate_id":"slate-1","request_id":"request-secret","user_id":"user-secret","request_ts":100,"user_features":{"subscription_level":"gold"},"context_features":{"country_code":"US"},"items":[{"position":0,"item_id":"item-1","clicked":1,"ordered":0,"label":1.0,"feedback_delay_ms":5000},{"position":1,"item_id":"item-2","clicked":0,"ordered":0,"label":0.0,"feedback_delay_ms":0},{"position":2,"item_id":"item-3","clicked":0,"ordered":0,"label":0.0,"feedback_delay_ms":-1}]}""", new java.sql.Timestamp(105000L))
    ).toDF("value", "timestamp")

    val metrics = RecommendationResponseStatsJob.buildMetricEvents(
      RecommendationResponseStatsJob.parseSlates(rawKafka)
    ).collect().map { row =>
      row.getAs[Map[String, String]]("tags") -> row.getAs[Long]("value")
    }.toSeq

    metrics.collect {
      case (tags, value) if tags.get("type").contains("feedback_delay_ms") => value
    }.toSet shouldBe Set(0L, 5000L)
    metrics.collect {
      case (tags, value) if tags.get("type").contains("kafka_ingest_lag_ms") => value
    } shouldBe Seq(5000L)
    metrics.exists(_._1.get("type").contains("feedback_delay_ms")) shouldBe true
    metrics.map(_._1).foreach { tags =>
      tags should not contain key ("request_id")
      tags should not contain key ("user_id")
      tags.values should not contain "request-secret"
      tags.values should not contain "user-secret"
    }
  }
}
