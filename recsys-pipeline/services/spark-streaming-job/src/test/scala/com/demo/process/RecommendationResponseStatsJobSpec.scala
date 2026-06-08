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
      ("s1", "req_1", "user_1", "item_1", 0, 100L, 1, 0, 1.0, Map("subscription_level" -> "gold", "country_code" -> "US"), Map("type" -> "post"), Map("AdsBlenderType" -> "balanced")),
      ("s2", "req_1", "user_1", "item_2", 1, 100L, 0, 0, 0.0, Map("subscription_level" -> "gold", "country_code" -> "US"), Map("product_type" -> "ad"), Map("AdsBlenderType" -> "balanced")),
      ("s3", "req_1", "user_1", "item_3", 2, 100L, 0, 0, 0.0, Map("subscription_level" -> "gold", "country_code" -> "US"), Map("type" -> "post"), Map("AdsBlenderType" -> "balanced"))
    ).toDF("sample_id", "request_id", "user_id", "item_id", "position", "impression_ts", "clicked", "ordered", "label", "user_features", "item_features", "context_features")

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
      ("s1", "req_2", "user_2", "item_1", 0, 100L, 0, 0, 0.0, Map.empty[String, String], Map("type" -> "post"), Map.empty[String, String])
    ).toDF("sample_id", "request_id", "user_id", "item_id", "position", "impression_ts", "clicked", "ordered", "label", "user_features", "item_features", "context_features")

    val metrics = RecommendationResponseStatsJob.buildMetricEvents(
      ExperienceCollectorStreamingJob.buildSlates(samples)
    ).collect().map { row =>
      val tags = row.getAs[Map[String, String]]("tags")
      (tags, row.getAs[Long]("value"))
    }.toMap

    metrics(Map("type" -> "empty_ads", "stage" -> "response", "subscription" -> "none", "blender" -> "default")) shouldBe 1L
  }
}
