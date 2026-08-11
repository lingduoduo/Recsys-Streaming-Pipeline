package com.demo.report

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MovieCategoryReportJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "withinLookback" should "keep only the most recent N partition dates" in {
    val s = spark; import s.implicits._
    val df = Seq(
      ("item_1", "2024-06-01"),
      ("item_2", "2024-06-07"),
      ("item_3", "2024-06-08"),
      ("item_4", "2024-06-10")
    ).toDF("item_id", "date")

    val kept = MovieCategoryReportJob.withinLookback(df, 3)
      .collect().map(_.getAs[String]("item_id")).toSet

    // Anchored to the newest date present (2024-06-10), not the wall clock, so a report over
    // historical data is deterministic.
    kept shouldBe Set("item_3", "item_4")
  }

  it should "read every partition when the lookback is not positive" in {
    val s = spark; import s.implicits._
    val df = Seq(("item_1", "2024-06-01"), ("item_2", "2024-06-10")).toDF("item_id", "date")

    MovieCategoryReportJob.withinLookback(df, 0).count() shouldBe 2L
    MovieCategoryReportJob.withinLookback(df, -1).count() shouldBe 2L
  }

  it should "pass input through unchanged when it has no date column" in {
    val s = spark; import s.implicits._
    val df = Seq(("item_1", 1, 0)).toDF("item_id", "clicked", "ordered")

    MovieCategoryReportJob.withinLookback(df, 7).count() shouldBe 1L
  }

  "perItemEngagement" should "count impressions and sum clicks/orders per item" in {
    val s = spark; import s.implicits._
    val df = Seq(
      ("item_1", 1, 0),
      ("item_1", 0, 0),
      ("item_1", 1, 1),
      ("item_2", 0, 0)
    ).toDF("item_id", "clicked", "ordered")

    val rows = MovieCategoryReportJob.perItemEngagement(df)
      .collect().map(r => r.getAs[String]("item_id") -> r).toMap

    rows("item_1").getAs[Long]("impressions") shouldBe 3L
    rows("item_1").getAs[Long]("clicks") shouldBe 2L
    rows("item_1").getAs[Long]("orders") shouldBe 1L
    rows("item_2").getAs[Long]("impressions") shouldBe 1L
    rows("item_2").getAs[Long]("clicks") shouldBe 0L
  }

  "categoriesDf" should "map Redis feature hashes to l1/l2/l3 rows" in {
    val features = Map(
      "item_1" -> Map("genres" -> "Sci-Fi,Action", "releaseYear" -> "1999"),
      "item_2" -> Map("genres" -> "Comedy")  // no year -> unknown decade
    )
    val rows = MovieCategoryReportJob.categoriesDf(spark, features)
      .collect().map(r => r.getAs[String]("item_id") -> r).toMap

    rows("item_1").getAs[String]("l1") shouldBe "SciFi&Fantasy"
    rows("item_1").getAs[String]("l2") shouldBe "Sci-Fi"
    rows("item_1").getAs[String]("l3") shouldBe "Sci-Fi·1990s"
    rows("item_2").getAs[String]("l3") shouldBe "Comedy·unknown"
  }

  "categoryMetrics" should "aggregate per category with ctr, order_rate, clicks_per_item and lift" in {
    val s = spark; import s.implicits._
    // two items in the same l2; overall CTR = 0.25 (3 clicks / 12 impressions)
    val joined = Seq(
      ("item_1", 10L, 3L, 1L, "Sci-Fi"),
      ("item_2", 2L, 0L, 0L, "Sci-Fi")
    ).toDF("item_id", "impressions", "clicks", "orders", "l2")

    val out = MovieCategoryReportJob.categoryMetrics(joined, "l2", overallCtr = 0.25)
    out.columns should contain allOf ("impressions", "clicks", "orders", "items", "ctr", "order_rate", "clicks_per_item", "ctr_lift_pct")

    val r = out.collect().head
    r.getAs[Long]("impressions") shouldBe 12L
    r.getAs[Long]("clicks") shouldBe 3L
    r.getAs[Long]("items") shouldBe 2L
    r.getAs[Double]("ctr") shouldBe 0.25
    r.getAs[Double]("clicks_per_item") shouldBe 1.5
    r.getAs[Double]("ctr_lift_pct") shouldBe 0.0  // equals overall
  }

  "fetchMovieFeatures" should "return empty map for no ids (no Redis call)" in {
    MovieCategoryReportJob.fetchMovieFeatures(Array.empty, "localhost", 6379, 8) shouldBe Map.empty
  }
}
