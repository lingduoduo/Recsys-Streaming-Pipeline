package com.demo.report

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class MovieCategoryReportJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

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

  "categoryRowOrNone" should "map a representative Redis hash to l1/l2/l3 identically to categoriesDf" in {
    val h: java.util.Map[String, String] = Map("genres" -> "Sci-Fi,Action", "releaseYear" -> "1999").asJava

    // categoryRowOrNone returns a plain, schema-less Row (it feeds an RDD that gets its schema
    // from createDataFrame later), so fields are read positionally here — column order matches
    // CategorySchema: item_id, l1, l2, l3.
    val row = MovieCategoryReportJob.categoryRowOrNone("item_1", h)
    row shouldBe defined
    row.get.getString(0) shouldBe "item_1"
    row.get.getString(1) shouldBe "SciFi&Fantasy"
    row.get.getString(2) shouldBe "Sci-Fi"
    row.get.getString(3) shouldBe "Sci-Fi·1990s"
  }

  it should "default the decade to unknown when releaseYear is absent" in {
    val h: java.util.Map[String, String] = Map("genres" -> "Comedy").asJava
    val row = MovieCategoryReportJob.categoryRowOrNone("item_2", h).get
    row.getString(3) shouldBe "Comedy·unknown"
  }

  it should "return None for a null hash (missing key)" in {
    MovieCategoryReportJob.categoryRowOrNone("missing", null) shouldBe None
  }

  it should "return None for an empty hash" in {
    MovieCategoryReportJob.categoryRowOrNone("missing", new java.util.HashMap[String, String]()) shouldBe None
  }

  "fetchMovieFeaturesDf" should "return an empty, correctly-shaped DataFrame without contacting Redis when there are no ids" in {
    val s = spark; import s.implicits._
    val ids = Seq.empty[String].toDF("item_id")

    // Host is unreachable on purpose: an empty ids DataFrame must never open a Jedis connection.
    val result = MovieCategoryReportJob.fetchMovieFeaturesDf(ids, "unreachable-host.invalid", 1, 1)

    result.columns.toSeq shouldBe Seq("item_id", "l1", "l2", "l3")
    result.isEmpty shouldBe true
  }
}
