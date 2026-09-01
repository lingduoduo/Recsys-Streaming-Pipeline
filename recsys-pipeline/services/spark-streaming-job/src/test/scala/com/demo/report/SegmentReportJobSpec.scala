package com.demo.report

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class SegmentReportJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "perUserEngagement" should "count impressions and sum clicks/orders per user" in {
    val s = spark; import s.implicits._
    val df = Seq(
      ("u1", 1, 0), ("u1", 0, 0), ("u1", 1, 1), ("u2", 0, 0)
    ).toDF("user_id", "clicked", "ordered")

    val rows = SegmentReportJob.perUserEngagement(df)
      .collect().map(r => r.getAs[String]("user_id") -> r).toMap
    rows("u1").getAs[Long]("impressions") shouldBe 3L
    rows("u1").getAs[Long]("clicks") shouldBe 2L
    rows("u1").getAs[Long]("orders") shouldBe 1L
    rows("u2").getAs[Long]("clicks") shouldBe 0L
  }

  "deviceMetrics" should "break down CTR by the typed device column when present" in {
    val s = spark; import s.implicits._
    // u1 ios (1 click / 2), u2 web (2 clicks / 2, 1 order); overall CTR = 0.75
    // context_features is empty, matching what current producers actually emit.
    val df = Seq(
      ("ios", Map.empty[String, String], 1, 0, "u1"),
      ("ios", Map.empty[String, String], 0, 0, "u1"),
      ("web", Map.empty[String, String], 1, 1, "u2"),
      ("web", Map.empty[String, String], 1, 0, "u2")
    ).toDF("device", "context_features", "clicked", "ordered", "user_id")

    val rows = SegmentReportJob.deviceMetrics(df, overallCtr = 0.75)
      .collect().map(r => r.getAs[String]("device") -> r).toMap

    rows("ios").getAs[Long]("impressions") shouldBe 2L
    rows("ios").getAs[Double]("ctr") shouldBe 0.5
    rows("ios").getAs[Long]("users") shouldBe 1L
    rows("web").getAs[Double]("ctr") shouldBe 1.0
    rows("web").getAs[Double]("order_rate") shouldBe 0.5
  }

  it should "prefer the typed device column over a stale legacy map key" in {
    val s = spark; import s.implicits._
    // Both sources present and disagreeing: a slate written during the migration can carry a
    // typed device AND a leftover context_features["platform"]. The typed field is the current
    // contract, so it must win. Neither single-source test can catch a reversed coalesce.
    val df = Seq(
      ("ios", Map("platform" -> "web"), 1, 0, "u1"),
      ("ios", Map("platform" -> "web"), 0, 0, "u1")
    ).toDF("device", "context_features", "clicked", "ordered", "user_id")

    val rows = SegmentReportJob.deviceMetrics(df, overallCtr = 0.5)
      .collect().map(r => r.getAs[String]("device") -> r).toMap

    rows.keySet shouldBe Set("ios")
    rows("ios").getAs[Long]("impressions") shouldBe 2L
  }

  it should "fall back to the legacy context_features[\"platform\"] map key when there is no typed device column" in {
    val s = spark; import s.implicits._
    // Pre-migration Parquet: no `device` column at all, platform only in context_features.
    val df = Seq(
      (Map("platform" -> "ios"), 1, 0, "u1"),
      (Map("platform" -> "ios"), 0, 0, "u1"),
      (Map("platform" -> "web"), 1, 1, "u2"),
      (Map("platform" -> "web"), 1, 0, "u2")
    ).toDF("context_features", "clicked", "ordered", "user_id")

    val rows = SegmentReportJob.deviceMetrics(df, overallCtr = 0.75)
      .collect().map(r => r.getAs[String]("device") -> r).toMap

    rows("ios").getAs[Long]("impressions") shouldBe 2L
    rows("ios").getAs[Double]("ctr") shouldBe 0.5
    rows("ios").getAs[Long]("users") shouldBe 1L
    rows("web").getAs[Double]("ctr") shouldBe 1.0
    rows("web").getAs[Double]("order_rate") shouldBe 0.5
  }

  "demographicMetrics" should "aggregate per dimension with a count-weighted avg_rating" in {
    val s = spark; import s.implicits._
    // both users in age_band 25-34; weighted avg = (4.0*10 + 5.0*30)/(10+30) = 4.75
    val joined = Seq(
      ("u1", 10L, 3L, 1L, "25-34", 4.0, 10L),
      ("u2", 2L, 0L, 0L, "25-34", 5.0, 30L)
    ).toDF("user_id", "impressions", "clicks", "orders", "age_band",
      "user_avg_rating", "user_rating_count")

    val out = SegmentReportJob.demographicMetrics(joined, "age_band", overallCtr = 0.25)
    out.columns should contain allOf ("impressions", "users", "ctr", "order_rate",
      "clicks_per_user", "ctr_lift_pct", "avg_rating")
    out.columns should not contain "_rsum"

    val r = out.collect().head
    r.getAs[Long]("impressions") shouldBe 12L
    r.getAs[Long]("users") shouldBe 2L
    r.getAs[Double]("ctr") shouldBe 0.25
    r.getAs[Double]("clicks_per_user") shouldBe 1.5
    r.getAs[Double]("avg_rating") shouldBe 4.75
  }

  "demographicsRowOrNone" should "derive age_band/geo and parse rating fields from a representative hash" in {
    val h: java.util.Map[String, String] = Map(
      "age" -> "30", "gender" -> "F", "occupation" -> "student",
      "zipCode" -> "90210", "avgRating" -> "4.5", "ratingCount" -> "12"
    ).asJava

    // demographicsRowOrNone returns a plain, schema-less Row (it feeds an RDD that gets its
    // schema from createDataFrame later), so fields are read positionally here — column order
    // matches DemographicsSchema: user_id, gender, occupation, age_band, geo, user_avg_rating,
    // user_rating_count.
    val row = SegmentReportJob.demographicsRowOrNone("u1", h)
    row shouldBe defined
    row.get.getString(0) shouldBe "u1"
    row.get.getString(1) shouldBe "F"
    row.get.getString(2) shouldBe "student"
    row.get.getString(3) shouldBe "25-34"
    row.get.getString(4) shouldBe "West"
    row.get.getDouble(5) shouldBe 4.5
    row.get.getLong(6) shouldBe 12L
  }

  it should "null out gender and default age_band/rating fields when fields are absent or unparseable" in {
    // gender absent, age absent (-> age_band "unknown"), avgRating unparseable (-> 0.0)
    val h: java.util.Map[String, String] = Map(
      "zipCode" -> "02139", "avgRating" -> "not-a-number"
    ).asJava

    val row = SegmentReportJob.demographicsRowOrNone("u2", h).get
    Option(row.getString(1)) shouldBe None
    row.getString(3) shouldBe "unknown"
    row.getString(4) shouldBe "Northeast"
    row.getDouble(5) shouldBe 0.0
    row.getLong(6) shouldBe 0L
  }

  it should "return None for a null hash (missing key)" in {
    SegmentReportJob.demographicsRowOrNone("missing", null) shouldBe None
  }

  it should "return None for an empty hash" in {
    SegmentReportJob.demographicsRowOrNone("missing", new java.util.HashMap[String, String]()) shouldBe None
  }

  "fetchDemographicsDf" should "return an empty, correctly-shaped DataFrame without contacting Redis when there are no ids" in {
    val s = spark; import s.implicits._
    val ids = Seq.empty[String].toDF("user_id")

    // Host is unreachable on purpose: an empty ids DataFrame must never open a Jedis connection.
    val result = SegmentReportJob.fetchDemographicsDf(ids, "unreachable-host.invalid", 1, 1)

    result.columns.toSeq shouldBe Seq(
      "user_id", "gender", "occupation", "age_band", "geo", "user_avg_rating", "user_rating_count")
    result.isEmpty shouldBe true
  }
}
