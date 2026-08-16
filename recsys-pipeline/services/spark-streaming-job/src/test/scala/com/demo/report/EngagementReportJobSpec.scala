package com.demo.report

import java.sql.Timestamp

import com.demo.SparkTestSupport
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngagementReportJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  /** Local wall-clock fixture for the local-time breakdowns only: two impressions Mon
    * 2026-06-01 09:00 (1 click), two Sat 2026-06-06 21:00 (2 clicks). `byHour` and `byDow` are
    * deliberately session-local, so a wall-clock fixture is the right shape for them. `daily` is
    * UTC-bucketed and uses the instant-based fixtures below. */
  private def fixture = {
    val s = spark; import s.implicits._
    Seq(
      (Timestamp.valueOf("2026-06-01 09:00:00"), 1),
      (Timestamp.valueOf("2026-06-01 09:00:00"), 0),
      (Timestamp.valueOf("2026-06-06 21:00:00"), 1),
      (Timestamp.valueOf("2026-06-06 21:00:00"), 1)
    ).toDF("impression_time", "clicked")
  }

  /** Two impressions at 2026-06-01T23:30:00Z, one clicked. Built from the instant rather than a
    * local wall-clock string, so the fixture itself does not shift with the session time zone. */
  private def utcBoundaryFixture = {
    val s = spark; import s.implicits._
    Seq((1780356600L, 1), (1780356600L, 0)).toDF("impression_ts", "clicked")
      .withColumn("impression_time", col("impression_ts").cast("timestamp"))
  }

  /** Two impressions at 2026-06-01T12:00:00Z (1 click) and two at 2026-06-06T12:00:00Z (2 clicks).
    * Midday UTC, so the day is unambiguous in every plausible session time zone. */
  private def twoUtcDaysFixture = {
    val s = spark; import s.implicits._
    Seq((1780315200L, 1), (1780315200L, 0), (1780747200L, 1), (1780747200L, 1))
      .toDF("impression_ts", "clicked")
      .withColumn("impression_time", col("impression_ts").cast("timestamp"))
  }

  "daily" should "bucket by UTC date in every session time zone" in {
    val s = spark
    val days = Seq("UTC", "America/New_York", "Asia/Tokyo").map { zone =>
      val previous = s.conf.get("spark.sql.session.timeZone")
      s.conf.set("spark.sql.session.timeZone", zone)
      try EngagementReportJob.daily(utcBoundaryFixture)
        .collect().head.getAs[java.sql.Date]("day").toString
      finally s.conf.set("spark.sql.session.timeZone", previous)
    }
    days.distinct shouldBe Seq("2026-06-01")
  }

  it should "compute CTR and impression count per day" in {
    val rows = EngagementReportJob.daily(twoUtcDaysFixture)
      .collect().map(r => r.getAs[java.sql.Date]("day").toString -> r).toMap

    rows("2026-06-01").getAs[Double]("ctr") shouldBe 0.5
    rows("2026-06-01").getAs[Long]("impressions") shouldBe 2L
    rows("2026-06-06").getAs[Double]("ctr") shouldBe 1.0
    rows("2026-06-06").getAs[Long]("impressions") shouldBe 2L
  }

  "byHour" should "compute CTR per hour-of-day" in {
    val rows = EngagementReportJob.byHour(fixture)
      .collect().map(r => r.getAs[Int]("hour") -> r.getAs[Double]("ctr")).toMap
    rows(9) shouldBe 0.5
    rows(21) shouldBe 1.0
  }

  "byDow" should "compute CTR per day-of-week with Spark's 1=Sun..7=Sat numbering" in {
    val rows = EngagementReportJob.byDow(fixture)
      .collect().map(r => r.getAs[String]("dow") -> r).toMap

    rows("Mon").getAs[Int]("dow_num") shouldBe 2   // Monday
    rows("Mon").getAs[Double]("ctr") shouldBe 0.5
    rows("Sat").getAs[Int]("dow_num") shouldBe 7   // Saturday
    rows("Sat").getAs[Double]("ctr") shouldBe 1.0
  }
}
