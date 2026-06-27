package com.demo.report

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngagementReportJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("EngagementReportJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  private def ts(s: String) = Timestamp.valueOf(s)

  // Two impressions Mon (1 click), two Sat (2 clicks) — mirrors the Python report test.
  private def sampleDf = {
    val s = spark; import s.implicits._
    Seq(
      (ts("2026-06-01 09:00:00"), 1),  // Mon
      (ts("2026-06-01 09:00:00"), 0),  // Mon
      (ts("2026-06-06 21:00:00"), 1),  // Sat
      (ts("2026-06-06 21:00:00"), 1)   // Sat
    ).toDF("impression_time", "clicked")
  }

  "daily" should "compute CTR = avg(clicked) per day" in {
    val rows = EngagementReportJob.daily(sampleDf).collect()
      .map(r => r.getDate(0).toString -> (r.getDouble(1), r.getLong(2))).toMap
    rows("2026-06-01") shouldBe (0.5, 2L)
    rows("2026-06-06") shouldBe (1.0, 2L)
  }

  "byHour" should "compute CTR per hour-of-day" in {
    val rows = EngagementReportJob.byHour(sampleDf).collect()
      .map(r => r.getInt(0) -> r.getDouble(1)).toMap
    rows(9) shouldBe 0.5
    rows(21) shouldBe 1.0
  }

  "byDayOfWeek" should "rank Sat above Mon and name the days" in {
    val rows = EngagementReportJob.byDayOfWeek(sampleDf).collect()
      .map(r => r.getString(1) -> r.getDouble(2)).toMap
    rows("Mon") shouldBe 0.5
    rows("Sat") shouldBe 1.0
    rows("Sat") should be > rows("Mon")
  }
}
