package com.demo.report

import java.sql.Timestamp

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngagementReportJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  /** Two impressions Mon 2026-06-01 09:00 (1 click), two Sat 2026-06-06 21:00 (2 clicks). */
  private def fixture = {
    val s = spark; import s.implicits._
    Seq(
      (Timestamp.valueOf("2026-06-01 09:00:00"), 1),
      (Timestamp.valueOf("2026-06-01 09:00:00"), 0),
      (Timestamp.valueOf("2026-06-06 21:00:00"), 1),
      (Timestamp.valueOf("2026-06-06 21:00:00"), 1)
    ).toDF("impression_time", "clicked")
  }

  "daily" should "compute CTR and impression count per day" in {
    val rows = EngagementReportJob.daily(fixture)
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
