package com.demo.util

import com.demo.SparkTestSupport
import com.demo.sequence.SequenceSchema
import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TimePartitionsSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  /** 2026-06-01T23:30:00Z. Late enough in the UTC day that Tokyo (+9) is already on 06-02 and
    * New York (-4) is still on 06-01, so a session-time-zone implementation cannot agree with
    * itself across the three. */
  private val LateInTheUtcDay = 1780356600L

  private def dateIn(zone: String, epochSeconds: Long): String = {
    val previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", zone)
    try
      spark.range(1)
        .select(TimePartitions.utcDate(lit(epochSeconds)).as("d"))
        .collect().head.getAs[java.sql.Date]("d").toString
    finally spark.conf.set("spark.sql.session.timeZone", previous)
  }

  "utcDate" should "return the same date in every session time zone" in {
    val zones = Seq("UTC", "America/New_York", "Asia/Tokyo", "Pacific/Kiritimati")
    zones.map(dateIn(_, LateInTheUtcDay)).distinct shouldBe Seq("2026-06-01")
  }

  it should "agree with the sequence store's UTC bucket for the same instant" in {
    dateIn("America/New_York", LateInTheUtcDay).replace("-", "") shouldBe
      SequenceSchema.bucket(LateInTheUtcDay * 1000L)
  }

  it should "floor toward the past before the epoch" in {
    dateIn("UTC", -1L) shouldBe "1969-12-31"
  }
}
