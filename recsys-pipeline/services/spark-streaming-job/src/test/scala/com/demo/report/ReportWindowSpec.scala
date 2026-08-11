package com.demo.report

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReportWindowSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "withinLookback" should "keep only the most recent N partition dates" in {
    val s = spark; import s.implicits._
    val df = Seq(
      ("item_1", "2024-06-01"),
      ("item_2", "2024-06-07"),
      ("item_3", "2024-06-08"),
      ("item_4", "2024-06-10")
    ).toDF("item_id", "date")

    val kept = ReportWindow.withinLookback(df, 3)
      .collect().map(_.getAs[String]("item_id")).toSet

    // Anchored to the newest date present (2024-06-10), not the wall clock, so a report over
    // historical data is deterministic.
    kept shouldBe Set("item_3", "item_4")
  }

  it should "read every partition when the lookback is not positive" in {
    val s = spark; import s.implicits._
    val df = Seq(("item_1", "2024-06-01"), ("item_2", "2024-06-10")).toDF("item_id", "date")

    ReportWindow.withinLookback(df, 0).count() shouldBe 2L
    ReportWindow.withinLookback(df, -1).count() shouldBe 2L
  }

  it should "pass input through unchanged when it has no date column" in {
    val s = spark; import s.implicits._
    val df = Seq(("item_1", 1, 0)).toDF("item_id", "clicked", "ordered")

    ReportWindow.withinLookback(df, 7).count() shouldBe 1L
  }
}
