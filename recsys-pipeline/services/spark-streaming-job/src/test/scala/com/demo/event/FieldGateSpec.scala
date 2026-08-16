package com.demo.event

import com.demo.SparkTestSupport
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FieldGateSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def rules = Seq(
    "null_user_id" -> col("user_id").isNull,
    "null_item_id" -> col("item_id").isNull,
    "null_timestamp" -> col("timestamp").isNull
  )

  private def frame = {
    val s = spark; import s.implicits._
    Seq(
      (Some("u1"), Some("i1"), Some(100L)), // passes
      (Some("u2"), Some("i2"), Some(200L)), // passes
      (None, Some("i3"), Some(300L)), // null_user_id
      (None, None, None), // all three -> first match only
      (Some("u5"), None, Some(500L)), // null_item_id
      (Some("u6"), Some("i6"), None) // null_timestamp
    ).toDF("user_id", "item_id", "timestamp")
  }

  "FieldGate" should "attribute a row to the first rule it violates" in {
    val rejected = FieldGate(frame, rules).rejected
      .select("user_id", FieldGate.ReasonColumn)
      .collect().map(r => (if (r.isNullAt(0)) None else Some(r.getString(0))) -> r.getString(1))

    // The all-null row violates every rule but is counted once, under the first.
    rejected.count(_._2 == "null_user_id") shouldBe 2
    rejected.count(_._2 == "null_item_id") shouldBe 1
    rejected.count(_._2 == "null_timestamp") shouldBe 1
  }

  it should "split kept from rejected and drop the reason column from kept" in {
    val gated = FieldGate(frame, rules)

    gated.kept.count() shouldBe 2
    gated.kept.columns should not contain FieldGate.ReasonColumn
    gated.rejected.count() shouldBe 4
    gated.rejected.columns should contain(FieldGate.ReasonColumn)
  }

  it should "report counts that partition the input, with declared zeros in order" in {
    val (kept, reasons) = FieldGate(frame, rules).counts

    kept shouldBe 2L
    reasons shouldBe Seq("null_user_id" -> 2L, "null_item_id" -> 1L, "null_timestamp" -> 1L)
    kept + reasons.map(_._2).sum shouldBe frame.count()
  }

  it should "keep a declared reason at zero rather than omitting it" in {
    val s = spark; import s.implicits._
    val allGood = Seq((Some("u1"), Some("i1"), Some(100L))).toDF("user_id", "item_id", "timestamp")

    val (kept, reasons) = FieldGate(allGood, rules).counts

    kept shouldBe 1L
    reasons shouldBe Seq("null_user_id" -> 0L, "null_item_id" -> 0L, "null_timestamp" -> 0L)
  }

  it should "not reject a row when a predicate evaluates to null" in {
    val s = spark; import s.implicits._
    val rows = Seq((Some("u1"), None: Option[String])).toDF("user_id", "maybe")

    // `maybe = "x"` is NULL, not false, when `maybe` is null. Without the coalesce guard the
    // unknown would fall through the chain unpredictably rather than meaning "keep".
    val (kept, reasons) = FieldGate(rows, Seq("unknown_predicate" -> (col("maybe") === lit("x")))).counts

    kept shouldBe 1L
    reasons shouldBe Seq("unknown_predicate" -> 0L)
  }

  it should "handle an empty input" in {
    val (kept, reasons) = FieldGate(frame.limit(0), rules).counts

    kept shouldBe 0L
    reasons.map(_._2).sum shouldBe 0L
    reasons.map(_._1) shouldBe Seq("null_user_id", "null_item_id", "null_timestamp")
  }

  it should "reject an empty rule list" in {
    an[IllegalArgumentException] should be thrownBy FieldGate(frame, Seq.empty)
  }
}
