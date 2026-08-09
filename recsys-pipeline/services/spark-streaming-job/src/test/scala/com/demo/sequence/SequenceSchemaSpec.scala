package com.demo.sequence

import com.demo.SparkTestSupport
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceSchemaSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  // 2026-07-23T00:00:00.000Z and 2026-07-23T23:59:59.999Z
  private val dayStart = 1784764800000L
  private val dayEnd   = 1784851199999L

  "bucket" should "map an entire UTC day to one stamp" in {
    SequenceSchema.bucket(dayStart) shouldBe "20260723"
    SequenceSchema.bucket(dayEnd) shouldBe "20260723"
  }

  it should "put the next millisecond in the next bucket" in {
    SequenceSchema.bucket(dayEnd + 1L) shouldBe "20260724"
  }

  "key" should "format the partition key" in {
    SequenceSchema.key("u1", SequenceSchema.KindRating, "20260723") shouldBe "seq:u1:rating:20260723"
  }

  "bucketColumn" should "agree with the scalar bucket function" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val timestamps = Seq(dayStart, dayEnd, dayEnd + 1L, 0L)
    val computed = timestamps.toDF("ts")
      .select(SequenceSchema.bucketColumn(col("ts")).as("bucket"))
      .as[String]
      .collect()
      .toSeq

    computed shouldBe timestamps.map(SequenceSchema.bucket)
  }

  "Columns" should "match the shared cross-language fixture" in {
    val stream = getClass.getResourceAsStream("/sequence-schema.json")
    val source = scala.io.Source.fromInputStream(stream)
    val fixture = try source.mkString finally source.close()
    // Deliberately a substring assertion, not a JSON parse: the fixture exists to
    // detect drift, and adding a JSON library to this module for one test is not worth it.
    SequenceSchema.Columns.foreach(c => fixture should include(s""""$c""""))
    fixture should include(s""""rowSeparator": "${SequenceSchema.RowSeparator}"""")
    fixture should include(s""""valueSeparator": "${SequenceSchema.ValueSeparator}"""")
    fixture should include(s""""keyPrefix": "seq"""")
    fixture should include(s""""countField": "${SequenceSchema.ColCount}"""")
    fixture should include(
      s""""kinds": ["${SequenceSchema.KindRating}", "${SequenceSchema.KindClick}"]"""
    )
  }
}
