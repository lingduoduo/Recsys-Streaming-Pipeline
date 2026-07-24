package com.demo.sequence

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

class SequenceBackfillJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def ratingsCsv(): String = {
    val dir = Files.createTempDirectory("seq-backfill")
    val file = dir.resolve("ratings.csv")
    Files.write(file, java.util.Arrays.asList(
      "userId,movieId,rating,timestamp",
      "u1,m1,4.0,1784764801",
      "u1,m2,5.0,1784764802",
      "u2,m1,3.0,1784851201"
    ))
    file.toString
  }

  "readRatings" should "project the ratings CSV into sequence-store event shape" in {
    val rows = SequenceBackfillJob.readRatings(spark, ratingsCsv()).orderBy("user_id", "ts").collect()

    rows.length shouldBe 3
    rows.head.getAs[String]("user_id") shouldBe "u1"
    rows.head.getAs[String]("kind") shouldBe "rating"
    rows.head.getAs[String]("item_id") shouldBe "m1"
    rows.head.getAs[Long]("ts") shouldBe 1784764801000L
    rows.head.getAs[String]("action") shouldBe "rate"
    rows.head.getAs[Double]("rating") shouldBe 4.0
  }

  it should "chunk into one partition per user and day" in {
    val chunks = SequenceEncoder
      .toColumnChunks(SequenceBackfillJob.readRatings(spark, ratingsCsv()))
      .orderBy("user_id", "bucket")
      .collect()

    chunks.length shouldBe 2
    chunks(0).getAs[String]("user_id") shouldBe "u1"
    chunks(0).getAs[String]("bucket") shouldBe "20260723"
    chunks(0).getAs[String]("item_id") shouldBe "m1,m2"
    chunks(1).getAs[String]("user_id") shouldBe "u2"
    chunks(1).getAs[String]("bucket") shouldBe "20260724"
  }

  it should "be idempotent under Overwrite: a second run reproduces the same Parquet" in {
    val events = SequenceBackfillJob.readRatings(spark, ratingsCsv())
    val chunks = SequenceEncoder.toColumnChunks(events)
    val path = Files.createTempDirectory("seq-backfill-out").toString + "/out"

    new SequenceParquetSink(path, SequenceWriteMode.Overwrite).write(chunks, 0L)
    val first = spark.read.parquet(path).count()
    new SequenceParquetSink(path, SequenceWriteMode.Overwrite).write(chunks, 1L)
    val second = spark.read.parquet(path).count()

    first shouldBe 3L
    second shouldBe 3L   // not 6 — Overwrite replaces rather than appending
  }
}
