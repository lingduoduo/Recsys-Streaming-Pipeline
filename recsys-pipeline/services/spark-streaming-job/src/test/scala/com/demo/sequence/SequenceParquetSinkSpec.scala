package com.demo.sequence

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceParquetSinkSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def chunks = {
    val sparkSession = spark
    import sparkSession.implicits._
    Seq(
      ("u1", "rating", "20260723", "m1,m2", "1000,2000", "rate,rate", ",4.0", "Drama|Comedy,", "1995,", 2L)
    ).toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")
  }

  "explodeChunks" should "produce one row per event with columns unpacked and aligned" in {
    val rows = SequenceParquetSink.explodeChunks(chunks)
      .orderBy("ts")
      .collect()

    rows.length shouldBe 2

    rows(0).getAs[String]("item_id") shouldBe "m1"
    rows(0).getAs[Long]("ts") shouldBe 1000L
    rows(0).getAs[String]("rating") shouldBe ""
    rows(0).getAs[String]("genres") shouldBe "Drama|Comedy"
    rows(0).getAs[String]("release_year") shouldBe "1995"

    rows(1).getAs[String]("item_id") shouldBe "m2"
    rows(1).getAs[Long]("ts") shouldBe 2000L
    rows(1).getAs[String]("rating") shouldBe "4.0"
    rows(1).getAs[String]("genres") shouldBe ""
    rows(1).getAs[String]("release_year") shouldBe ""
  }

  it should "carry the partition columns onto every exploded row" in {
    val row = SequenceParquetSink.explodeChunks(chunks).collect().head
    row.getAs[String]("user_id") shouldBe "u1"
    row.getAs[String]("kind") shouldBe "rating"
    row.getAs[String]("bucket") shouldBe "20260723"
  }

  it should "emit nothing for a zero-row chunk" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val empty = Seq(("u1", "rating", "20260723", "", "", "", "", "", "", 0L))
      .toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")

    SequenceParquetSink.explodeChunks(empty).count() shouldBe 0L
  }

  "write" should "partition the output by bucket and kind" in {
    val path = java.nio.file.Files.createTempDirectory("seq-parquet").toString + "/out"
    new SequenceParquetSink(path, SequenceWriteMode.Overwrite).write(chunks, 0L)

    val readBack = spark.read.parquet(path)
    readBack.count() shouldBe 2L
    readBack.columns should contain allOf ("bucket", "kind")
    new java.io.File(path).list().toSeq.filter(_.startsWith("bucket=")) shouldBe Seq("bucket=20260723")
  }
}
