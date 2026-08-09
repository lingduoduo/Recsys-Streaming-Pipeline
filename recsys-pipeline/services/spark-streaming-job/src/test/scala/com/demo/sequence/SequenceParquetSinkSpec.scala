package com.demo.sequence

import com.demo.SparkTestSupport
import com.demo.engine.SinkWriteContext
import com.demo.engine.DurableParquetCommit
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
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

  it should "clamp a ragged chunk to the shortest column instead of emitting NULLs" in {
    val sparkSession = spark
    import sparkSession.implicits._
    // n=3, but "rating" only has 2 packed elements -- a torn write.
    val ragged = Seq(
      ("u1", "rating", "20260723", "m1,m2,m3", "1000,2000,3000", "rate,rate,rate", "3.0,4.0", "a,b,c", "1995,1996,1997", 3L)
    ).toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")

    val rows = SequenceParquetSink.explodeChunks(ragged)
      .orderBy("ts")
      .collect()

    rows.length shouldBe 2

    rows(0).getAs[String]("item_id") shouldBe "m1"
    rows(0).getAs[Long]("ts") shouldBe 1000L
    rows(0).getAs[String]("rating") shouldBe "3.0"

    rows(1).getAs[String]("item_id") shouldBe "m2"
    rows(1).getAs[Long]("ts") shouldBe 2000L
    rows(1).getAs[String]("rating") shouldBe "4.0"
  }

  it should "round-trip a single-event chunk (n = 1)" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val single = Seq(
      ("u1", "rating", "20260723", "m1", "1000", "rate", "4.0", "Drama", "1995", 1L)
    ).toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")

    val rows = SequenceParquetSink.explodeChunks(single).collect()

    rows.length shouldBe 1
    rows(0).getAs[String]("item_id") shouldBe "m1"
    rows(0).getAs[Long]("ts") shouldBe 1000L
    rows(0).getAs[String]("rating") shouldBe "4.0"
    rows(0).getAs[String]("genres") shouldBe "Drama"
    rows(0).getAs[String]("release_year") shouldBe "1995"
  }

  it should "round-trip a single-event chunk (n = 1) with null rating/genres/release_year (click event)" in {
    val sparkSession = spark
    import sparkSession.implicits._
    // A click-kind event structurally has null rating, genres, release_year, which
    // SequenceCodec.pack encodes as the empty string "" -- one element, not zero.
    val single = Seq(
      ("u1", "click", "20260723", "m1", "1000", "click", "", "", "", 1L)
    ).toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")

    val rows = SequenceParquetSink.explodeChunks(single).collect()

    rows.length shouldBe 1
    rows(0).getAs[String]("item_id") shouldBe "m1"
    rows(0).getAs[Long]("ts") shouldBe 1000L
  }

  it should "emit both rows for a chunk whose packed rating is two null values (n = 2)" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val twoNullRatings = Seq(
      ("u1", "rating", "20260723", "m1,m2", "1000,2000", "rate,rate", ",", "a,b", "1995,1996", 2L)
    ).toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")

    val rows = SequenceParquetSink.explodeChunks(twoNullRatings)
      .orderBy("ts")
      .collect()

    rows.length shouldBe 2

    rows(0).getAs[String]("item_id") shouldBe "m1"
    rows(0).getAs[Long]("ts") shouldBe 1000L
    rows(0).getAs[String]("rating") shouldBe ""

    rows(1).getAs[String]("item_id") shouldBe "m2"
    rows(1).getAs[Long]("ts") shouldBe 2000L
    rows(1).getAs[String]("rating") shouldBe ""
  }

  "write" should "partition the output by bucket and kind" in {
    val path = java.nio.file.Files.createTempDirectory("seq-parquet").toString + "/out"
    new SequenceParquetSink(path, SequenceWriteMode.Overwrite).write(chunks, 0L)

    val readBack = spark.read.parquet(path)
    readBack.count() shouldBe 2L
    readBack.columns should contain allOf ("bucket", "kind")
    new java.io.File(path).list().toSeq.filter(_.startsWith("bucket=")) shouldBe Seq("bucket=20260723")
  }

  it should "commit one deterministic directory when a durable batch is retried" in {
    val path = java.nio.file.Files.createTempDirectory("durable-seq-parquet").toString + "/out"
    val context = SinkWriteContext(
      "checkpoint://sequence", "query-ns", "sequence:user-events", "sink-ns", 4L)
    val sink = new SequenceParquetSink(path, SequenceWriteMode.Append)

    sink.writeDurably(chunks, context)
    sink.writeDurably(chunks, context)

    val committed = java.nio.file.Paths.get(sink.committedBatchPath(context))
    java.nio.file.Files.exists(committed.resolve("_SUCCESS")) shouldBe true
    java.nio.file.Files.exists(committed.resolve("_COMMITTED")) shouldBe true
    spark.read.parquet(committed.toString).count() shouldBe 2L
    val expectedSchema = StructType(Seq(
      StructField("user_id", StringType, nullable = true),
      StructField("kind", StringType, nullable = true),
      StructField("bucket", StringType, nullable = true),
      StructField("item_id", StringType, nullable = true),
      StructField("ts", LongType, nullable = true),
      StructField("action", StringType, nullable = true),
      StructField("rating", StringType, nullable = true),
      StructField("genres", StringType, nullable = true),
      StructField("release_year", StringType, nullable = true)))
    DurableParquetCommit.readIdentity(
      spark, path, context.queryNamespace, context.sinkNamespace, expectedSchema).count() shouldBe 2L
  }
}
