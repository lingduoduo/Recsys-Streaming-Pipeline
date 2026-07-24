package com.demo.sequence

import com.demo.SparkTestSupport
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceEncoderSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val dayStart = 1784764800000L // 2026-07-23T00:00:00Z

  private def events(rows: Seq[(String, String, String, Long, String, java.lang.Double, Seq[String], java.lang.Integer)]) = {
    val sparkSession = spark
    import sparkSession.implicits._
    rows.toDF("user_id", "kind", "item_id", "ts", "action", "rating", "genres", "release_year")
  }

  "toColumnChunks" should "group by user, kind and bucket with ts-ascending columns" in {
    val df = events(Seq(
      ("u1", "rating", "m2", dayStart + 2000L, "rate", 5.0: java.lang.Double, Seq("Action"), 1999: java.lang.Integer),
      ("u1", "rating", "m1", dayStart + 1000L, "rate", 4.0: java.lang.Double, Seq("Drama", "Comedy"), 1995: java.lang.Integer)
    ))

    val chunk = SequenceEncoder.toColumnChunks(df, "day").collect().head

    chunk.getAs[String]("user_id") shouldBe "u1"
    chunk.getAs[String]("kind") shouldBe "rating"
    chunk.getAs[String]("bucket") shouldBe "20260723"
    chunk.getAs[String]("item_id") shouldBe "m1,m2"
    chunk.getAs[String]("ts") shouldBe s"${dayStart + 1000L},${dayStart + 2000L}"
    chunk.getAs[String]("action") shouldBe "rate,rate"
    chunk.getAs[String]("genres") shouldBe "Drama|Comedy,Action"
    chunk.getAs[String]("release_year") shouldBe "1995,1999"
    chunk.getAs[Long]("n") shouldBe 2L
  }

  it should "split a user across buckets on the UTC day boundary" in {
    val df = events(Seq(
      ("u1", "rating", "m1", dayStart, "rate", 4.0: java.lang.Double, Seq("Drama"), 1995: java.lang.Integer),
      ("u1", "rating", "m2", dayStart + 86400000L, "rate", 4.0: java.lang.Double, Seq("Drama"), 1995: java.lang.Integer)
    ))

    val buckets = SequenceEncoder.toColumnChunks(df, "day")
      .collect().map(_.getAs[String]("bucket")).toSet

    buckets shouldBe Set("20260723", "20260724")
  }

  it should "encode null rating and release_year as empty elements" in {
    val df = events(Seq(
      ("u1", "click", "m1", dayStart + 1000L, "click", null.asInstanceOf[java.lang.Double], Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("u1", "click", "m2", dayStart + 2000L, "click", 3.0: java.lang.Double, Seq.empty[String], 2001: java.lang.Integer)
    ))

    val chunk = SequenceEncoder.toColumnChunks(df, "day").collect().head

    chunk.getAs[String]("rating") shouldBe ",3.0"
    chunk.getAs[String]("release_year") shouldBe ",2001"
    chunk.getAs[String]("genres") shouldBe ","
    chunk.getAs[Long]("n") shouldBe 2L
  }

  it should "strip separators out of genre values so they cannot break alignment" in {
    val df = events(Seq(
      ("u1", "rating", "m1", dayStart + 1000L, "rate", 4.0: java.lang.Double, Seq("Sci-Fi, Fantasy", "Drama"), 1995: java.lang.Integer),
      ("u1", "rating", "m2", dayStart + 2000L, "rate", 4.0: java.lang.Double, Seq("Action"), 1996: java.lang.Integer)
    ))

    val chunk = SequenceEncoder.toColumnChunks(df, "day").collect().head

    chunk.getAs[String]("genres") shouldBe "Sci-Fi Fantasy|Drama,Action"
    chunk.getAs[String]("item_id") shouldBe "m1,m2"
  }

  it should "produce columns whose element count always equals n" in {
    val df = events(Seq(
      ("u1", "rating", "m1", dayStart + 1000L, "rate", null.asInstanceOf[java.lang.Double], Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("u1", "rating", "m2", dayStart + 2000L, "rate", null.asInstanceOf[java.lang.Double], Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("u1", "rating", "m3", dayStart + 3000L, "rate", null.asInstanceOf[java.lang.Double], Seq.empty[String], null.asInstanceOf[java.lang.Integer])
    ))

    val chunk: Row = SequenceEncoder.toColumnChunks(df, "day").collect().head
    val n = chunk.getAs[Long]("n").toInt

    SequenceSchema.Columns.foreach { column =>
      withClue(s"column $column: ") {
        SequenceCodec.unpack(chunk.getAs[String](column), n).length shouldBe n
        chunk.getAs[String](column).split(",", -1).length shouldBe n
      }
    }
  }
}
