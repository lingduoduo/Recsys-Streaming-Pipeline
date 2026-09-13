package com.demo.util

import java.nio.file.Files

import com.demo.SparkTestSupport
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmbeddingTextSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def tmpDir(name: String): String = {
    val dir = Files.createTempDirectory(name)
    dir.toFile.deleteOnExit()
    dir.resolve("out").toString
  }

  "writeText then read" should "round-trip ids and vectors" in {
    val s = spark; import s.implicits._
    val path = tmpDir("emb-roundtrip")
    val df = Seq(("a", Seq(1.0, -0.5)), ("b", Seq(0.25, 2.0))).toDF("id", "vector")

    EmbeddingText.writeText(df, "id", "vector", path)
    val back = EmbeddingText.read(spark, path).collect()
      .map(row => row.getAs[String]("id") -> row.getAs[Seq[Double]]("vector")).toMap

    back shouldBe Map("a" -> Seq(1.0, -0.5), "b" -> Seq(0.25, 2.0))
  }

  "read" should "skip lines without a colon, with an empty id, or with a non-numeric token" in {
    val path = tmpDir("emb-malformed")
    val file = new java.io.File(path)
    Files.write(file.toPath, java.util.Arrays.asList(
      "ok:1.0 2.0", "nocolon 1.0", ":1.0", "bad:1.0 x", "also:3"))
    val ids = EmbeddingText.read(spark, path).collect().map(_.getAs[String]("id")).toSet
    ids shouldBe Set("ok", "also")
  }

  "vectorString" should "render a float array as space-separated values" in {
    val s = spark; import s.implicits._
    val rendered = Seq(Seq(1.5f, -2.0f)).toDF("v")
      .select(EmbeddingText.vectorString(col("v")).as("s")).collect().head.getString(0)
    rendered shouldBe "1.5 -2.0"
  }
}
