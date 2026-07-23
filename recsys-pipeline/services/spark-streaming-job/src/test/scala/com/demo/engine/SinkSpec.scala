package com.demo.engine

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("SinkSpec")
      .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()

  "KafkaSink.payload" should "produce key/value with the value carrying all columns as JSON" in {
    val s = spark; import s.implicits._
    val df = Seq(("k1", "v1")).toDF("id", "other")
    val out = new KafkaSink("localhost:9092", "t", "id").payload(df)
    out.columns should contain allOf ("key", "value")
    val row = out.first()
    row.getAs[String]("key") shouldBe "k1"
    val json = row.getAs[String]("value")
    json should include ("\"id\":\"k1\"")
    json should include ("\"other\":\"v1\"")
  }

  "ParquetSink" should "write at most outputFiles parquet files per partition" in {
    import java.nio.file.Files
    val s = spark; import s.implicits._
    val dir = Files.createTempDirectory("parquet-sink").toFile
    val out = new java.io.File(dir, "samples").getAbsolutePath

    val batch = Seq(
      ("s1", java.sql.Timestamp.valueOf("2026-06-26 00:00:00")),
      ("s2", java.sql.Timestamp.valueOf("2026-06-26 00:00:00")),
      ("s3", java.sql.Timestamp.valueOf("2026-06-26 00:00:00"))
    ).toDF("sample_id", "impression_time")

    val sink = new ParquetSink(out, "date", outputFiles = 1,
      transform = df => df.withColumn("date", to_date(col("impression_time"))))
    sink.write(batch, 0L)

    val partDir = new java.io.File(out, "date=2026-06-26")
    partDir.listFiles().count(_.getName.endsWith(".parquet")) shouldBe 1
  }
}
