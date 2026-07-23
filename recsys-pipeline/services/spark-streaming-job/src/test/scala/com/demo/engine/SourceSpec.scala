package com.demo.engine

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SourceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("SourceSpec")
      .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()

  private def cfg = EngineConfig("localhost:9092", "in", "earliest", "g", 5000,
    "10 seconds", "/tmp/ck", "10 minutes", 0)

  "KafkaSource" should "build a streaming DataFrame carrying the kafka value column" in {
    val df = KafkaSource.read(spark, cfg)
    df.isStreaming shouldBe true
    df.columns should contain ("value")
  }
}
