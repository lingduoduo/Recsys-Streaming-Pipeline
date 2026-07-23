package com.demo.engine

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StageSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("StageSpec")
      .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()

  "a Stage lambda" should "transform a DataFrame" in {
    val s = spark; import s.implicits._
    val stage: Stage = (df) => df.filter(col("x") > 1)
    val out = stage(Seq(1, 2, 3).toDF("x")).collect().map(_.getInt(0)).sorted
    out shouldBe Array(2, 3)
  }

  "a BatchStage lambda" should "receive the batchId" in {
    val s = spark; import s.implicits._
    val stage: BatchStage = (df, id) => df.withColumn("bid", lit(id))
    val out = stage(Seq("a").toDF("x"), 7L).select("bid").first().getLong(0)
    out shouldBe 7L
  }
}
