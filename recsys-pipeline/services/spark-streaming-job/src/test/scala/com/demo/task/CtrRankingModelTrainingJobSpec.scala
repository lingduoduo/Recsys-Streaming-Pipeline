package com.demo.task

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CtrRankingModelTrainingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("CtrRankingModelTrainingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "labelColumn" should "map label>0 to 1.0 in positive mode" in {
    val s = spark; import s.implicits._
    val df = Seq((1, 0.0), (1, 1.0), (0, 2.0)).toDF("clicked", "label")
    val out = CtrRankingModelTrainingJob.labelColumn(df, "positive")
      .select("ctr_label").collect().map(_.getDouble(0))
    out shouldBe Array(0.0, 1.0, 1.0)
  }

  it should "map clicked==1 to 1.0 in click mode" in {
    val s = spark; import s.implicits._
    val df = Seq((1, 0.0), (0, 2.0)).toDF("clicked", "label")
    val out = CtrRankingModelTrainingJob.labelColumn(df, "click")
      .select("ctr_label").collect().map(_.getDouble(0))
    out shouldBe Array(1.0, 0.0)
  }

  "assembleFeatures" should "produce a features vector of the expected size" in {
    val s = spark; import s.implicits._
    val df = Seq(
      ("item_1", 0,
        Map("tier" -> "gold"),
        Map("bucket" -> "b1"),
        Map("device" -> "ios", "country" -> "US"),
        Seq("drama"), Seq("classic"))
    ).toDF("item_id", "position", "user_features", "item_features",
           "context_features", "genres", "tags")

    val out = CtrRankingModelTrainingJob.assembleFeatures(df, numFeatures = 1024)
    out.columns should contain ("features")
    val v = out.select("features").first().getAs[Vector](0)
    v.size shouldBe (1024 + 2 * CtrRankingModelTrainingJob.HashTfSize)
  }

  "splitByDate" should "hold out the latest date" in {
    val s = spark; import s.implicits._
    val df = Seq(
      ("a", "2026-06-01"), ("b", "2026-06-02"), ("c", "2026-06-03")
    ).toDF("id", "date")

    val (train, valid) = CtrRankingModelTrainingJob.splitByDate(df, holdoutDays = 1)
    train.select("date").distinct().collect().map(_.getString(0)).sorted shouldBe
      Array("2026-06-01", "2026-06-02")
    valid.select("date").distinct().collect().map(_.getString(0)) shouldBe
      Array("2026-06-03")
  }
}
