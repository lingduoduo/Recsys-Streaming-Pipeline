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
}
