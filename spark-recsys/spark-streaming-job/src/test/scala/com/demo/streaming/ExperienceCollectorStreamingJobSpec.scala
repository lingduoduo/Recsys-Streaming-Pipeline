package com.demo.streaming

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExperienceCollectorStreamingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("ExperienceCollectorStreamingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "buildSlates" should "collect item samples into a position-sorted slate" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq(
      ("s2", "req_1", "user_1", "item_2", 1, 100L, 0, 0, 0.0, Map("tier" -> "gold"), Map("genre" -> "comedy"), Map("device" -> "ios")),
      ("s1", "req_1", "user_1", "item_1", 0, 100L, 1, 0, 1.0, Map("tier" -> "gold"), Map("genre" -> "drama"), Map("device" -> "ios"))
    ).toDF("sample_id", "request_id", "user_id", "item_id", "position", "impression_ts", "clicked", "ordered", "label", "user_features", "item_features", "context_features")

    val row = ExperienceCollectorStreamingJob.buildSlates(samples).first()
    val items = row.getAs[Seq[org.apache.spark.sql.Row]]("items")

    row.getAs[String]("slate_id") shouldBe "req_1:user_1"
    row.getAs[Int]("slate_clicked") shouldBe 1
    row.getAs[Double]("slate_reward") shouldBe 1.0
    row.getAs[Int]("slate_size") shouldBe 2
    items.map(_.getAs[String]("item_id")) shouldBe Seq("item_1", "item_2")
  }
}
