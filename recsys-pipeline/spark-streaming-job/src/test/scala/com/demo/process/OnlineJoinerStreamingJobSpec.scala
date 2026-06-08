package com.demo.process

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OnlineJoinerStreamingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("OnlineJoinerStreamingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "buildTrainingSamples" should "join impressions with click and order labels" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("req_1", "user_1", "item_1", "impression", 100L, 0, Map("tier" -> "gold"), Map("genre" -> "drama"), Map("device" -> "ios")),
      ("req_1", "user_1", "item_2", "impression", 100L, 1, Map("tier" -> "gold"), Map("genre" -> "comedy"), Map("device" -> "ios")),
      ("req_1", "user_1", "item_1", "click", 105L, 0, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String]),
      ("req_1", "user_1", "item_2", "order", 120L, 1, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String])
    ).toDF("request_id", "user_id", "item_id", "event_type", "timestamp", "position", "user_features", "item_features", "context_features")

    val rows = OnlineJoinerStreamingJob.buildTrainingSamples(events)
      .select("item_id", "clicked", "ordered", "label")
      .collect()
      .map(row => row.getString(0) -> (row.getInt(1), row.getInt(2), row.getDouble(3)))
      .toMap

    rows("item_1") shouldBe (1, 0, 1.0)
    rows("item_2") shouldBe (0, 1, 2.0)
  }

  it should "keep unclicked impressions as negative samples" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("req_2", "user_1", "item_9", "exposure", 200L, 3, Map.empty[String, String], Map("genre" -> "news"), Map.empty[String, String])
    ).toDF("request_id", "user_id", "item_id", "event_type", "timestamp", "position", "user_features", "item_features", "context_features")

    val row = OnlineJoinerStreamingJob.buildTrainingSamples(events).first()

    row.getAs[Int]("clicked") shouldBe 0
    row.getAs[Int]("ordered") shouldBe 0
    row.getAs[Double]("label") shouldBe 0.0
  }
}
