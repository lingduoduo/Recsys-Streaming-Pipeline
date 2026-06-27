package com.demo.process

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RecallSampleStreamingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("RecallSampleStreamingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "buildRecallSamples" should "emit per-impression rows; click_movie_id set only when clicked" in {
    val s = spark; import s.implicits._

    val samples = Seq(
      ("u1", "sess_1", "item_1", 1, 0, 1.0, 100L),  // recommended + clicked
      ("u1", "sess_1", "item_2", 0, 0, 0.0, 100L),  // recommended, not clicked
      ("u2", "sess_2", "item_3", 1, 1, 2.0, 200L)   // clicked + ordered
    ).toDF("user_id", "session_id", "item_id", "clicked", "ordered", "label", "impression_ts")

    val rows = RecallSampleStreamingJob.buildRecallSamples(samples).collect()
      .map(r => r.getAs[String]("recommended_movie_id") ->
        (r.getAs[String]("user_id"), Option(r.getAs[String]("click_movie_id")), r.getAs[Double]("rating")))
      .toMap

    rows("item_1") shouldBe ("u1", Some("item_1"), 1.0)
    rows("item_2") shouldBe ("u1", None, 0.0)              // not clicked -> null click_movie_id
    rows("item_3") shouldBe ("u2", Some("item_3"), 2.0)    // order -> rating 2.0
  }

  it should "expose the recall schema columns incl. session_id and event_ts" in {
    val s = spark; import s.implicits._
    val samples = Seq(
      ("u1", "sess_1", "item_1", 1, 0, 1.0, 100L)
    ).toDF("user_id", "session_id", "item_id", "clicked", "ordered", "label", "impression_ts")

    RecallSampleStreamingJob.buildRecallSamples(samples).columns should contain allOf
      ("user_id", "session_id", "event_ts", "recommended_movie_id", "click_movie_id", "rating")
  }
}
