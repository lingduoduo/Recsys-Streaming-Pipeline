package com.demo.process

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RelevanceSampleStreamingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("RelevanceSampleStreamingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "buildRelevanceSamples" should "build query, join title/genres/year, and score by label" in {
    val s = spark; import s.implicits._

    val samples = Seq(
      ("u1", "sess_1", "req_1", "item_1", 1, 0, 1.0, 100L),
      ("u1", "sess_1", "req_1", "item_2", 0, 0, 0.0, 100L)  // unknown movie -> null title, empty genres
    ).toDF("user_id", "session_id", "request_id", "item_id", "clicked", "ordered", "label",
      "impression_ts")

    val titleMap  = Map("item_1" -> "The Matrix")
    val genresMap = Map("item_1" -> "Sci-Fi,Action")
    val yearMap   = Map("item_1" -> "1999")

    val out = RelevanceSampleStreamingJob.buildRelevanceSamples(samples, titleMap, genresMap, yearMap)
    out.columns should contain allOf
      ("query", "event_ts", "recommended_movie_id", "title", "genres", "release_year", "score")

    val rows = out.collect().map(r => r.getAs[String]("recommended_movie_id") -> r).toMap

    val r1 = rows("item_1")
    r1.getAs[String]("query") shouldBe "u1:sess_1"
    r1.getAs[String]("title") shouldBe "The Matrix"
    r1.getAs[Seq[String]]("genres") shouldBe Seq("Sci-Fi", "Action")
    r1.getAs[Int]("release_year") shouldBe 1999
    r1.getAs[Double]("score") shouldBe 1.0

    val r2 = rows("item_2")
    Option(r2.getAs[String]("title")) shouldBe None
    r2.getAs[Seq[String]]("genres") shouldBe Seq.empty[String]
    r2.isNullAt(r2.fieldIndex("release_year")) shouldBe true
    r2.getAs[Double]("score") shouldBe 0.0
  }

  it should "give sessionless impressions from different requests distinct query keys" in {
    val s = spark; import s.implicits._

    // The joiner emits coalesce(session_id, ""), so a sessionless sample carries an empty string.
    // Under the old key both of these collapsed into the single query "u1:".
    val samples = Seq(
      ("u1", "", "req_1", "item_1", 1, 0, 1.0, 100L),
      ("u1", "", "req_2", "item_2", 0, 0, 0.0, 200L)
    ).toDF("user_id", "session_id", "request_id", "item_id", "clicked", "ordered", "label",
      "impression_ts")

    val queries = RelevanceSampleStreamingJob
      .buildRelevanceSamples(samples, Map.empty, Map.empty, Map.empty)
      .select("query").as[String].collect().toSet

    queries shouldBe Set("u1:req_1", "u1:req_2")
  }

  "fetchMovieFeatures" should "return empty map for no ids (no Redis call)" in {
    RelevanceSampleStreamingJob.fetchMovieFeatures(Array.empty, "localhost", 6379, 8) shouldBe Map.empty
  }
}
