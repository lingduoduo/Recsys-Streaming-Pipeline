package com.demo.process

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

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

  "buildRelevanceSamples" should "build query, join title/genres/year from a features DataFrame, and score by label" in {
    val s = spark; import s.implicits._

    val samples = Seq(
      ("u1", "sess_1", "req_1", "item_1", 1, 0, 1.0, 100L),
      ("u1", "sess_1", "req_1", "item_2", 0, 0, 0.0, 100L)  // item_2 has no Redis hash at all
    ).toDF("user_id", "session_id", "request_id", "item_id", "clicked", "ordered", "label",
      "impression_ts")

    // Only item_1 gets a row here: fetchMovieFeaturesDf omits ids with a missing/empty Redis hash,
    // so `features` never carries item_2 at all -- this is what buildRelevanceSamples must join
    // against with a LEFT join, not an inner one.
    val features = Seq(
      ("item_1", "The Matrix", Seq("Sci-Fi", "Action"), 1999)
    ).toDF("item_id", "title", "genres", "release_year")

    val out = RelevanceSampleStreamingJob.buildRelevanceSamples(samples, features)
    out.columns should contain allOf
      ("query", "event_ts", "recommended_movie_id", "title", "genres", "release_year", "score")

    val rows = out.collect().map(r => r.getAs[String]("recommended_movie_id") -> r).toMap

    val r1 = rows("item_1")
    r1.getAs[String]("query") shouldBe "u1:sess_1"
    r1.getAs[String]("title") shouldBe "The Matrix"
    r1.getAs[Seq[String]]("genres") shouldBe Seq("Sci-Fi", "Action")
    r1.getAs[Int]("release_year") shouldBe 1999
    r1.getAs[Double]("score") shouldBe 1.0

    // THE TRAP: item_2 is entirely missing from `features` (no Redis hash). An inner join would
    // silently drop this row; buildRelevanceSamples must LEFT join so it still comes out, with
    // nulled-out metadata -- exactly like the old UDF-based path did for an unknown movie.
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

    val emptyFeatures = Seq.empty[(String, String, Seq[String], Int)]
      .toDF("item_id", "title", "genres", "release_year")

    val queries = RelevanceSampleStreamingJob
      .buildRelevanceSamples(samples, emptyFeatures)
      .select("query").as[String].collect().toSet

    queries shouldBe Set("u1:req_1", "u1:req_2")
  }

  "featuresRowOrNone" should "map a representative Redis hash to title/genres/release_year" in {
    val h: java.util.Map[String, String] =
      Map("title" -> "The Matrix", "genres" -> "Sci-Fi,Action", "releaseYear" -> "1999").asJava

    val row = RelevanceSampleStreamingJob.featuresRowOrNone("item_1", h)
    row shouldBe defined
    row.get.getString(0) shouldBe "item_1"
    row.get.getString(1) shouldBe "The Matrix"
    row.get.getAs[Seq[String]](2) shouldBe Seq("Sci-Fi", "Action")
    row.get.getInt(3) shouldBe 1999
  }

  it should "default genres to empty and release_year to null when those fields are absent from a present hash" in {
    val h: java.util.Map[String, String] = Map("title" -> "Unknown Metadata").asJava
    val row = RelevanceSampleStreamingJob.featuresRowOrNone("item_2", h).get
    row.getAs[Seq[String]](2) shouldBe Seq.empty[String]
    row.isNullAt(3) shouldBe true
  }

  it should "yield a null release_year, not throw, for a non-numeric releaseYear" in {
    val h: java.util.Map[String, String] = Map("releaseYear" -> "not-a-year").asJava
    val row = RelevanceSampleStreamingJob.featuresRowOrNone("item_3", h).get
    row.isNullAt(3) shouldBe true
  }

  it should "return None for a null hash (missing key)" in {
    RelevanceSampleStreamingJob.featuresRowOrNone("missing", null) shouldBe None
  }

  it should "return None for an empty hash" in {
    RelevanceSampleStreamingJob.featuresRowOrNone("missing", new java.util.HashMap[String, String]()) shouldBe None
  }

  "fetchMovieFeaturesDf" should "return an empty, correctly-shaped DataFrame without contacting Redis when there are no ids" in {
    val s = spark; import s.implicits._
    val ids = Seq.empty[String].toDF("item_id")

    // Host is unreachable on purpose: an empty ids DataFrame must never open a Jedis connection.
    val result = RelevanceSampleStreamingJob.fetchMovieFeaturesDf(ids, "unreachable-host.invalid", 1, 1, 500)

    result.columns.toSeq shouldBe Seq("item_id", "title", "genres", "release_year")
    result.isEmpty shouldBe true
  }
}
