package com.demo.process

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MovieLensContextCollectorStreamingJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "parseEvents" should "classify user movie and rating context events" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val raw = Seq(
      """{"user_id":"u1","age":31,"gender":"F","occupation":"engineer","zip_code":"10001","timestamp":100}""",
      """{"item_id":"m1","title":"Arrival","genres":["Drama","Sci-Fi"],"release_year":2016,"timestamp":101}""",
      """{"user_id":"u1","item_id":"m1","event_type":"rating","rating":4.5,"timestamp":102}""",
      """{"user_id":"u2","item_id":"m2","event_type":"click","timestamp":103}"""
    ).toDF("value")

    val kinds = MovieLensContextCollectorStreamingJob.parseEvents(raw)
      .select("event_kind")
      .as[String]
      .collect()
      .toSet

    kinds shouldBe Set("user_update", "movie_update", "rating")
  }

  "buildUserFeatureUpdates" should "aggregate demographics and rating deltas per user" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("user_update", "u1", null, null.asInstanceOf[java.lang.Double], 100L, 31: java.lang.Integer, "F", "engineer", "10001", null, Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("rating", "u1", "m1", 4.0: java.lang.Double, 105L, null, null, null, null, null, Seq.empty[String], null),
      ("rating", "u1", "m2", 5.0: java.lang.Double, 110L, null, null, null, null, null, Seq.empty[String], null)
    ).toDF("event_kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    val row = MovieLensContextCollectorStreamingJob.buildUserFeatureUpdates(events, recentRatingsLimit = 10).first()

    row.getAs[String]("user_id") shouldBe "u1"
    row.getAs[Int]("age") shouldBe 31
    row.getAs[String]("gender") shouldBe "F"
    row.getAs[String]("occupation") shouldBe "engineer"
    row.getAs[String]("zipCode") shouldBe "10001"
    row.getAs[Long]("ratingCountDelta") shouldBe 2L
    row.getAs[Double]("ratingSumDelta") shouldBe 9.0
    row.getAs[Seq[String]]("recentlyRatedMovieIds") shouldBe Seq("m2", "m1")
  }

  it should "aggregate movie metadata updates" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("movie_update", null, "m1", null.asInstanceOf[java.lang.Double], 101L, null.asInstanceOf[java.lang.Integer], null, null, null, "Arrival", Seq("Drama", "Sci-Fi"), 2016: java.lang.Integer)
    ).toDF("event_kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    val row = MovieLensContextCollectorStreamingJob.buildMovieFeatureUpdates(events).first()

    row.getAs[String]("item_id") shouldBe "m1"
    row.getAs[String]("title") shouldBe "Arrival"
    row.getAs[Seq[String]]("genres") shouldBe Seq("Drama", "Sci-Fi")
    row.getAs[Int]("releaseYear") shouldBe 2016
  }
}
