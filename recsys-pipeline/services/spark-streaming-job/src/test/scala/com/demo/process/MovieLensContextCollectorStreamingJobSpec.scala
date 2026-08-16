package com.demo.process

import com.demo.SparkTestSupport
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MovieLensContextCollectorStreamingJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  /** These fixtures label each row with one `kind`; `parseEvents` emits three independent flags,
    * so a row can now carry more than one. Expand the single label into the flag columns the
    * aggregations read. */
  private def withKindFlags(df: DataFrame): DataFrame =
    df.withColumn("is_rating", col("kind") === "rating")
      .withColumn("is_user_update", col("kind") === "user_update")
      .withColumn("is_movie_update", col("kind") === "movie_update")
      .drop("kind")

  "parseEvents" should "classify user movie and rating context events" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val raw = Seq(
      """{"user_id":"u1","age":31,"gender":"F","occupation":"engineer","zip_code":"10001","timestamp":100}""",
      """{"item_id":"m1","title":"Arrival","genres":["Drama","Sci-Fi"],"release_year":2016,"timestamp":101}""",
      """{"user_id":"u1","item_id":"m1","event_type":"rating","rating":4.5,"timestamp":102}""",
      """{"user_id":"u2","item_id":"m2","event_type":"click","timestamp":103}"""
    ).toDF("value")

    val classified = MovieLensContextCollectorStreamingJob.parseEvents(raw)
      .select("is_rating", "is_user_update", "is_movie_update")
      .collect()
      .map(r => (r.getBoolean(0), r.getBoolean(1), r.getBoolean(2)))

    // The click event matches no rule and is dropped; the other three each match exactly one.
    classified.length shouldBe 3
    classified.count(_._1) shouldBe 1
    classified.count(_._2) shouldBe 1
    classified.count(_._3) shouldBe 1
  }

  it should "feed both aggregates when one event carries a rating and demographics" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val raw = Seq(
      """{"user_id":"u1","item_id":"m1","event_type":"rating","rating":4.0,"timestamp":100,""" +
        """"age":31,"gender":"F","occupation":"engineer","zip_code":"94107"}"""
    ).toDF("value")

    val events = MovieLensContextCollectorStreamingJob.parseEvents(raw)
    val row = MovieLensContextCollectorStreamingJob.buildUserFeatureUpdates(events).first()

    // Under the old first-match chain this row classified as "rating" and its demographics
    // were discarded outright.
    row.getAs[Int]("age") shouldBe 31
    row.getAs[String]("gender") shouldBe "F"
    row.getAs[String]("occupation") shouldBe "engineer"
    row.getAs[String]("zipCode") shouldBe "94107"
    row.getAs[Long]("ratingCountDelta") shouldBe 1L
  }

  "buildUserFeatureUpdates" should "aggregate demographics and rating deltas per user" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("user_update", "u1", null, null.asInstanceOf[java.lang.Double], 100L, 31: java.lang.Integer, "F", "engineer", "10001", null, Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("rating", "u1", "m1", 4.0: java.lang.Double, 105L, null, null, null, null, null, Seq.empty[String], null),
      ("rating", "u1", "m2", 5.0: java.lang.Double, 110L, null, null, null, null, null, Seq.empty[String], null)
    ).toDF("kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    val row = MovieLensContextCollectorStreamingJob.buildUserFeatureUpdates(withKindFlags(events), recentRatingsLimit = 10).first()

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
    ).toDF("kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    val row = MovieLensContextCollectorStreamingJob.buildMovieFeatureUpdates(withKindFlags(events)).first()

    row.getAs[String]("item_id") shouldBe "m1"
    row.getAs[String]("title") shouldBe "Arrival"
    row.getAs[Seq[String]]("genres") shouldBe Seq("Drama", "Sci-Fi")
    row.getAs[Int]("releaseYear") shouldBe 2016
  }

  "buildSequenceEvents" should "project rating events into sequence-store shape with millisecond timestamps" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("rating", "u1", "m1", 4.0: java.lang.Double, 105L, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq("Drama"), 1995: java.lang.Integer),
      ("user_update", "u1", null, null.asInstanceOf[java.lang.Double], 100L, 31: java.lang.Integer, "F", "engineer", "10001", null, Seq.empty[String], null)
    ).toDF("kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    val rows = MovieLensContextCollectorStreamingJob.buildSequenceEvents(withKindFlags(events)).collect()

    rows.length shouldBe 1
    rows.head.getAs[String]("user_id") shouldBe "u1"
    rows.head.getAs[String]("kind") shouldBe "rating"
    rows.head.getAs[String]("item_id") shouldBe "m1"
    rows.head.getAs[Long]("ts") shouldBe 105000L
    rows.head.getAs[String]("action") shouldBe "rate"
    rows.head.getAs[Double]("rating") shouldBe 4.0
    rows.head.getAs[Seq[String]]("genres") shouldBe Seq("Drama")
    rows.head.getAs[Int]("release_year") shouldBe 1995
  }

  it should "drop rows missing a user, item or timestamp" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("rating", null, "m1", 4.0: java.lang.Double, 105L: java.lang.Long, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("rating", "u1", null, 4.0: java.lang.Double, 105L: java.lang.Long, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq.empty[String], null),
      ("rating", "u1", "m1", 4.0: java.lang.Double, null.asInstanceOf[java.lang.Long], null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq.empty[String], null)
    ).toDF("kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    MovieLensContextCollectorStreamingJob.buildSequenceEvents(withKindFlags(events)).count() shouldBe 0L
  }

  it should "feed SequenceEncoder to produce one chunk per user and day" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("rating", "u1", "m1", 4.0: java.lang.Double, 1784764801L, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq("Drama"), 1995: java.lang.Integer),
      ("rating", "u1", "m2", 5.0: java.lang.Double, 1784764802L, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq("Action"), 1999: java.lang.Integer)
    ).toDF("kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    val chunk = com.demo.sequence.SequenceEncoder
      .toColumnChunks(MovieLensContextCollectorStreamingJob.buildSequenceEvents(withKindFlags(events)))
      .collect()

    chunk.length shouldBe 1
    chunk.head.getAs[String]("bucket") shouldBe "20260723"
    chunk.head.getAs[String]("item_id") shouldBe "m1,m2"
    chunk.head.getAs[Long]("n") shouldBe 2L
  }
}
