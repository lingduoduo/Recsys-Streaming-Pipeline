package com.demo.recsys

import com.demo.SparkTestSupport
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ItemSequencePreprocessingJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def makeRatings(rows: (String, String, Double, Long)*) = {
    val sparkSession = spark
    import sparkSession.implicits._
    rows.toDF("userId", "movieId", "rating", "timestamp")
  }

  "processItemSequenceDataFrame" should "sort items by timestamp ascending within each user" in {
    val ratings = makeRatings(
      ("u1", "item1", 4.0, 200L),
      ("u1", "item2", 4.0, 50L),
      ("u1", "item3", 4.0, 100L)
    )
    val result = ItemSequencePreprocessingJob.processItemSequenceDataFrame(
      ratingSamples = ratings,
      minRating = 3.5,
      userIdColumn = "userId",
      itemIdColumn = "movieId",
      ratingColumn = "rating",
      timestampColumn = "timestamp"
    )
    val row = result.filter(col("userId") === "u1").first()
    row.getAs[Seq[String]]("movieIds") shouldBe Seq("item2", "item3", "item1")
  }

  it should "exclude users with fewer than two interactions" in {
    val ratings = makeRatings(
      ("u1", "item1", 4.0, 100L),
      ("u1", "item2", 4.0, 200L),
      ("u2", "item3", 4.0, 100L)
    )
    val result = ItemSequencePreprocessingJob.processItemSequenceDataFrame(
      ratingSamples = ratings,
      minRating = 3.5,
      userIdColumn = "userId",
      itemIdColumn = "movieId",
      ratingColumn = "rating",
      timestampColumn = "timestamp"
    )
    val userIds = result.select("userId").collect().map(_.getString(0)).toSet
    userIds should contain("u1")
    userIds should not contain "u2"
  }

  it should "exclude items rated below minRating" in {
    val ratings = makeRatings(
      ("u1", "item1", 4.0, 100L),
      ("u1", "item2", 2.0, 200L),
      ("u1", "item3", 4.0, 300L)
    )
    val result = ItemSequencePreprocessingJob.processItemSequenceDataFrame(
      ratingSamples = ratings,
      minRating = 3.5,
      userIdColumn = "userId",
      itemIdColumn = "movieId",
      ratingColumn = "rating",
      timestampColumn = "timestamp"
    )
    val row = result.filter(col("userId") === "u1").first()
    row.getAs[Seq[String]]("movieIds") should not contain "item2"
  }

  it should "produce movieIdStr matching space-joined movieIds" in {
    val ratings = makeRatings(
      ("u1", "a", 4.0, 1L),
      ("u1", "b", 4.0, 2L),
      ("u1", "c", 4.0, 3L)
    )
    val result = ItemSequencePreprocessingJob.processItemSequenceDataFrame(
      ratingSamples = ratings,
      minRating = 3.5,
      userIdColumn = "userId",
      itemIdColumn = "movieId",
      ratingColumn = "rating",
      timestampColumn = "timestamp"
    )
    val row = result.filter(col("userId") === "u1").first()
    val ids   = row.getAs[Seq[String]]("movieIds")
    val idStr = row.getAs[String]("movieIdStr")
    idStr shouldBe ids.mkString(" ")
  }
}
