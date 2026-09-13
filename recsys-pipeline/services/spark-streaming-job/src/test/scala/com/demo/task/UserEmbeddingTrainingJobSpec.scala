package com.demo.task

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserEmbeddingTrainingJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def embeddings() = {
    val s = spark; import s.implicits._
    val ratings = Seq(
      ("u1", "m1", 5.0), ("u1", "m2", 4.0), ("u1", "m3", 1.0),   // m3 falls below the threshold
      ("u2", "m3", 2.0),                                         // nothing qualifies
      ("u3", "m9", 5.0)                                          // qualifies, but m9 has no vector
    ).toDF("userId", "movieId", "rating")
    val items = Seq(("m1", Seq(1.0, 0.0)), ("m2", Seq(0.0, 1.0)), ("m3", Seq(1.0, 1.0)))
      .toDF("movieId", "vector")
    UserEmbeddingTrainingJob.trainUserEmbeddings(ratings, items, minRating = 3.5)
      .collect()
      .map(row => row.getAs[String]("userId") -> row.getAs[Seq[Double]]("userEmbedding"))
      .toMap
  }

  "trainUserEmbeddings" should "average the item vectors of a user's qualifying ratings" in {
    embeddings()("u1") shouldBe Seq(0.5, 0.5)
  }

  it should "omit users with no qualifying rating and users whose items carry no vector" in {
    embeddings().keySet shouldBe Set("u1")
  }
}
