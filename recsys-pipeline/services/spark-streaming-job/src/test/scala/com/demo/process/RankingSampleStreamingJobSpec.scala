package com.demo.process

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RankingSampleStreamingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("RankingSampleStreamingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "buildRankingSamples" should "attach embeddings, is_click and rating per impression" in {
    val s = spark; import s.implicits._

    val samples = Seq(
      ("u1", "sess_1", "item_1", 1, 0, 1.0, 100L, Map("tier" -> "gold"), Map("genre" -> "drama")),
      ("u1", "sess_1", "item_2", 0, 0, 0.0, 100L, Map("tier" -> "gold"), Map("genre" -> "comedy"))
    ).toDF("user_id", "session_id", "item_id", "clicked", "ordered", "label", "impression_ts",
           "user_features", "item_features")

    val userEmb = Map("u1" -> Seq(0.1, 0.2))
    val itemEmb = Map("item_1" -> Seq(0.3, 0.4))   // item_2 has no embedding → empty vector

    val out = RankingSampleStreamingJob.buildRankingSamples(samples, userEmb, itemEmb)
    out.columns should contain allOf
      ("user_id", "user_features", "user_embedding", "session_id", "event_ts",
       "recommended_movie_id", "item_features", "item_embedding", "is_click", "rating")

    val rows = out.collect()
      .map(r => r.getAs[String]("recommended_movie_id") ->
        (r.getAs[Boolean]("is_click"),
         r.getAs[Double]("rating"),
         r.getAs[Seq[Double]]("user_embedding"),
         r.getAs[Seq[Double]]("item_embedding")))
      .toMap

    rows("item_1") shouldBe (true, 1.0, Seq(0.1, 0.2), Seq(0.3, 0.4))
    rows("item_2") shouldBe (false, 0.0, Seq(0.1, 0.2), Seq.empty[Double])  // missing item emb → []
  }

  "fetchEmbeddings" should "return empty map for no ids (no Redis call)" in {
    RankingSampleStreamingJob.fetchEmbeddings(Array.empty, "uEmb", "localhost", 6379, 8) shouldBe Map.empty
  }
}
