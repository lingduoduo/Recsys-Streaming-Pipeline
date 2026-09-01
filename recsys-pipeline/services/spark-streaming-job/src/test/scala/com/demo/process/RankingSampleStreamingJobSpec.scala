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

  "buildRankingSamples" should "attach embeddings via a LEFT join, is_click and rating per impression" in {
    val s = spark; import s.implicits._

    val samples = Seq(
      ("u1", "sess_1", "item_1", 1, 0, 1.0, 100L, Map("tier" -> "gold"), Map("genre" -> "drama")),
      ("u1", "sess_1", "item_2", 0, 0, 0.0, 100L, Map("tier" -> "gold"), Map("genre" -> "comedy"))
    ).toDF("user_id", "session_id", "item_id", "clicked", "ordered", "label", "impression_ts",
           "user_features", "item_features")

    val userEmbeddings = Seq(("u1", Seq(0.1, 0.2))).toDF("user_id", "user_embedding")
    // item_2 has no Redis embedding at all -- fetchEmbeddingsDf omits ids with a missing/empty
    // value, so `itemEmbeddings` never carries item_2. buildRankingSamples must LEFT join against
    // this so the row still comes out (with an empty embedding), not be silently dropped by an
    // inner join.
    val itemEmbeddings = Seq(("item_1", Seq(0.3, 0.4))).toDF("item_id", "item_embedding")

    val out = RankingSampleStreamingJob.buildRankingSamples(samples, userEmbeddings, itemEmbeddings)
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

    // THE TRAP: item_2 is entirely missing from `itemEmbeddings` (no Redis value). An inner join
    // would silently drop this row; buildRankingSamples must LEFT join so it still comes out,
    // with an empty embedding -- exactly like the old map-lookup path's getOrElse(Seq.empty)
    // default for an unknown id.
    rows("item_2") shouldBe (false, 0.0, Seq(0.1, 0.2), Seq.empty[Double])
  }

  "embeddingRowOrNone" should "split a space-separated Redis value into a Double vector" in {
    val row = RankingSampleStreamingJob.embeddingRowOrNone("u1", "0.1 0.2 0.3")
    row shouldBe defined
    row.get.getString(0) shouldBe "u1"
    row.get.getAs[Seq[Double]](1) shouldBe Seq(0.1, 0.2, 0.3)
  }

  it should "drop non-numeric tokens rather than throw" in {
    val row = RankingSampleStreamingJob.embeddingRowOrNone("u1", "0.1 nope 0.3").get
    row.getAs[Seq[Double]](1) shouldBe Seq(0.1, 0.3)
  }

  it should "return None for a null value (missing key)" in {
    RankingSampleStreamingJob.embeddingRowOrNone("missing", null) shouldBe None
  }

  it should "return None for a blank value" in {
    RankingSampleStreamingJob.embeddingRowOrNone("missing", "   ") shouldBe None
  }

  "fetchEmbeddingsDf" should "return an empty, correctly-shaped DataFrame without contacting Redis when there are no ids" in {
    val s = spark; import s.implicits._
    val ids = Seq.empty[String].toDF("id")

    // Host is unreachable on purpose: an empty ids DataFrame must never open a Jedis connection.
    val result = RankingSampleStreamingJob
      .fetchEmbeddingsDf(ids, "uEmb", "unreachable-host.invalid", 1, 1, 500)

    result.columns.toSeq shouldBe Seq("id", "embedding")
    result.isEmpty shouldBe true
  }
}
