package com.demo.recsys

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmbeddingCandidateGenerationJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def embeddings(vecs: (String, Seq[Double])*) = {
    val ss = spark; import ss.implicits._
    vecs.toSeq.toDF("id", "vector")
  }

  "topKCandidates" should "return the closest item first for each user" in {
    // u1 = [1,0], u2 = [0,1] — orthogonal users
    val users = embeddings("u1" -> Seq(1.0, 0.0), "u2" -> Seq(0.0, 1.0))
    // iA = [1,0] aligns with u1; iB = [0,1] aligns with u2; iC = [1,1] is mid
    val items = embeddings("iA" -> Seq(1.0, 0.0), "iB" -> Seq(0.0, 1.0), "iC" -> Seq(1.0, 1.0))

    val result = EmbeddingCandidateGenerationJob
      .topKCandidates(spark, users, items, topK = 2)
      .collect()
      .map(r => r.getAs[String]("userId") -> r.getAs[Seq[String]]("candidateItems"))
      .toMap

    result("u1").head shouldBe "iA"
    result("u2").head shouldBe "iB"
    result("u1") should not contain "iB"
    result("u2") should not contain "iA"
  }

  it should "respect the topK limit" in {
    val users = embeddings("u1" -> Seq(1.0, 0.0))
    val items = embeddings(
      "i1" -> Seq(1.0, 0.0), "i2" -> Seq(0.9, 0.1),
      "i3" -> Seq(0.5, 0.5), "i4" -> Seq(0.0, 1.0), "i5" -> Seq(-1.0, 0.0)
    )

    val candidates = EmbeddingCandidateGenerationJob
      .topKCandidates(spark, users, items, topK = 3)
      .first()
      .getAs[Seq[String]]("candidateItems")

    candidates.size shouldBe 3
  }

  it should "return scores in descending order" in {
    val users = embeddings("u1" -> Seq(1.0, 0.0))
    val items = embeddings("iA" -> Seq(1.0, 0.0), "iB" -> Seq(0.5, 0.5), "iC" -> Seq(0.0, 1.0))

    val scores = EmbeddingCandidateGenerationJob
      .topKCandidates(spark, users, items, topK = 3)
      .first()
      .getAs[Seq[Double]]("scores")

    scores.zip(scores.tail).foreach { case (a, b) => a should be >= b }
  }

  "cosine" should "return 1.0 for identical unit vectors" in {
    EmbeddingCandidateGenerationJob.cosine(Array(1.0, 0.0), Array(1.0, 0.0)) shouldBe 1.0 +- 1e-9
  }

  it should "return 0.0 for orthogonal vectors" in {
    EmbeddingCandidateGenerationJob.cosine(Array(1.0, 0.0), Array(0.0, 1.0)) shouldBe 0.0 +- 1e-9
  }

  it should "return -1.0 for opposite vectors" in {
    EmbeddingCandidateGenerationJob.cosine(Array(1.0, 0.0), Array(-1.0, 0.0)) shouldBe -1.0 +- 1e-9
  }

  it should "be invariant to vector magnitude" in {
    val cos1 = EmbeddingCandidateGenerationJob.cosine(Array(1.0, 0.0), Array(1.0, 1.0))
    val cos2 = EmbeddingCandidateGenerationJob.cosine(Array(2.0, 0.0), Array(3.0, 3.0))
    cos1 shouldBe cos2 +- 1e-9
    cos1 shouldBe (1.0 / math.sqrt(2)) +- 1e-9
  }
}
