package com.demo.grpo

import scala.jdk.CollectionConverters._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoWeightStoreSpec extends AnyFlatSpec with Matchers {

  private val cfg = GrpoJobConfig.from(Map.empty)

  "initial" should "be a zero vector of the configured width" in {
    val w = GrpoWeightStore.initial(cfg)
    w.weights.length shouldBe cfg.dim
    w.weights.forall(_ == 0.0) shouldBe true
    w.batchId shouldBe -1L
  }

  "encode then decode" should "round-trip the weights exactly" in {
    val original = GrpoWeights(Array(0.1, -0.2, 0.3, 0.0, 1.5, -0.7, 0.05, 0.0, 2.0, -1.0), "v1", 42L, 900L)
    val fields = GrpoWeightStore.encode(original, 1000L)
    val decoded = GrpoWeightStore.decode(fields.asScala.toMap, cfg)
    decoded.map(_.weights.toSeq) shouldBe Right(original.weights.toSeq)
    decoded.map(_.batchId) shouldBe Right(42L)
    decoded.map(_.slatesApplied) shouldBe Right(900L)
  }

  "decode" should "refuse weights fit against a different feature version" in {
    // Applying v1 weights to a v2 feature layout is silent, total nonsense. Refuse loudly.
    val fields = Map("weights" -> Array.fill(10)(0.1).mkString(","),
      "feature_version" -> "v0", "dim" -> "10", "batch_id" -> "1", "slates_applied" -> "1")
    GrpoWeightStore.decode(fields, cfg).left.map(_.contains("feature_version")) shouldBe Left(true)
  }

  it should "refuse a weight vector of the wrong width" in {
    val fields = Map("weights" -> "0.1,0.2", "feature_version" -> "v1", "dim" -> "2",
      "batch_id" -> "1", "slates_applied" -> "1")
    GrpoWeightStore.decode(fields, cfg).isLeft shouldBe true
  }

  it should "treat an empty hash as a cold start rather than an error" in {
    GrpoWeightStore.decode(Map.empty, cfg).map(_.weights.forall(_ == 0.0)) shouldBe Right(true)
  }
}
