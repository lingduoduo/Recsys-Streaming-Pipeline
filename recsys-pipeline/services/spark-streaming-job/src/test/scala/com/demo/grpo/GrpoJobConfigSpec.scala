package com.demo.grpo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoJobConfigSpec extends AnyFlatSpec with Matchers {

  "from" should "supply the documented defaults on an empty environment" in {
    val cfg = GrpoJobConfig.from(Map.empty)
    cfg.hyper.temperature shouldBe 1.0
    cfg.hyper.clipEpsilon shouldBe 0.2
    cfg.hyper.klBeta shouldBe 0.02
    cfg.hyper.learningRate shouldBe 0.01
    cfg.hyper.innerEpochs shouldBe 4
    cfg.featureVersion shouldBe "v2"
    cfg.dim shouldBe 9
    cfg.weightsKey shouldBe "grpo:policy:weights"
  }

  it should "read every hyperparameter from the environment" in {
    val cfg = GrpoJobConfig.from(Map(
      "GRPO_TEMPERATURE" -> "0.5", "GRPO_CLIP_EPSILON" -> "0.1",
      "GRPO_KL_BETA" -> "0.05", "GRPO_LEARNING_RATE" -> "0.003",
      "GRPO_INNER_EPOCHS" -> "8"))
    cfg.hyper.temperature shouldBe 0.5
    cfg.hyper.innerEpochs shouldBe 8
  }

  it should "fall back to the default on an unparseable value" in {
    // Mirrors SequenceJobConfig: a typo must not silently change the objective.
    GrpoJobConfig.from(Map("GRPO_KL_BETA" -> "yes")).hyper.klBeta shouldBe 0.02
  }

  it should "force inner epochs above one" in {
    // At one step per batch pi still equals the snapshot when the ratio is formed, so r = 1
    // everywhere and the clip branch is unreachable. Clipping would be decorative.
    GrpoJobConfig.from(Map("GRPO_INNER_EPOCHS" -> "1")).hyper.innerEpochs shouldBe 2
    GrpoJobConfig.from(Map("GRPO_INNER_EPOCHS" -> "0")).hyper.innerEpochs shouldBe 2
  }

  it should "reject a non-positive temperature, which would divide by zero" in {
    GrpoJobConfig.from(Map("GRPO_TEMPERATURE" -> "0")).hyper.temperature shouldBe 1.0
    GrpoJobConfig.from(Map("GRPO_TEMPERATURE" -> "-1")).hyper.temperature shouldBe 1.0
  }

  it should "reject a non-positive learning rate, which is gradient ascent" in {
    // A leading minus is a typo the job cannot detect at runtime: every batch would look healthy
    // while the policy learned to do the opposite of what the reward says.
    GrpoJobConfig.from(Map("GRPO_LEARNING_RATE" -> "-0.01")).hyper.learningRate shouldBe 0.01
    GrpoJobConfig.from(Map("GRPO_LEARNING_RATE" -> "0")).hyper.learningRate shouldBe 0.01
  }

  it should "reject a non-positive clip epsilon, which inverts the PPO min" in {
    GrpoJobConfig.from(Map("GRPO_CLIP_EPSILON" -> "-0.2")).hyper.clipEpsilon shouldBe 0.2
    GrpoJobConfig.from(Map("GRPO_CLIP_EPSILON" -> "0")).hyper.clipEpsilon shouldBe 0.2
  }

  it should "reject a negative kl beta but keep zero, which turns the anchor off" in {
    GrpoJobConfig.from(Map("GRPO_KL_BETA" -> "-0.02")).hyper.klBeta shouldBe 0.02
    GrpoJobConfig.from(Map("GRPO_KL_BETA" -> "0")).hyper.klBeta shouldBe 0.0
  }

  it should "reject an infinite hyperparameter" in {
    // "Infinity" parses as a Double, so a positivity check alone would let it through.
    GrpoJobConfig.from(Map("GRPO_LEARNING_RATE" -> "Infinity")).hyper.learningRate shouldBe 0.01
    GrpoJobConfig.from(Map("GRPO_KL_BETA" -> "NaN")).hyper.klBeta shouldBe 0.02
  }
}
