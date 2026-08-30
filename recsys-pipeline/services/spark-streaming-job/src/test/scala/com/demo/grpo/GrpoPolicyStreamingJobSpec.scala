package com.demo.grpo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoPolicyStreamingJobSpec extends AnyFlatSpec with Matchers {

  private val cfg = GrpoJobConfig.from(Map.empty)

  private def group(rewards: Array[Double]): GrpoGroup = GrpoGroup(
    slateId = "s1",
    x = rewards.indices.map(i => Array.fill(cfg.dim)(0.1 * (i + 1))).toArray,
    logged = rewards.indices.map(_ => 0.5).toArray,
    rewards = rewards)

  "applyBatch" should "leave the weights untouched when no group survived the gate" in {
    val before = GrpoWeightStore.initial(cfg)
    val after = GrpoPolicyStreamingJob.applyBatch(before, Seq.empty, cfg, 7L)
    after.weights.toSeq shouldBe before.weights.toSeq
    // The batch id still advances: the job ran, it simply had nothing to learn from.
    after.batchId shouldBe 7L
    after.slatesApplied shouldBe 0L
  }

  it should "move the weights when a group carries reward variance" in {
    val after = GrpoPolicyStreamingJob.applyBatch(
      GrpoWeightStore.initial(cfg), Seq(group(Array(1.0, 0.0, 0.0))), cfg, 1L)
    after.weights.exists(_ != 0.0) shouldBe true
    after.slatesApplied shouldBe 1L
  }

  it should "accumulate slatesApplied across batches" in {
    val first = GrpoPolicyStreamingJob.applyBatch(
      GrpoWeightStore.initial(cfg), Seq(group(Array(1.0, 0.0))), cfg, 1L)
    val second = GrpoPolicyStreamingJob.applyBatch(first, Seq(group(Array(1.0, 0.0))), cfg, 2L)
    second.slatesApplied shouldBe 2L
  }

  it should "produce finite weights after many batches" in {
    // A diverging policy that silently becomes NaN would be served as a real score.
    val trained = (1 to 200).foldLeft(GrpoWeightStore.initial(cfg)) { (w, batch) =>
      GrpoPolicyStreamingJob.applyBatch(w, Seq(group(Array(1.0, 0.0, 0.0))), cfg, batch.toLong)
    }
    trained.weights.forall(v => !v.isNaN && !v.isInfinite) shouldBe true
  }

  it should "preserve the feature version it was fit under" in {
    GrpoPolicyStreamingJob.applyBatch(
      GrpoWeightStore.initial(cfg), Seq(group(Array(1.0, 0.0))), cfg, 1L
    ).featureVersion shouldBe cfg.featureVersion
  }
}
