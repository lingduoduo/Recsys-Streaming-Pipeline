package com.demo.grpo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoPolicyStreamingJobSpec extends AnyFlatSpec with Matchers {

  private val cfg = GrpoJobConfig.from(Map.empty)

  private def group(rewards: Array[Double], logged: Array[Double] = null): GrpoGroup = GrpoGroup(
    slateId = "s1",
    x = rewards.indices.map(i => Array.fill(cfg.dim)(0.1 * (i + 1))).toArray,
    logged = if (logged != null) logged else rewards.indices.map(_ => 0.5).toArray,
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
    // A diverging policy that silently becomes NaN would be served as a real score. The original
    // fixture (lr 0.01, reward spread 1.0) converges quietly and never approaches an edge case, so
    // it could only ever catch a gross NaN that AdvantageFloor already forecloses. An aggressive
    // learning rate plus a wide reward spread pushes logits hard enough on every batch that any
    // regression to the numerical guards (softmax overflow, AdvantageFloor) would show up here.
    val stressCfg = cfg.copy(hyper = cfg.hyper.copy(learningRate = 5.0))
    val trained = (1 to 200).foldLeft(GrpoWeightStore.initial(stressCfg)) { (w, batch) =>
      GrpoPolicyStreamingJob.applyBatch(
        w, Seq(group(Array(1000.0, -1000.0, 0.0))), stressCfg, batch.toLong)
    }
    trained.weights.forall(v => !v.isNaN && !v.isInfinite) shouldBe true
  }

  it should "preserve the feature version it was fit under" in {
    GrpoPolicyStreamingJob.applyBatch(
      GrpoWeightStore.initial(cfg), Seq(group(Array(1.0, 0.0))), cfg, 1L
    ).featureVersion shouldBe cfg.featureVersion
  }

  it should "not mutate the input weights array" in {
    // GrpoWeights.weights is a plain Array, so a missing clone silently mutates the caller's copy
    // in place. `before` is the SAME object handed to applyBatch, not a fresh read -- if the
    // implementation aliases it instead of cloning, this array changes underneath us.
    val before = GrpoWeightStore.initial(cfg)
    val original = before.weights.clone()
    GrpoPolicyStreamingJob.applyBatch(before, Seq(group(Array(1.0, 0.0, 0.0))), cfg, 1L)
    before.weights.toSeq shouldBe original.toSeq
  }

  it should "keep the ratio anchored to the batch-start snapshot across inner epochs" in {
    // If the snapshot tracked the epoch-updated weights instead of staying frozen, the first
    // epoch would still look right (snapshot == current weights at that point either way) but the
    // second epoch would silently start measuring the ratio against a moving target. Reproduce
    // the two epochs by hand, anchored to ONE frozen snapshot throughout, using the
    // already-verified GrpoMath.gradient directly -- then check applyBatch agrees at both points.
    val groups = Seq(group(Array(1.0, 0.0, 0.0)))
    val hyper1 = cfg.hyper.copy(innerEpochs = 1)
    val hyper2 = cfg.hyper.copy(innerEpochs = 2)
    val cfg1 = cfg.copy(hyper = hyper1)
    val cfg2 = cfg.copy(hyper = hyper2)

    val snapshot = GrpoWeightStore.initial(cfg).weights.clone()
    val snapshotLogitsByGroup = groups.map { g =>
      g.x.map(row => row.indices.foldLeft(0.0)((a, i) => a + row(i) * snapshot(i)))
    }
    var wRef = snapshot.clone()
    var afterEpoch1: Array[Double] = null
    (1 to 2).foreach { epoch =>
      val total = Array.fill(cfg.dim)(0.0)
      groups.zip(snapshotLogitsByGroup).foreach { case (g, snapshotLogits) =>
        GrpoMath.advantages(g.rewards).foreach { adv =>
          val grad = GrpoMath.gradient(g.x, snapshotLogits, g.logged, wRef, adv, cfg.hyper)
          (0 until cfg.dim).foreach(d => total(d) += grad(d))
        }
      }
      (0 until cfg.dim).foreach(d => wRef(d) -= cfg.hyper.learningRate * total(d) / groups.size)
      if (epoch == 1) afterEpoch1 = wRef.clone()
    }

    val oneEpochResult = GrpoPolicyStreamingJob.applyBatch(GrpoWeightStore.initial(cfg1), groups, cfg1, 1L)
    val twoEpochResult = GrpoPolicyStreamingJob.applyBatch(GrpoWeightStore.initial(cfg2), groups, cfg2, 1L)

    oneEpochResult.weights.zip(afterEpoch1).foreach { case (a, b) => a shouldBe b +- 1e-9 }
    twoEpochResult.weights.zip(wRef).foreach { case (a, b) => a shouldBe b +- 1e-9 }
  }

  it should "not let the logged reference leak into the ratio term" in {
    // Starting from all-zero weights, the TRUE snapshot logits are 0 for every candidate
    // regardless of features, so the surrogate/ratio term is mathematically independent of
    // `logged` -- only the KL term reads `logged`. Zero out klBeta and the result must therefore
    // be identical no matter what `logged` says. If the ratio term were fed `logged`-derived
    // logits instead of the real snapshot (collapsing the two references), it would keep
    // responding to `logged` even with the KL term disabled. The original fixture's uniform
    // `logged = 0.5` can't expose this: a uniform vector produces a uniform softmax, which is
    // indistinguishable from the always-uniform zero-weight snapshot.
    val noKl = cfg.copy(hyper = cfg.hyper.copy(klBeta = 0.0))
    val rewards = Array(1.0, 0.0, 0.0)
    val g1 = group(rewards, logged = Array(2.0, -1.0, 0.0))
    val g2 = group(rewards, logged = Array(-3.0, 4.0, 1.0))

    val r1 = GrpoPolicyStreamingJob.applyBatch(GrpoWeightStore.initial(noKl), Seq(g1), noKl, 1L)
    val r2 = GrpoPolicyStreamingJob.applyBatch(GrpoWeightStore.initial(noKl), Seq(g2), noKl, 1L)

    r1.weights.zip(r2.weights).foreach { case (a, b) => a shouldBe b +- 1e-9 }
  }

  "stepBatch" should "pass a finite update through unchanged" in {
    val current = GrpoWeightStore.initial(cfg)
    val stepped = GrpoPolicyStreamingJob.stepBatch(current, Seq(group(Array(1.0, 0.0, 0.0))), cfg, 1L)
    stepped.map(_.weights.toSeq) shouldBe
      Some(GrpoPolicyStreamingJob.applyBatch(current, Seq(group(Array(1.0, 0.0, 0.0))), cfg, 1L).weights.toSeq)
  }

  it should "refuse a diverged update rather than persist it" in {
    // The weights key has no TTL: one NaN written to it is permanent, every later batch reads it
    // back, and serving scores NaN until someone deletes the key by hand. A NaN feature is the
    // shortest route to a non-finite gradient; the point under test is the refusal, not the cause.
    val poisoned = GrpoGroup(
      slateId = "s1",
      x = Array(Array.fill(cfg.dim)(Double.NaN), Array.fill(cfg.dim)(0.1)),
      logged = Array(0.5, 0.5),
      rewards = Array(1.0, 0.0))
    GrpoPolicyStreamingJob.stepBatch(GrpoWeightStore.initial(cfg), Seq(poisoned), cfg, 1L) shouldBe None
  }

  it should "leave the caller holding the last good weights when a batch diverges" in {
    val lastGood = GrpoPolicyStreamingJob.applyBatch(
      GrpoWeightStore.initial(cfg), Seq(group(Array(1.0, 0.0))), cfg, 1L)
    val infinite = GrpoGroup(
      slateId = "s2",
      x = Array(Array.fill(cfg.dim)(Double.PositiveInfinity), Array.fill(cfg.dim)(0.1)),
      logged = Array(0.5, 0.5),
      rewards = Array(1.0, 0.0))
    GrpoPolicyStreamingJob.stepBatch(lastGood, Seq(infinite), cfg, 2L) shouldBe None
    // applyBatch clones, so the rejected batch cannot have touched what the job still holds.
    lastGood.weights.forall(java.lang.Double.isFinite) shouldBe true
  }
}
