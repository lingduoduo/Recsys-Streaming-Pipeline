package com.demo.grpo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoMathSpec extends AnyFlatSpec with Matchers {

  private val cfg = GrpoHyperParams(
    temperature = 1.0, clipEpsilon = 0.2, klBeta = 0.02, learningRate = 0.01, innerEpochs = 4)

  "softmax" should "produce a distribution summing to one" in {
    GrpoMath.softmax(Array(1.0, 2.0, 3.0), 1.0).sum shouldBe 1.0 +- 1e-12
  }

  it should "not overflow on large logits" in {
    // Naive exp() overflows here; the max must be subtracted first.
    val p = GrpoMath.softmax(Array(1000.0, 1001.0), 1.0)
    p.forall(v => !v.isNaN) shouldBe true
    p.sum shouldBe 1.0 +- 1e-12
  }

  "advantages" should "centre on the group mean and scale by its deviation" in {
    val adv = GrpoMath.advantages(Array(1.0, 0.0, 0.0, 0.0)).get
    adv.sum shouldBe 0.0 +- 1e-9
    adv(0) should be > 0.0
    adv(1) should be < 0.0
  }

  it should "reject a group whose rewards are all identical" in {
    // The no-click slate: zero variance, undefined advantage. Dividing by a floor would not
    // produce a small gradient, it would produce noise amplified by 1/floor.
    GrpoMath.advantages(Array(0.0, 0.0, 0.0)) shouldBe None
  }

  it should "reject a group smaller than two" in {
    GrpoMath.advantages(Array(1.0)) shouldBe None
    GrpoMath.advantages(Array.empty[Double]) shouldBe None
  }

  "kl" should "be zero for identical distributions" in {
    val p = GrpoMath.softmax(Array(0.5, 1.5, 2.0), 1.0)
    GrpoMath.kl(p, p) shouldBe 0.0 +- 1e-12
  }

  it should "be positive and finite for differing distributions" in {
    val p = GrpoMath.softmax(Array(0.0, 3.0), 1.0)
    val q = GrpoMath.softmax(Array(3.0, 0.0), 1.0)
    GrpoMath.kl(p, q) should be > 0.0
    GrpoMath.kl(p, q).isInfinite shouldBe false
  }

  "gradient" should "match a finite-difference approximation of the loss" in {
    val x = Array(Array(1.0, 0.2), Array(1.0, 0.9), Array(1.0, 0.4))
    val logged = Array(0.3, 0.6, 0.1)
    val w = Array(0.05, -0.3)
    val adv = GrpoMath.advantages(Array(1.0, 0.0, 0.0)).get

    val snapshot = Array(0.2, 0.5, 0.1)
    val analytic = GrpoMath.gradient(x, snapshot, logged, w, adv, cfg)
    val h = 1e-6
    val numeric = w.indices.map { i =>
      val up = w.clone(); up(i) += h
      val down = w.clone(); down(i) -= h
      (GrpoMath.loss(x, snapshot, logged, up, adv, cfg) -
        GrpoMath.loss(x, snapshot, logged, down, adv, cfg)) / (2 * h)
    }.toArray

    analytic.zip(numeric).foreach { case (a, n) => a shouldBe n +- 1e-4 }
  }

  it should "be inert when the policy equals the snapshot and the advantage is zero" in {
    val x = Array(Array(1.0, 0.2), Array(1.0, 0.9))
    val g = GrpoMath.gradient(x, Array(0.1, 0.1), Array(0.1, 0.1), Array(0.0, 0.0), Array(0.0, 0.0), cfg)
    g.foreach(_ shouldBe 0.0 +- 1e-12)
  }

  it should "keep the ratio and the KL on separate references" in {
    // The whole shadow-mode correctness argument: moving the KL anchor alone must change the
    // gradient. If it does not, the two references have been collapsed into one somewhere.
    val x = Array(Array(1.0, 0.2), Array(1.0, 0.9))
    val w = Array(0.05, -0.3)
    val adv = GrpoMath.advantages(Array(1.0, 0.0)).get
    val snapshot = Array(0.3, 0.3)
    val a = GrpoMath.gradient(x, snapshot, Array(0.3, 0.3), w, adv, cfg)
    val b = GrpoMath.gradient(x, snapshot, Array(2.0, -1.0), w, adv, cfg)
    a.toSeq should not be b.toSeq
  }
}
