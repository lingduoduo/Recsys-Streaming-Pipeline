package com.demo.grpo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoMathSpec extends AnyFlatSpec with Matchers {

  private val cfg = GrpoHyperParams(
    temperature = 1.0, clipEpsilon = 0.2, klBeta = 0.02, learningRate = 0.01, innerEpochs = 4)

  // Central differences at h = 1e-6 are accurate to about 1e-10 here (rounding error ~ machine
  // epsilon / h dominates the O(h^2) truncation error) -- tight enough to catch a wrong constant
  // factor (the original bug was off by ~3x) but not so tight that ordinary floating-point noise
  // fails the build.
  private val gradTol = 1e-10

  private def numericGradient(x: Array[Array[Double]], snapshot: Array[Double], logged: Array[Double],
                               w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double] = {
    val h = 1e-6
    w.indices.map { i =>
      val up = w.clone(); up(i) += h
      val down = w.clone(); down(i) -= h
      (GrpoMath.loss(x, snapshot, logged, up, adv, cfg) -
        GrpoMath.loss(x, snapshot, logged, down, adv, cfg)) / (2 * h)
    }.toArray
  }

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

    analytic.zip(numeric).foreach { case (a, n) => a shouldBe n +- gradTol }
  }

  it should "match a finite-difference approximation with multiple informative feature columns" in {
    // The fixture above has a constant bias column (x_i0 = 1.0 for every i), so x_i0 - E[x_0] is
    // identically zero and grad(0) is checked only against 0 by construction -- only grad(1) is
    // actually exercised. Here both columns vary across candidates, so both gradient components
    // depend on w in a nontrivial way and are both genuinely constrained by the comparison below.
    val x = Array(Array(0.3, 0.9), Array(0.8, 0.2), Array(0.5, 0.6))
    val snapshot = Array(0.1, 0.4, -0.2)
    val logged = Array(0.2, -0.1, 0.3)
    val w = Array(0.4, -0.25)
    val adv = GrpoMath.advantages(Array(1.0, 0.0, 0.0)).get

    val analytic = GrpoMath.gradient(x, snapshot, logged, w, adv, cfg)
    val numeric = numericGradient(x, snapshot, logged, w, adv, cfg)
    analytic.zip(numeric).foreach { case (a, n) => a shouldBe n +- gradTol }
  }

  it should "match a finite-difference approximation at a non-unit temperature" in {
    // Every other test in this file uses temperature = 1.0, under which the temperature divisor
    // in d log pi_i / dw is a no-op -- deleting it, or squaring it, would still pass. This pins
    // gradient's use of temperature against loss's, at a temperature where it actually matters.
    val x = Array(Array(0.3, 0.9), Array(0.8, 0.2), Array(0.5, 0.6))
    val snapshot = Array(0.1, 0.4, -0.2)
    val logged = Array(0.2, -0.1, 0.3)
    val w = Array(0.4, -0.25)
    val adv = GrpoMath.advantages(Array(1.0, 0.0, 0.0)).get
    val halfTempCfg = cfg.copy(temperature = 0.5)

    val analytic = GrpoMath.gradient(x, snapshot, logged, w, adv, halfTempCfg)
    val numeric = numericGradient(x, snapshot, logged, w, adv, halfTempCfg)
    analytic.zip(numeric).foreach { case (a, n) => a shouldBe n +- gradTol }
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

  it should "keep the KL and the ratio on separate references, varied the other way" in {
    // The mirror of the test above: this time snapshot moves and loggedLogits is held fixed.
    // Collapsing both `loss` and `gradient` onto loggedLogits (i.e. measuring the ratio against
    // the logged policy instead of the snapshot) passes every other test in this file, including
    // the one above, since that one only ever varies loggedLogits. This is the shadow-mode failure
    // the two-reference signature exists to prevent: the ratio must react to snapshot moving even
    // while the KL anchor stays put.
    val x = Array(Array(1.0, 0.2), Array(1.0, 0.9))
    val w = Array(0.05, -0.3)
    val adv = GrpoMath.advantages(Array(1.0, 0.0)).get
    val logged = Array(0.3, 0.3)
    val a = GrpoMath.gradient(x, Array(0.3, 0.3), logged, w, adv, cfg)
    val b = GrpoMath.gradient(x, Array(2.0, -1.0), logged, w, adv, cfg)
    a.toSeq should not be b.toSeq
  }

  "loss" should "cap the surrogate for a candidate whose ratio clears the clip band with positive advantage" in {
    // The canonical PPO assertion, pinned to a hand-computed value rather than checked only for
    // gradient/loss consistency: a consistently-wrong pair (e.g. max() instead of min() in both
    // `loss` and `gradient`) would still pass every finite-difference test in this file, since
    // finite differences only ever compare the two functions to each other.
    //
    // One-hot features make logits(x, w) == w exactly, so pi is under direct control here.
    val x = Array(Array(1.0, 0.0), Array(0.0, 1.0))
    val w = Array(2.0, 0.0)
    val snapshot = Array(0.0, 0.0)
    val logged = Array(0.3, -0.9)
    val adv = GrpoMath.advantages(Array(1.0, 0.0)).get // exactly [1.0, -1.0]

    val pi = GrpoMath.softmax(w, cfg.temperature)
    val piSnap = GrpoMath.softmax(snapshot, cfg.temperature)
    val piOld = GrpoMath.softmax(logged, cfg.temperature)
    val ratio0 = pi(0) / piSnap(0)
    val ratio1 = pi(1) / piSnap(1)

    // Confirm the fixture actually reaches the branch the brief's fixture never hit: candidate 0
    // has positive advantage and a ratio past the clip band's *high* side.
    adv(0) should be > 0.0
    ratio0 should be > (1.0 + cfg.clipEpsilon)
    adv(1) should be < 0.0
    ratio1 should be < (1.0 - cfg.clipEpsilon)

    // Both candidates are clipped, so the surrogate uses the clip bound, not the raw ratio, for
    // each of them: (1+eps)*adv(0) for candidate 0, (1-eps)*adv(1) for candidate 1.
    val expectedSurrogate =
      ((1.0 + cfg.clipEpsilon) * adv(0) + (1.0 - cfg.clipEpsilon) * adv(1)) / 2.0
    val expectedLoss = -expectedSurrogate + cfg.klBeta * GrpoMath.kl(pi, piOld)

    GrpoMath.loss(x, snapshot, logged, w, adv, cfg) shouldBe expectedLoss +- 1e-12
  }

  "gradient" should "match a finite-difference approximation once the clip band is engaged with a positive advantage" in {
    // Same fixture as the loss test above, where `loss` is now pinned to a hand-derived value
    // independent of gradient. Checking gradient against loss's numeric derivative here, on top of
    // that ground truth, closes the gap a pure consistency check leaves open: it confirms gradient
    // reproduces the same (now independently verified correct) capped behaviour, not merely that
    // the two functions agree with each other while both being wrong.
    val x = Array(Array(1.0, 0.0), Array(0.0, 1.0))
    val w = Array(2.0, 0.0)
    val snapshot = Array(0.0, 0.0)
    val logged = Array(0.3, -0.9)
    val adv = GrpoMath.advantages(Array(1.0, 0.0)).get

    val analytic = GrpoMath.gradient(x, snapshot, logged, w, adv, cfg)
    val numeric = numericGradient(x, snapshot, logged, w, adv, cfg)
    analytic.zip(numeric).foreach { case (a, n) => a shouldBe n +- gradTol }
  }
}
