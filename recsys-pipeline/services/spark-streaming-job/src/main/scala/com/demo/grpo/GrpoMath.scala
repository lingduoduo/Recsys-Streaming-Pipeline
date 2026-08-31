package com.demo.grpo

/** Hyperparameters of the surrogate objective. See GrpoJobConfig for how they are read. */
final case class GrpoHyperParams(
    temperature: Double,
    clipEpsilon: Double,
    klBeta: Double,
    learningRate: Double,
    innerEpochs: Int)

/** The GRPO learning rule as pure functions.
  *
  * The action space of a group is the slate: finite, small, and fully enumerated in the logged
  * event. So the softmax partition function is computable and the KL term is EXACT, rather than
  * the k3 estimator language-model implementations are forced into.
  *
  * The policy is linear in the features, so the gradient is analytic and no autodiff library is
  * needed:  d log pi_i / dw = (x_i - sum_j pi_j x_j) / temperature.
  */
object GrpoMath {

  /** Guards the advantage denominator. Deliberately not configurable: a group that needs a larger
    * floor is a degenerate group, which `advantages` rejects outright instead. */
  val AdvantageFloor: Double = 1e-8

  def softmax(logits: Array[Double], temperature: Double): Array[Double] = {
    val scaled = logits.map(_ / temperature)
    val max = scaled.max                 // subtract before exp, or large logits overflow
    val exp = scaled.map(v => math.exp(v - max))
    val total = exp.sum
    exp.map(_ / total)
  }

  /** Group-relative advantage, or None when the group cannot produce one.
    *
    * A slate with fewer than two items has no group to be relative to. A slate whose rewards are
    * all identical -- the ordinary no-click case -- has zero variance, and normalizing it would
    * amplify floating-point noise by 1/AdvantageFloor rather than yield a small gradient.
    */
  def advantages(rewards: Array[Double]): Option[Array[Double]] = {
    if (rewards.length < 2) return None
    val mean = rewards.sum / rewards.length
    val variance = rewards.map(r => (r - mean) * (r - mean)).sum / rewards.length
    val std = math.sqrt(variance)
    if (std < AdvantageFloor) None
    else Some(rewards.map(r => (r - mean) / std))
  }

  /** Exact KL(p || q) over the enumerated slate. */
  def kl(p: Array[Double], q: Array[Double]): Double =
    p.indices.foldLeft(0.0) { (acc, i) =>
      if (p(i) <= 0.0) acc
      else acc + p(i) * math.log(p(i) / math.max(q(i), AdvantageFloor))
    }

  private def logits(x: Array[Array[Double]], w: Array[Double]): Array[Double] =
    x.map(row => row.indices.foldLeft(0.0)((acc, i) => acc + row(i) * w(i)))

  /** The clipped surrogate plus the KL penalty, averaged over the group.
    *
    * TWO references, deliberately not one:
    *
    *   `snapshotLogits` — the policy frozen at the start of the micro-batch. The ratio is measured
    *     against this, so inner epochs see a ratio that departs from 1 and clipping engages.
    *   `loggedLogits`   — the behavior policy that actually served the slate. The KL anchors here,
    *     bounding how far the policy drifts from what is live.
    *
    * Collapsing them would be a silent failure in shadow mode, where serving never changes: the
    * ratio would grow without bound, clipping would latch permanently active, and the gradient
    * would go to zero while every batch still looked healthy.
    */
  def loss(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double],
           w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Double = {
    val piSnap = softmax(snapshotLogits, cfg.temperature)
    val piOld = softmax(loggedLogits, cfg.temperature)
    val pi = softmax(logits(x, w), cfg.temperature)
    val surrogate = pi.indices.map { i =>
      val ratio = pi(i) / math.max(piSnap(i), AdvantageFloor)
      val clipped = math.max(1.0 - cfg.clipEpsilon, math.min(1.0 + cfg.clipEpsilon, ratio))
      math.min(ratio * adv(i), clipped * adv(i))
    }.sum / pi.length
    -surrogate + cfg.klBeta * kl(pi, piOld)
  }

  /** Analytic gradient of `loss` with respect to w.
    *
    * Derivation: with z_i = w.x_i / temperature, the softmax Jacobian gives
    *   d pi_i / dw_d = (pi_i / temperature) * (x_i,d - E_pi[x_d]).
    * So for any term of the loss expressed as a function of pi, dL/dw_d = sum_i (dL/dpi_i) *
    * d pi_i/dw_d. Each candidate's contribution below is therefore built as `dL/dpi_i` (not
    * dL/d log pi_i -- that would double-count a factor of pi_i) and only multiplied by pi_i once,
    * together with the shared 1/temperature factor.
    */
  def gradient(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double],
               w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double] = {
    val dim = w.length
    val piSnap = softmax(snapshotLogits, cfg.temperature)
    val piOld = softmax(loggedLogits, cfg.temperature)
    val pi = softmax(logits(x, w), cfg.temperature)

    // Expected feature vector under pi -- the term that makes d log pi_i / dw a centred difference.
    val expected = Array.fill(dim)(0.0)
    pi.indices.foreach(i => (0 until dim).foreach(d => expected(d) += pi(i) * x(i)(d)))

    val grad = Array.fill(dim)(0.0)
    pi.indices.foreach { i =>
      val ratio = pi(i) / math.max(piSnap(i), AdvantageFloor)   // ratio: snapshot reference
      val clippedActive =
        ratio < 1.0 - cfg.clipEpsilon || ratio > 1.0 + cfg.clipEpsilon
      // Outside the clip range the surrogate is flat in w, so it contributes no gradient --
      // unless the unclipped branch is the smaller one, which is when min() selects it.
      val unclippedSelected = !clippedActive ||
        (ratio * adv(i)) < (math.max(1.0 - cfg.clipEpsilon, math.min(1.0 + cfg.clipEpsilon, ratio)) * adv(i))
      // d(-1/N * ratio_i * adv_i)/dpi_i = -(1/N) * adv_i / piSnap_i -- NOT -(1/N) * adv_i * ratio_i,
      // which would carry a spurious extra factor of pi_i once multiplied through below.
      val surrogateScale =
        if (unclippedSelected) -adv(i) / (math.max(piSnap(i), AdvantageFloor) * pi.length) else 0.0
      val klScale = cfg.klBeta * (math.log(math.max(pi(i), AdvantageFloor) /
        math.max(piOld(i), AdvantageFloor)) + 1.0)
      val scale = (surrogateScale + klScale) * pi(i) / cfg.temperature
      (0 until dim).foreach(d => grad(d) += scale * (x(i)(d) - expected(d)))
    }
    grad
  }
}
