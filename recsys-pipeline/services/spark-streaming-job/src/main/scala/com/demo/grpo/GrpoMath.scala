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

  /** w . x_i for each candidate. */
  def logits(x: Array[Array[Double]], w: Array[Double]): Array[Double] =
    x.map(row => row.indices.foldLeft(0.0)((acc, i) => acc + row(i) * w(i)))

  /** (pi, piSnap, piOld): the current, batch-snapshot, and logged policies over one group. */
  private def policies(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double],
                       w: Array[Double], cfg: GrpoHyperParams): (Array[Double], Array[Double], Array[Double]) =
    (softmax(logits(x, w), cfg.temperature),
     softmax(snapshotLogits, cfg.temperature),
     softmax(loggedLogits, cfg.temperature))

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
    val (pi, piSnap, piOld) = policies(x, snapshotLogits, loggedLogits, w, cfg)
    val surrogate = pi.indices.map { i =>
      val ratio = pi(i) / math.max(piSnap(i), AdvantageFloor)
      val clipped = math.max(1.0 - cfg.clipEpsilon, math.min(1.0 + cfg.clipEpsilon, ratio))
      math.min(ratio * adv(i), clipped * adv(i))
    }.sum / pi.length
    -surrogate + cfg.klBeta * kl(pi, piOld)
  }

  /** Analytic gradient of `loss` with respect to w.
    *
    * With z_i = w.x_i / temperature, the softmax Jacobian gives
    *   d pi_i / dw = (pi_i / temperature) * (x_i - E_pi[x]),
    * so dL/dw = sum_i (dL/dpi_i) * d pi_i / dw. Each candidate's contribution is built as dL/dpi_i
    * (not dL/d log pi_i, which would double-count a factor of pi_i) and multiplied by pi_i once.
    * Because sum_i pi_i (x_i - E_pi[x]) = 0, any part of dL/dpi_i that is the same for every
    * candidate drops out; the KL derivative log(pi_i / piOld_i) + 1 therefore contributes only
    * its log.
    */
  def gradient(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double],
               w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double] =
    gradientFromPolicies(x, softmax(snapshotLogits, cfg.temperature),
                         softmax(loggedLogits, cfg.temperature), w, adv, cfg)

  /** `gradient` with the two fixed reference POLICIES supplied already softmaxed.
    *
    * `piSnap` and `piOld` are distributions over the slate, not logits -- both are
    * `Array[Double]`, so the compiler cannot tell them apart and a caller passing logits here
    * would get a silently wrong gradient. Package-visible for that reason: the one production
    * caller is `GrpoPolicyStreamingJob.applyBatch`, which holds both references fixed for a whole
    * micro-batch and would otherwise re-derive them on every inner epoch. Only `pi` depends on
    * `w`, so only `pi` is computed here.
    */
  private[grpo] def gradientFromPolicies(
      x: Array[Array[Double]], piSnap: Array[Double], piOld: Array[Double],
      w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double] = {
    val dim = w.length
    val pi = softmax(logits(x, w), cfg.temperature)

    // Expected feature vector under pi -- the term that makes d log pi_i / dw a centred difference.
    val expected = Array.fill(dim)(0.0)
    pi.indices.foreach(i => (0 until dim).foreach(d => expected(d) += pi(i) * x(i)(d)))

    val grad = Array.fill(dim)(0.0)
    pi.indices.foreach { i =>
      val piSnapFloored = math.max(piSnap(i), AdvantageFloor)
      val ratio = pi(i) / piSnapFloored                          // ratio: snapshot reference
      val clipped = math.max(1.0 - cfg.clipEpsilon, math.min(1.0 + cfg.clipEpsilon, ratio))
      // The surrogate moves with w only when min() kept the raw ratio; the clip bound is flat.
      val unclippedSelected = ratio * adv(i) <= clipped * adv(i)
      val surrogateScale = if (unclippedSelected) -adv(i) / (piSnapFloored * pi.length) else 0.0
      val klScale = cfg.klBeta *
        math.log(math.max(pi(i), AdvantageFloor) / math.max(piOld(i), AdvantageFloor))
      val scale = (surrogateScale + klScale) * pi(i) / cfg.temperature
      (0 until dim).foreach(d => grad(d) += scale * (x(i)(d) - expected(d)))
    }
    grad
  }
}
