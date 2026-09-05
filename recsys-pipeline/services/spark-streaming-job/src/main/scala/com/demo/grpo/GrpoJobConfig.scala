package com.demo.grpo

/** Job knobs, read once at start.
  *
  * Follows SequenceJobConfig: only an explicitly parseable value counts, and anything else falls
  * back to the default. A typo in a hyperparameter must not silently change the objective.
  */
final case class GrpoJobConfig(
    hyper: GrpoHyperParams,
    featureVersion: String,
    dim: Int,
    redisHost: String,
    redisPort: Int,
    weightsKey: String)

object GrpoJobConfig {

  /** Must match GrpoFeatures.DIM and GrpoFeatures.VERSION on the Java side. */
  val FeatureVersion = "v2"
  val Dim = 9
  val WeightsKey = "grpo:policy:weights"

  private def doubleFrom(env: Map[String, String], key: String, default: Double): Double =
    env.get(key).flatMap(v => try Some(v.toDouble) catch { case _: NumberFormatException => None })
      .getOrElse(default)

  private def intFrom(env: Map[String, String], key: String, default: Int): Int =
    env.get(key).flatMap(v => try Some(v.toInt) catch { case _: NumberFormatException => None })
      .getOrElse(default)

  /** Finite and strictly positive, or the default. `"Infinity"` parses, so the finite check earns
    * its keep. (Scala 2.12 has no Double#isFinite; java.lang.Double.isFinite is the equivalent.)
    */
  private def positiveOr(env: Map[String, String], key: String, default: Double): Double = {
    val value = doubleFrom(env, key, default)
    if (value > 0.0 && java.lang.Double.isFinite(value)) value else default
  }

  def from(env: Map[String, String]): GrpoJobConfig = {
    // Sign matters as much as magnitude here. A negative learning rate is gradient ASCENT: the job
    // would run, log healthy batches, and converge on the worst policy it can find. A negative
    // clip range inverts the PPO min() so the clipped branch is always selected, and a negative KL
    // beta pays the policy to diverge from the logged one. None of the three is detectable
    // downstream, so each falls back to its default exactly as temperature does.
    val klBeta = doubleFrom(env, "GRPO_KL_BETA", 0.02)
    GrpoJobConfig(
      hyper = GrpoHyperParams(
        temperature  = positiveOr(env, "GRPO_TEMPERATURE", 1.0),
        clipEpsilon  = positiveOr(env, "GRPO_CLIP_EPSILON", 0.2),
        // Zero is allowed here and nowhere else: it turns the KL anchor off, which is a real
        // configuration. Zero clipping or zero learning rate would just disable the objective.
        klBeta       = if (klBeta >= 0.0 && java.lang.Double.isFinite(klBeta)) klBeta else 0.02,
        learningRate = positiveOr(env, "GRPO_LEARNING_RATE", 0.01),
        // Below two, the ratio is identically 1 on every step and clipping never engages.
        innerEpochs  = math.max(2, intFrom(env, "GRPO_INNER_EPOCHS", 4))),
      featureVersion = FeatureVersion,
      dim            = Dim,
      redisHost      = env.getOrElse("REDIS_HOST", "localhost"),
      redisPort      = intFrom(env, "REDIS_PORT", 6379),
      weightsKey     = WeightsKey)
  }

  def fromEnv(): GrpoJobConfig = from(sys.env)
}
