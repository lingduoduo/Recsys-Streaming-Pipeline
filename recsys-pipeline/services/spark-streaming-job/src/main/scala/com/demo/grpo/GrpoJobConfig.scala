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
  val FeatureVersion = "v1"
  val Dim = 10
  val WeightsKey = "grpo:policy:weights"

  private def doubleFrom(env: Map[String, String], key: String, default: Double): Double =
    env.get(key).flatMap(v => try Some(v.toDouble) catch { case _: NumberFormatException => None })
      .getOrElse(default)

  private def intFrom(env: Map[String, String], key: String, default: Int): Int =
    env.get(key).flatMap(v => try Some(v.toInt) catch { case _: NumberFormatException => None })
      .getOrElse(default)

  def from(env: Map[String, String]): GrpoJobConfig = {
    val temperature = doubleFrom(env, "GRPO_TEMPERATURE", 1.0)
    GrpoJobConfig(
      hyper = GrpoHyperParams(
        temperature  = if (temperature > 0.0) temperature else 1.0,
        clipEpsilon  = doubleFrom(env, "GRPO_CLIP_EPSILON", 0.2),
        klBeta       = doubleFrom(env, "GRPO_KL_BETA", 0.02),
        learningRate = doubleFrom(env, "GRPO_LEARNING_RATE", 0.01),
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
