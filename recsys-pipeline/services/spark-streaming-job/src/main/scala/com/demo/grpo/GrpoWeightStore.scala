package com.demo.grpo

import scala.jdk.CollectionConverters._

final case class GrpoWeights(
    weights: Array[Double],
    featureVersion: String,
    batchId: Long,
    slatesApplied: Long)

/** The policy's durable form.
  *
  * A restart resumes training rather than resetting the policy, so the weights live in Redis with
  * no TTL. The feature version travels with them: weights fit against one feature layout applied
  * to another produce a plausible-looking score that means nothing, and nothing downstream could
  * detect it. Decode refuses instead.
  */
object GrpoWeightStore {

  def initial(cfg: GrpoJobConfig): GrpoWeights =
    GrpoWeights(Array.fill(cfg.dim)(0.0), cfg.featureVersion, -1L, 0L)

  def encode(w: GrpoWeights, nowMs: Long): java.util.Map[String, String] =
    Map(
      "weights" -> w.weights.mkString(","),
      "dim" -> w.weights.length.toString,
      "feature_version" -> w.featureVersion,
      "updated_at" -> nowMs.toString,
      "batch_id" -> w.batchId.toString,
      "slates_applied" -> w.slatesApplied.toString
    ).asJava

  def decode(fields: Map[String, String], cfg: GrpoJobConfig): Either[String, GrpoWeights] = {
    if (fields.isEmpty) return Right(initial(cfg))
    val version = fields.getOrElse("feature_version", "")
    if (version != cfg.featureVersion)
      return Left(s"stored feature_version '$version' does not match job's '${cfg.featureVersion}'")
    val parts = fields.getOrElse("weights", "").split(",").filter(_.nonEmpty)
    if (parts.length != cfg.dim)
      return Left(s"stored weight width ${parts.length} does not match dim ${cfg.dim}")
    try Right(GrpoWeights(
      parts.map(_.toDouble), version,
      fields.getOrElse("batch_id", "-1").toLong,
      fields.getOrElse("slates_applied", "0").toLong))
    catch { case e: NumberFormatException => Left(s"unparseable stored weights: ${e.getMessage}") }
  }
}
