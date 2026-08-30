package com.demo.grpo

import org.apache.spark.sql.{DataFrame, Row}

/** One GRPO group: the candidates of a single slate, with their features, logged logits, rewards. */
final case class GrpoGroup(
    slateId: String,
    x: Array[Array[Double]],
    logged: Array[Double],
    rewards: Array[Double])

/** Why slates were kept or dropped in one micro-batch.
  *
  * At low click-through almost every slate is zero-variance, and that is the expected steady
  * state, not a fault. The surviving fraction has to be visible or an operator cannot tell
  * "learning from few slates" from "learning from none".
  */
final case class GateCounts(kept: Long, tooSmall: Long, zeroVariance: Long, badFeatureVersion: Long) {
  def total: Long = kept + tooSmall + zeroVariance + badFeatureVersion
  def reasons: Seq[(String, Long)] =
    Seq("slate_too_small" -> tooSmall, "zero_reward_variance" -> zeroVariance,
      "bad_feature_version" -> badFeatureVersion)
}

object GrpoSlates {

  /** Parse a packed feature vector, or None if it cannot be trusted to align with the weights. */
  def parseFeatureVector(packed: String, expectedVersion: String, dim: Int): Option[Array[Double]] = {
    if (packed == null) return None
    val separator = packed.indexOf(':')
    if (separator < 0) return None
    if (packed.substring(0, separator) != expectedVersion) return None
    val parts = packed.substring(separator + 1).split(",")
    if (parts.length != dim) return None
    try Some(parts.map(_.toDouble)) catch { case _: NumberFormatException => None }
  }

  def toGroups(slates: DataFrame, cfg: GrpoJobConfig): (Seq[GrpoGroup], GateCounts) = {
    var tooSmall = 0L
    var zeroVariance = 0L
    var badVersion = 0L
    val kept = scala.collection.mutable.ArrayBuffer.empty[GrpoGroup]

    slates.select("slate_id", "items").collect().foreach { row =>
      val slateId = row.getString(0)
      val items = row.getSeq[Row](1)
      if (items.size < 2) {
        tooSmall += 1L
      } else {
        val parsed = items.map { item =>
          val features = item.getAs[Map[String, String]]("item_features")
          val x = parseFeatureVector(features.getOrElse("grpo_x", null), cfg.featureVersion, cfg.dim)
          val logged = try features.getOrElse("prediction_score", "0.0").toDouble
                       catch { case _: NumberFormatException => 0.0 }
          val label = if (item.isNullAt(item.fieldIndex("label"))) 0.0
                      else item.getAs[Double]("label")
          (x, logged, label)
        }
        if (parsed.exists(_._1.isEmpty)) {
          badVersion += 1L
        } else {
          val rewards = parsed.map(_._3).toArray
          GrpoMath.advantages(rewards) match {
            case None => zeroVariance += 1L
            case Some(_) =>
              kept += GrpoGroup(slateId, parsed.map(_._1.get).toArray, parsed.map(_._2).toArray, rewards)
          }
        }
      }
    }
    (kept.toSeq, GateCounts(kept.size.toLong, tooSmall, zeroVariance, badVersion))
  }
}
