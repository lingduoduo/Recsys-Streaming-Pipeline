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
  def reasons: Seq[(String, Long)] =
    Seq(GrpoSlates.TooSmall -> tooSmall, GrpoSlates.ZeroVariance -> zeroVariance,
      GrpoSlates.BadFeatureVersion -> badFeatureVersion)
}

object GrpoSlates {

  /** Drop reasons, as DropMetrics reports them. */
  val TooSmall = "slate_too_small"
  val BadFeatureVersion = "bad_feature_version"
  val ZeroVariance = "zero_reward_variance"

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

  /** KNOWN SCALING LIMITATION: this collects the whole micro-batch to the driver.
    *
    * The design spec calls for summing the per-slate gradient contributions with `treeAggregate`
    * "so the driver never collects per-slate vectors". That is not what happens: `collect()` here
    * pulls every surviving slate's feature matrix into driver memory, and
    * `GrpoPolicyStreamingJob.applyBatch` then folds over them there. With MAX_OFFSETS_PER_TRIGGER
    * at its 5000 default and a ten-item slate of nine doubles, a batch is on the order of a few MB,
    * so the driver holds it comfortably — but the ceiling is the driver's heap, not the cluster's,
    * and raising the trigger size is the operation that hits it.
    *
    * Deliberately not migrated. Doing it properly means returning a distributed collection instead
    * of a Seq, which forces the gate counts onto Spark accumulators (they only settle after an
    * action), turns `applyBatch` from a pure Array function into one that broadcasts the weights
    * and launches a job per inner epoch, and rewrites both this file's spec and
    * GrpoPolicyStreamingJobSpec, which construct `Seq[GrpoGroup]` directly and today need no Spark
    * session to test the learning rule at all. That is a larger change than the scaling headroom
    * currently justifies; revisit it when a batch actually outgrows the driver.
    */
  def toGroups(slates: DataFrame, cfg: GrpoJobConfig): (Seq[GrpoGroup], GateCounts) = {
    val classified = slates.select("slate_id", "items").collect().map(row => classify(row, cfg))
    val kept = classified.collect { case Right(group) => group }.toSeq
    val dropped = classified.collect { case Left(reason) => reason }
      .groupBy(identity).map { case (reason, hits) => reason -> hits.length.toLong }
    (kept, GateCounts(
      kept = kept.size.toLong,
      tooSmall = dropped.getOrElse(TooSmall, 0L),
      zeroVariance = dropped.getOrElse(ZeroVariance, 0L),
      badFeatureVersion = dropped.getOrElse(BadFeatureVersion, 0L)))
  }

  /** The group a slate row yields, or the first gate it fails: size, feature version, variance. */
  private def classify(row: Row, cfg: GrpoJobConfig): Either[String, GrpoGroup] = {
    val items = row.getSeq[Row](1)
    if (items.size < 2) return Left(TooSmall)
    val features = items.map(_.getAs[Map[String, String]]("item_features"))
    val x = features.map(f => parseFeatureVector(f.getOrElse("grpo_x", null), cfg.featureVersion, cfg.dim))
    if (x.exists(_.isEmpty)) return Left(BadFeatureVersion)
    val rewards = items.map { item =>
      if (item.isNullAt(item.fieldIndex("label"))) 0.0 else item.getAs[Double]("label")
    }.toArray
    if (GrpoMath.advantages(rewards).isEmpty) return Left(ZeroVariance)
    val logged = features.map { f =>
      try f.getOrElse("prediction_score", "0.0").toDouble catch { case _: NumberFormatException => 0.0 }
    }.toArray
    Right(GrpoGroup(row.getString(0), x.map(_.get).toArray, logged, rewards))
  }
}
