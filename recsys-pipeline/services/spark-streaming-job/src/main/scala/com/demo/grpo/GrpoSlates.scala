package com.demo.grpo

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.storage.StorageLevel

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

  /** Column the gate reason is tagged under. Package-visible so the spec can assert that dropped
    * slates are filtered out before the collect, which is otherwise unobservable. */
  private[grpo] val DropReasonColumn = "grpo_drop_reason"

  /** The first gate a slate fails, or None when it survives all three.
    *
    * The single definition of the gates. Both the Spark-side filter and the driver-side group
    * builder go through this, so the two cannot drift -- which matters now that the counts come
    * from one side and the groups from the other: a disagreement would report a batch the job did
    * not train on. Precedence is size, then feature version, then reward variance, and a slate
    * failing several is counted under the first.
    *
    * Takes the two primitives the gates read rather than a GrpoJobConfig, so the UDF below closes
    * over a String and an Int instead of shipping the Redis host, port and hyperparameters to
    * every executor.
    */
  private[grpo] def dropReason(items: Seq[Row], featureVersion: String,
                               dim: Int): Option[String] = {
    if (items == null || items.size < 2) return Some(TooSmall)
    if (featureVectors(items, featureVersion, dim).exists(_.isEmpty)) return Some(BadFeatureVersion)
    if (GrpoMath.advantages(rewardsOf(items)).isEmpty) return Some(ZeroVariance)
    None
  }

  private def featureVectors(items: Seq[Row], featureVersion: String,
                             dim: Int): Seq[Option[Array[Double]]] = items.map { item =>
    val features = item.getAs[Map[String, String]]("item_features")
    val packed = if (features == null) null else features.getOrElse("grpo_x", null)
    parseFeatureVector(packed, featureVersion, dim)
  }

  private def rewardsOf(items: Seq[Row]): Array[Double] = items.map { item =>
    if (item.isNullAt(item.fieldIndex("label"))) 0.0 else item.getAs[Double]("label")
  }.toArray

  /** `slate_id`, `items`, and the gate reason -- null for a slate that survives. */
  private[grpo] def tagged(slates: DataFrame, cfg: GrpoJobConfig): DataFrame = {
    val featureVersion = cfg.featureVersion
    val dim = cfg.dim
    val reason = udf((items: Seq[Row]) => dropReason(items, featureVersion, dim).orNull)
    slates.select(col("slate_id"), col("items"))
      .withColumn(DropReasonColumn, reason(col("items")))
  }

  /** Parse slates into groups, gating in Spark so only survivors reach the driver.
    *
    * PREVIOUSLY A KNOWN SCALING LIMITATION: the gates ran on the driver, after `collect()` had
    * already pulled every slate's feature matrix across. At the click-through this job sees, the
    * variance gate rejects most of what it was handed -- a measured 5,000-slate batch with 5%
    * engaged keeps 250 -- so the driver was holding twenty times the data it would use, and worse
    * as click-through falls. Gating first makes the collect carry survivors only.
    *
    * The design spec's `treeAggregate` -- summing per-slate gradient contributions in Spark so no
    * slate vector ever reaches the driver -- is still not done, for the reasons it always was not:
    * it forces the gate counts onto Spark accumulators, which only settle after an action; it
    * turns `applyBatch` from a pure Array function into one that broadcasts the weights and
    * launches a job per inner epoch; and it rewrites this file's spec and
    * GrpoPolicyStreamingJobSpec, which today test the learning rule with no Spark session at all.
    * What remains bounded by memory is the cached tagged frame, which lives on the cluster rather
    * than the driver -- so raising MAX_OFFSETS_PER_TRIGGER now hits the cluster's ceiling.
    */
  def toGroups(slates: DataFrame, cfg: GrpoJobConfig): (Seq[GrpoGroup], GateCounts) = {
    // Cached because both actions below read it: without this the parse and every gate would run
    // twice, which would trade the driver's memory for double the executor work.
    val frame = tagged(slates, cfg).persist(StorageLevel.MEMORY_AND_DISK)
    try {
      // At most four small rows reach the driver here; no slate data does.
      val byReason = frame.groupBy(col(DropReasonColumn)).count().collect()
        .map(row => (Option(row.getString(0)), row.getLong(1))).toMap
      val kept = frame.filter(col(DropReasonColumn).isNull).select("slate_id", "items")
        .collect().map(row => buildGroup(row, cfg)).toSeq
      (kept, GateCounts(
        kept = byReason.getOrElse(None, 0L),
        tooSmall = byReason.getOrElse(Some(TooSmall), 0L),
        zeroVariance = byReason.getOrElse(Some(ZeroVariance), 0L),
        badFeatureVersion = byReason.getOrElse(Some(BadFeatureVersion), 0L)))
    } finally frame.unpersist()
  }

  /** The group for a slate already known to pass every gate, so every parse here succeeds. */
  private def buildGroup(row: Row, cfg: GrpoJobConfig): GrpoGroup = {
    val items = row.getSeq[Row](1)
    val x = featureVectors(items, cfg.featureVersion, cfg.dim).map(_.get).toArray
    val logged = items.map { item =>
      val features = item.getAs[Map[String, String]]("item_features")
      try features.getOrElse("prediction_score", "0.0").toDouble
      catch { case _: NumberFormatException => 0.0 }
    }.toArray
    GrpoGroup(row.getString(0), x, logged, rewardsOf(items))
  }
}
