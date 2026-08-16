package com.demo.event

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, col, lit, when}

/** The outcome of a gate: the tagged frame plus the reason order it was declared with.
  *
  * Pure DataFrame algebra — nothing here logs. `DropMetrics` renders and emits.
  *
  * The caller owns persistence. `counts` is an action, so on an unpersisted frame it recomputes
  * the upstream parse that `kept` will then recompute again; persist `tagged` wherever the
  * upstream is not already persisted, and unpersist in a `finally`, as `ExecutionEngine` does.
  */
final case class Gated(tagged: DataFrame, reasons: Seq[String]) {

  def kept: DataFrame =
    tagged.filter(col(FieldGate.ReasonColumn).isNull).drop(FieldGate.ReasonColumn)

  def rejected: DataFrame = tagged.filter(col(FieldGate.ReasonColumn).isNotNull)

  /** `(kept, per-reason counts)` from a single shuffle, reasons in declared order with zeros kept.
    *
    * One `groupBy` over the tagged frame yields both halves, so they cannot disagree across a
    * recompute the way two separate `count()` calls could. */
  def counts: (Long, Seq[(String, Long)]) = {
    val tallies = tagged
      .groupBy(col(FieldGate.ReasonColumn)).count().collect()
      .map(row => (if (row.isNullAt(0)) None else Some(row.getString(0))) -> row.getLong(1))
      .toMap
    (tallies.getOrElse(None, 0L), reasons.map(reason => reason -> tallies.getOrElse(Some(reason), 0L)))
  }
}

/** Splits a frame by an ordered list of rejection rules, counting why each row was dropped.
  *
  * Rules are phrased as rejection conditions — `"null_user_id" -> col("user_id").isNull` — the
  * same shape as the `rejection_reason` chain in `UserBehaviorProfileBatchJob`.
  */
object FieldGate {

  val ReasonColumn = "rejection_reason"

  /** Tag each row with the **first** rule it violates; null means it passed every rule.
    *
    * First-match means one reason per dropped row, so the per-reason counts sum exactly to the
    * drop total and read as a partition of the input.
    *
    * Each condition is wrapped in `coalesce(_, false)` so a null-valued predicate cannot leak an
    * unknown into the chain and silently reject a row — the guard `LateFeedbackJoin` already
    * applies to its own due computation.
    */
  def apply(df: DataFrame, rules: Seq[(String, Column)]): Gated = {
    require(rules.nonEmpty, "a gate needs at least one rule")
    val chain = rules.foldLeft(when(lit(false), lit(null).cast("string"))) {
      case (acc, (reason, rejectWhen)) => acc.when(coalesce(rejectWhen, lit(false)), lit(reason))
    }
    Gated(df.withColumn(ReasonColumn, chain), rules.map(_._1))
  }
}
