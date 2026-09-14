# GRPO Gate Before Collect Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collect only the slates that survive the gates, instead of collecting every slate and then gating on the driver.

**Architecture:** Extract the gate decision into `dropReason`, expose it as a UDF, and let `toGroups` take its counts from a Spark aggregation and its rows from a filtered collect. `classify` becomes `buildGroup`, which shares `dropReason`'s feature-parsing helper, so the Spark side and the driver side cannot disagree about which slates are valid.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17.

**Spec:** [GRPO gate before collect design](../specs/2026-09-14-grpo-gate-before-collect-design.md)

## Global Constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17; no new dependencies.
- Preserve `toGroups(slates: DataFrame, cfg: GrpoJobConfig): (Seq[GrpoGroup], GateCounts)` exactly.
- Preserve the gate precedence: size, then feature version, then reward variance. `GateCounts` must be identical to today's for every input.
- Preserve `GrpoGroup`'s contents and the returned `Seq`'s order, which is the slates' order in the input DataFrame.
- Preserve `parseFeatureVector`, `TooSmall`, `BadFeatureVersion`, `ZeroVariance` and `GateCounts` as they are.
- One definition of the gates: the Spark-side and driver-side paths must call the same code.
- `applyBatch` stays pure over `Array`s; `toGroups` keeps returning a strict `Seq`; neither may require a Spark session to test.

## Environment

sbt must run under JDK 17; the default JDK aborts Spark-session tests with a misleading `getSubject` error. All sbt commands run from `recsys-pipeline/services/spark-streaming-job`:

```bash
export JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home
```

## File Structure

- `src/main/scala/com/demo/grpo/GrpoSlates.scala` — `dropReason` and a shared feature-parsing helper extracted, `classify` replaced by `buildGroup`, `toGroups` reworked. `parseFeatureVector`, the drop-reason constants and `GateCounts` are untouched.
- `src/test/scala/com/demo/grpo/GrpoSlatesSpec.scala` — gains the equivalence test against a frozen copy of the old `toGroups`, the gate-precedence cases, and the collected-row-count assertion.

---

### Task 0: Publish the design first

**Files:**
- Create: `.superpowers/docs/specs/2026-09-14-grpo-gate-before-collect-design.md`
- Create: `.superpowers/docs/plans/2026-09-14-grpo-gate-before-collect.md`

**Interfaces:** Produces the branch and draft PR that Tasks 1-2 commit into. No code.

- [x] **Step 1: Commit the spec and plan on a branch off `master`.**

```bash
git checkout master && git pull
git checkout -b perf/grpo-gate-before-collect
git add .superpowers/docs/specs/2026-09-14-grpo-gate-before-collect-design.md \
        .superpowers/docs/plans/2026-09-14-grpo-gate-before-collect.md
git commit -m "docs: specify gating GRPO slates before the collect"
```

- [x] **Step 2: Push and open a draft PR against `master`** titled `Gate GRPO slates before the collect`, whose body carries the 5,000-to-250 row measurement, the spike result that established the parse/collect path dominates, and the explicit statement that no latency claim is made because that measurement failed. Use `gh pr create --draft --body-file`. Record the PR number here.

---

### Task 1: Gate in Spark, collect only survivors

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoSlates.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoSlatesSpec.scala`

**Interfaces:**
- Consumes: `GrpoSlates.parseFeatureVector`, `GrpoMath.advantages`, and the `items` column of the slate schema (`item_id`, `label`, `item_features`).
- Produces: `private[grpo] def dropReason(items: Seq[Row], featureVersion: String, dim: Int): Option[String]`, called by both the UDF and the driver-side builder; `private[grpo] def tagged(slates: DataFrame, cfg: GrpoJobConfig): DataFrame`; `private[grpo] val DropReasonColumn`. `toGroups` keeps its exact signature. `dropReason` takes the two primitives rather than a `GrpoJobConfig` so the UDF closes over only what the gates read — no fabricated config, and the Redis host, port and hyperparameters never ship to an executor.

- [x] **Step 1: Append the equivalence, precedence and volume tests to `GrpoSlatesSpec.scala`.** `legacyToGroups` is a frozen copy of the current implementation and is the oracle; it must not later be re-pointed at the production function.

```scala
  /** A frozen copy of the collect-then-gate implementation, as the equivalence oracle.
    *
    * Deliberately duplicated rather than delegating: comparing the new path against a copy of
    * itself would assert nothing. If toGroups is rewritten again, this stays as it is.
    */
  private def legacyToGroups(slates: org.apache.spark.sql.DataFrame,
                             cfg: GrpoJobConfig): (Seq[GrpoGroup], GateCounts) = {
    def classifyRow(row: org.apache.spark.sql.Row): Either[String, GrpoGroup] = {
      val items = row.getSeq[org.apache.spark.sql.Row](1)
      if (items.size < 2) return Left(GrpoSlates.TooSmall)
      val features = items.map(_.getAs[Map[String, String]]("item_features"))
      val x = features.map(f =>
        GrpoSlates.parseFeatureVector(f.getOrElse("grpo_x", null), cfg.featureVersion, cfg.dim))
      if (x.exists(_.isEmpty)) return Left(GrpoSlates.BadFeatureVersion)
      val rewards = items.map { item =>
        if (item.isNullAt(item.fieldIndex("label"))) 0.0 else item.getAs[Double]("label")
      }.toArray
      if (GrpoMath.advantages(rewards).isEmpty) return Left(GrpoSlates.ZeroVariance)
      val logged = features.map { f =>
        try f.getOrElse("prediction_score", "0.0").toDouble
        catch { case _: NumberFormatException => 0.0 }
      }.toArray
      Right(GrpoGroup(row.getString(0), x.map(_.get).toArray, logged, rewards))
    }
    val classified = slates.select("slate_id", "items").collect().map(classifyRow)
    val kept = classified.collect { case Right(g) => g }.toSeq
    val dropped = classified.collect { case Left(r) => r }
      .groupBy(identity).map { case (r, hits) => r -> hits.length.toLong }
    (kept, GateCounts(
      kept = kept.size.toLong,
      tooSmall = dropped.getOrElse(GrpoSlates.TooSmall, 0L),
      zeroVariance = dropped.getOrElse(GrpoSlates.ZeroVariance, 0L),
      badFeatureVersion = dropped.getOrElse(GrpoSlates.BadFeatureVersion, 0L)))
  }

  private def sameGroups(a: Seq[GrpoGroup], b: Seq[GrpoGroup]): Boolean =
    a.length == b.length && a.zip(b).forall { case (l, r) =>
      l.slateId == r.slateId && l.rewards.sameElements(r.rewards) &&
        l.logged.sameElements(r.logged) &&
        l.x.length == r.x.length && l.x.zip(r.x).forall { case (p, q) => p.sameElements(q) }
    }

  private def item(id: String, label: Double, v: Double, prediction: String = "0.5"): TestItem =
    TestItem(id, label, Map("grpo_x" -> vec(v), "prediction_score" -> prediction))

  it should "return the same groups and counts as the collect-then-gate implementation" in {
    val s = spark
    import s.implicits._
    // One slate per gate outcome plus two kept, so every branch is compared in a single frame.
    val frame = Seq(
      TestSlate("keep1", "r1", "u1", 1L, 1.0, 2,
        Seq(item("a", 1.0, 0.5, "0.7"), item("b", 0.0, 0.2, "0.1"))),
      TestSlate("small", "r2", "u1", 2L, 0.0, 1, Seq(item("a", 1.0, 0.5))),
      TestSlate("badver", "r3", "u1", 3L, 0.0, 2, Seq(
        TestItem("a", 1.0, Map("grpo_x" -> "v1:0.5", "prediction_score" -> "0.7")),
        item("b", 0.0, 0.2))),
      TestSlate("flat", "r4", "u1", 4L, 0.0, 2,
        Seq(item("a", 0.0, 0.5), item("b", 0.0, 0.2))),
      // "bad" prediction_score exercises the NumberFormatException fallback on a KEPT slate.
      TestSlate("keep2", "r5", "u1", 5L, 1.0, 3, Seq(
        item("a", 1.0, 0.9, "0.9"), item("b", 0.0, 0.1, "bad"), item("c", 0.0, 0.3, "0.3")))
    ).toDF()

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)
    val (wantGroups, wantCounts) = legacyToGroups(frame, cfg)

    sameGroups(groups, wantGroups) shouldBe true
    counts shouldBe wantCounts
    counts.kept shouldBe 2L
    counts.tooSmall shouldBe 1L
    counts.badFeatureVersion shouldBe 1L
    counts.zeroVariance shouldBe 1L
    groups.map(_.slateId) shouldBe Seq("keep1", "keep2")   // input order preserved
  }

  it should "count a slate under the first gate it fails, not a later one" in {
    val s = spark
    import s.implicits._
    // Counts now come from the Spark side while groups come from the driver side, so precedence
    // is the property most at risk. A one-item slate with a bad vector fails size AND version;
    // a flat two-item slate with a bad vector fails version AND variance.
    val frame = Seq(
      TestSlate("small-and-bad", "r1", "u1", 1L, 0.0, 1, Seq(
        TestItem("a", 1.0, Map("grpo_x" -> "v1:nope", "prediction_score" -> "0.7")))),
      TestSlate("bad-and-flat", "r2", "u1", 2L, 0.0, 2, Seq(
        TestItem("a", 0.0, Map("grpo_x" -> "v2:1,2", "prediction_score" -> "0.7")),
        item("b", 0.0, 0.2)))
    ).toDF()

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)
    val (_, wantCounts) = legacyToGroups(frame, cfg)

    groups shouldBe empty
    counts shouldBe wantCounts
    counts.tooSmall shouldBe 1L            // not badFeatureVersion
    counts.badFeatureVersion shouldBe 1L   // not zeroVariance
    counts.zeroVariance shouldBe 0L
  }

  it should "leave the dropped slates behind rather than collecting them first" in {
    val s = spark
    import s.implicits._
    // The whole point of the change: at low click-through the variance gate rejects most slates
    // and they must not reach the driver on the way to being discarded. Every observable value is
    // identical either way, so the claim is only testable by asserting on the gated frame -- which
    // is exactly the frame toGroups collects from, exposed for this purpose and nothing else.
    val keptCount = 5
    val droppedCount = 95
    val frame = ((0 until keptCount).map { i =>
      TestSlate(s"k$i", s"r$i", "u1", i.toLong, 1.0, 2,
        Seq(item("a", 1.0, 0.5), item("b", 0.0, 0.2)))
    } ++ (0 until droppedCount).map { i =>
      TestSlate(s"d$i", s"rd$i", "u1", i.toLong, 0.0, 2,
        Seq(item("a", 0.0, 0.5), item("b", 0.0, 0.2)))
    }).toDF()

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)
    groups should have size keptCount
    counts.kept shouldBe keptCount.toLong
    counts.zeroVariance shouldBe droppedCount.toLong

    val gated = GrpoSlates.tagged(frame, cfg)
    gated.count() shouldBe (keptCount + droppedCount).toLong
    gated.filter(gated(GrpoSlates.DropReasonColumn).isNull).count() shouldBe keptCount.toLong
  }

  it should "treat a slate with no items as too small rather than failing" in {
    val s = spark
    import s.implicits._
    val frame = Seq(
      TestSlate("empty", "r1", "u1", 1L, 0.0, 0, Seq.empty[TestItem]),
      TestSlate("keep", "r2", "u1", 2L, 1.0, 2,
        Seq(item("a", 1.0, 0.5), item("b", 0.0, 0.2)))
    ).toDF()

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)
    val (wantGroups, wantCounts) = legacyToGroups(frame, cfg)
    sameGroups(groups, wantGroups) shouldBe true
    counts shouldBe wantCounts
    counts.tooSmall shouldBe 1L
  }
```

- [x] **Step 2: Run the spec and record what fails.**

```bash
sbt 'testOnly com.demo.grpo.GrpoSlatesSpec'
```

Expected: a compile error — `tagged` and `DropReasonColumn` do not exist yet. The equivalence, precedence and empty-items tests would pass against the current implementation once it compiles (they compare it against a copy of itself); the volume test is the one that cannot be satisfied without the change. Record the compile error.

- [x] **Step 3: Rewrite `GrpoSlates.scala`'s gate and parse path.** Add to the imports at the top of the file:

```scala
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.storage.StorageLevel
```

Replace `toGroups` and `classify` in their entirety with:

```scala
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
```

- [x] **Step 4: Run the spec.**

```bash
sbt 'testOnly com.demo.grpo.GrpoSlatesSpec'
```

Expected: all pass, the four new tests included. A failure in the equivalence test names which gate or field diverged; a failure in the volume test means the filter is not actually being applied before the collect.

- [x] **Step 5: Run the whole Spark module suite.**

```bash
sbt test
```

Expected: zero failures, zero aborted suites. Record the counts. `GrpoPolicyStreamingJobSpec` and `GrpoMathSpec` must be untouched by this change, since `applyBatch` and the learning rule are not involved.

- [x] **Step 6: Commit.**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoSlates.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoSlatesSpec.scala
git commit -m "perf(grpo): gate slates in Spark before collecting them"
```

---

### Task 2: Verify and deliver

**Files:** Task 1 files, plus this plan.

**Interfaces:** Consumes the tested diff; produces a reviewed PR against `master`.

- [x] **Step 1: Update the architecture note.** `GrpoSlates.toGroups`'s KNOWN SCALING LIMITATION comment describes the collect-everything behavior and must now describe what actually happens: the gates run in Spark, the driver receives only survivors, and the remaining ceiling is the cached tagged frame on the cluster. Keep the paragraph explaining why `treeAggregate` is still not done, since that reasoning is unchanged.

- [x] **Step 2: Confirm the tree is clean.**

```bash
git status --short
git diff --check
```

- [ ] **Step 3: Request a read-only code review.** Ask specifically: that `GateCounts` is identical for overlapping-gate inputs, since counts now come from the Spark side and groups from the driver side; that `dropReason` and `buildGroup` cannot disagree about which slates are valid; that the UDF closes over two primitives rather than the whole config; that the returned `Seq`'s order is still the input order; that the cached frame is unpersisted on every path including exceptions; and whether a null or empty `items` array behaves as it did. Resolve substantive findings before publishing.

- [ ] **Step 4: Publish.** Push, record the suite counts and the row-volume evidence in the PR body, restate that no latency claim is made, fill in this plan's verification record, commit it, and mark the PR ready.

## Verification record

- Task 0: draft PR #240, https://github.com/lingduoduo/Recsys-Streaming-Pipeline/pull/240.
- Task 1 Step 2, red: `GrpoSlatesSpec.scala:243:28: value tagged is not a member of object com.demo.grpo.GrpoSlates`, then `one error found`. Only the volume test cannot be satisfied without the change; the equivalence, precedence and empty-items tests compare the pre-change implementation against a frozen copy of itself and are green by construction, which is what makes the oracle valid once the implementation moves.
- Task 1 Step 4, green: 14 tests in `GrpoSlatesSpec` -- the four added plus all ten pre-existing.
- Task 1 Step 5, full Spark module suite under JDK 17: 439 tests succeeded, 66 suites, 0 failed, 0 aborted, up from 435. `GrpoMathSpec` and `GrpoPolicyStreamingJobSpec` are untouched, which is the check that `applyBatch` and the learning rule were not involved. The `simulated Redis command error` lines in the log are deliberate fault-injection tests.
- `git diff --check` clean. No throwaway harness in the tree.

### Evidence for the claim this rests on

The design claims a row-volume reduction, not a latency win, so the evidence is a count rather than a timing. `test_..."leave the dropped slates behind rather than collecting them first"` builds 5 keepable and 95 zero-variance slates and asserts on the frame `toGroups` collects from: `tagged(frame, cfg).count()` is 100 while `tagged(...).filter(reason.isNull).count()` is 5. Every observable output is identical either way, so this is the only way the claim is testable at all without putting a counter in production code.

The spike that motivated the work, for the record: parse + collect + classify against `applyBatch`, 5,000 slates of ten items -- 591.8 ms against 23.7 ms at full reward variance (3.8% math share), and 750.5 ms against 1.2 ms at 5% engaged (0.2%). One to two orders apart, far larger than the noise, which is what makes that part robust.

### A crash this change fixes, found after the implementation

The spec's acceptance list originally said a null `items` array is "handled as today". That was wrong, and the tests as planned would not have caught it: they covered an EMPTY items array but not a null one. The old path did `row.getSeq[Row](1).size`, and `Row.getSeq` returns null for a null field, so it threw `NullPointerException` -- and `items` is nullable in `SlateSchema` with `from_json` yielding null for a missing field, so one malformed slate off Kafka could fail a whole micro-batch.

Verified both directions in one run: `a[NullPointerException] should be thrownBy legacyToGroups(frame, cfg)` passes, and the new path drops the slate as `TooSmall`. Two tests now pin it -- one asserting the new behavior, one characterizing the old -- and the spec's acceptance item 4 was rewritten to describe a deliberate improvement rather than an equivalence.

The lesson matches the DPO one: an equivalence suite built from shapes the happy path produces will not contain the shape that distinguishes the implementations. Null and empty are different fixtures and need writing separately.

### Why no latency number appears anywhere

Splitting the parse/collect path into `from_json`, collect and driver-classify components produced a NEGATIVE driver-classify component at five repetitions -- `toGroups` measured faster than a bare collect of the same frame, which is impossible and means the noise exceeded the effect. A second attempt with 25 interleaved repetitions was not completed. Rather than publish a number that could not be stood behind, the spec added a "What this does and does not claim" section and rests the justification on driver memory. Anyone re-measuring this should interleave the variants within one JVM and expect roughly 150 ms of noise at 5,000 slates.
