# GRPO Surface Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify the three remaining GRPO components that carry avoidable complexity (Java scorer, Scala slate gate, Python evaluator) without changing behavior.

**Architecture:** Each component is rewritten in place and guarded by its existing suite: the scorer loses its dead entry points and boxed sort, the gate becomes a pure `classify` plus a fold, the evaluator shares one row-parse helper and tells its history once. A JSON parity dump guards the evaluator's output.

**Tech Stack:** Java 17 + Maven + JUnit 5 + Mockito; Scala 2.12.18 + sbt (JDK 17) + ScalaTest; Python 3.12 + pytest + pandas.

**Spec:** [GRPO surface simplification](../specs/2026-09-12-grpo-surface-simplification-design.md)

## Global Constraints

- Java 17 with Maven; Scala 2.12.18 with sbt under JDK 17; Python 3 with pandas; add no dependencies.
- Keep `GrpoPolicyScorer`'s public `MODE_*`, `WEIGHTS_KEY`, `ON_BLEND_WEIGHT`, `blendWeight`, `reRankOrder`, `recordShadowSlate`, and package-private `pairwiseConcordance` unchanged in signature and behavior, including the stable descending sort and its exact blend arithmetic.
- Keep `GrpoGroup`, `GateCounts` fields and `reasons`, `parseFeatureVector`, and the `(Seq[GrpoGroup], GateCounts)` return of `toGroups` unchanged; keep the gate order too-small, bad-feature-version, zero-variance.
- Keep every public function signature and every `SystemExit` message in `grpo_offline_eval.py`, and the printed lines and summary keys of its `main`.

## Execution notes

All paths are repository-relative. Maven runs from `recsys-pipeline/services/java-retrieval-service/`
and sbt from `recsys-pipeline/services/spark-streaming-job/`, both with
`JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home`.
pytest runs from `recsys-pipeline/`. sbt holds a project lock; never overlap two sbt runs. The work
is on `simplify/grpo-surface`, based on `origin/master`. Publish a PR against `master`.

Baselines (2026-09-12): `GrpoPolicyScorerTest` 29 tests; `testOnly com.demo.grpo.*` 51 tests;
`test_grpo_offline_eval.py` 36 passed (one parametrized). The evaluator parity script `grpo_eval_parity.py` in the
session scratchpad ran on the unchanged code and saved `grpo_eval_parity_before.json`.

---

### Task 1: Java scorer

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoPolicyScorer.java`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoPolicyScorerTest.java`

**Interfaces:**
- Produces: the same public API minus `score(ServedMovie)` and `enabled()`.

- [x] **Step 1: Move the seven `score`-based tests to the production entry points**

Replace the tests `offModeIsDisabledAndScoresZero` through `aNonFiniteWeightIsUnusable` (lines 121-189) with:

```java
    /** A two-item slate: the smallest one recordShadowSlate will log. */
    private ServingSideEffectRequest twoItemSlate() {
        return slate(List.of(itemWithBanditScore("m1", 0.9), itemWithBanditScore("m2", 0.1)));
    }

    /** The weight-guard contract: unusable weights mean no shadow line, never an exception. */
    private void assertLogsNoShadowLine(GrpoPolicyScorer s) {
        assertTrue(capturingLogs(ignored -> s.recordShadowSlate(twoItemSlate())).isEmpty());
    }

    @Test
    void offModeClaimsNoBlendWeight() {
        assertEquals(0.0, scorer("off", distinctWeights(), "v2").blendWeight());
    }

    @Test
    void offModeReadsNothingFromRedis() {
        // The rollout's first promise: with the flag off, the serving path pays nothing at all.
        GrpoPolicyScorer s = scorer("off", distinctWeights(), "v2");
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis, never()).opsForHash();
    }

    @Test
    void shadowModeReadsWeightsButClaimsNoBlendWeight() {
        GrpoPolicyScorer s = scorer("shadow", distinctWeights(), "v2");
        assertEquals(0.0, s.blendWeight(), "shadow must never move a recommendation");
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis).opsForHash();
    }

    @Test
    void onModeClaimsOnlyTheUnclaimedBlendWeight() {
        GrpoPolicyScorer s = scorer("on", distinctWeights(), "v2");
        assertEquals(GrpoPolicyScorer.ON_BLEND_WEIGHT, s.blendWeight());
        // MovieLensOutcomeScorer's weights sum to 0.85; taking more than 0.15 would silently
        // change every existing score.
        assertTrue(s.blendWeight() <= 0.15);
    }

    @Test
    void anUnrecognisedModeIsTreatedAsOff() {
        GrpoPolicyScorer s = scorer("enabled", distinctWeights(), "v2");
        assertEquals(0.0, s.blendWeight());
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis, never()).opsForHash();
    }

    @Test
    void weightsOfADifferentFeatureVersionAreIgnored() {
        // The exact cutover hazard: v1 weights left behind in Redis after the v2 rollout must be
        // refused, not silently applied against the v2 (9-wide) feature layout.
        assertLogsNoShadowLine(scorer("shadow", distinctWeights(), "v1"));
    }

    @Test
    void aV1WidthWeightVectorIsIgnoredEvenWhenTaggedV2() {
        // Belt and suspenders on the same cutover hazard: even if the version tag were (wrongly)
        // left as v2, a stale 10-wide v1 vector must not be applied against 9 v2 features.
        String tenWide = java.util.stream.IntStream.rangeClosed(1, 10)
            .mapToObj(i -> Double.toString((double) i))
            .collect(Collectors.joining(","));
        assertLogsNoShadowLine(scorer("shadow", tenWide, "v2"));
    }

    @Test
    void aMissingWeightVectorLogsNothingRatherThanFailing() {
        assertLogsNoShadowLine(scorer("shadow", null, "v2"));
    }

    @Test
    void aNonFiniteWeightIsUnusable() {
        // Double.parseDouble accepts "NaN": without the finite check every score would be NaN,
        // which sorts unpredictably instead of degrading to the no-weights case.
        String withNaN = "NaN," + String.join(",", java.util.Collections.nCopies(GrpoFeatures.DIM - 1, "1.0"));
        assertLogsNoShadowLine(scorer("shadow", withNaN, "v2"));
        String withInf = "Infinity," + String.join(",", java.util.Collections.nCopies(GrpoFeatures.DIM - 1, "1.0"));
        assertLogsNoShadowLine(scorer("shadow", withInf, "v2"));
    }
```

Delete the `expectedScore()` helper (lines 46-53) and the now-unused `import static org.junit.jupiter.api.Assertions.assertFalse;`.

- [x] **Step 2: Run the scorer suite to confirm it still passes on the unchanged scorer**

Run: `cd recsys-pipeline/services/java-retrieval-service && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn test -Dtest=GrpoPolicyScorerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 29, Failures: 0`. The tests now go through the real entry points; nothing in production has changed yet. Observed: 29 run, 0 failures.

- [x] **Step 3: Remove the dead entry points and unbox the sort**

In `GrpoPolicyScorer.java`:

- delete `enabled()`, `score(ServedMovie)`, and `private static double dot(double[] w, ServedMovie movie)`;
- in `recordShadowSlate`, change `scores[i] = dot(w, served.get(i));` to `scores[i] = dot(w, GrpoFeatures.of(served.get(i)));`;
- replace `import java.util.Arrays;` with `import java.util.stream.IntStream;` (keep the other imports);
- in `reRankOrder`, replace everything from `double blend = blendWeight();` through `return Optional.of(result);` with:

```java
            double blend = blendWeight();
            double[] adjusted = new double[n];
            for (int i = 0; i < n; i++) {
                double norm = (raw[i] - min) / range;
                adjusted[i] = (1.0 - blend) * finalScores[i] + blend * norm;
            }
            // Stable, descending: ties keep their incumbent order, as Arrays.sort did before.
            int[] order = IntStream.range(0, n).boxed()
                .sorted((a, b) -> Double.compare(adjusted[b], adjusted[a]))
                .mapToInt(Integer::intValue)
                .toArray();
            return Optional.of(order);
```

- [x] **Step 4: Verify**

Run: `grep -n "score(\|enabled()\|Arrays" recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoPolicyScorer.java`
Expected: no output.

Run the same Maven command as Step 2.
Expected: `Tests run: 29, Failures: 0`. Observed: 29 run, 0 failures.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoPolicyScorer.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoPolicyScorerTest.java
git commit -m "refactor: drop the GRPO scorer's dead entry points and boxed sort"
```

---

### Task 2: Scala slate gate

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoSlates.scala:18-91`

**Interfaces:**
- Produces: `GrpoSlates.TooSmall`, `GrpoSlates.BadFeatureVersion`, `GrpoSlates.ZeroVariance` (Strings); private `classify(row: Row, cfg: GrpoJobConfig): Either[String, GrpoGroup]`; `toGroups` unchanged in signature.

- [x] **Step 1: Rewrite `GateCounts.reasons`, add the constants, `classify`, and the fold-based `toGroups`**

Replace `GateCounts` with:

```scala
final case class GateCounts(kept: Long, tooSmall: Long, zeroVariance: Long, badFeatureVersion: Long) {
  def reasons: Seq[(String, Long)] =
    Seq(GrpoSlates.TooSmall -> tooSmall, GrpoSlates.ZeroVariance -> zeroVariance,
      GrpoSlates.BadFeatureVersion -> badFeatureVersion)
}
```

Keep its doc comment. Inside `object GrpoSlates`, add after the opening brace:

```scala
  /** Drop reasons, as DropMetrics reports them. */
  val TooSmall = "slate_too_small"
  val BadFeatureVersion = "bad_feature_version"
  val ZeroVariance = "zero_reward_variance"
```

Replace `toGroups` (keeping its scaling-limitation doc comment verbatim) with:

```scala
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
```

- [x] **Step 2: Run the GRPO suites**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home sbt -batch "testOnly com.demo.grpo.*"`
Expected: 51 tests, 51 succeeded. The five gate tests pin each reason and the kept group's rewards. Observed: 51 succeeded.

Run: `grep -n "var \|ArrayBuffer\|\.total\b" recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoSlates.scala`
Expected: no output.

- [x] **Step 3: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoSlates.scala
git commit -m "refactor: classify GRPO slates purely and fold the gate counts"
```

---

### Task 3: Python evaluator

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/post-training/grpo_offline_eval.py`

**Interfaces:**
- Produces: private `_parse_rows(rows)` yielding `(row, features, parsed)`; every public function unchanged.

- [x] **Step 1: Share the row parse and collapse the schema check**

Add after `_as_feature_map`:

```python
def _parse_rows(rows):
    """Each row with its normalized item_features and its parsed grpo_x, or None if absent/malformed."""
    for row in rows:
        features = _as_feature_map(row.get("item_features"))
        yield row, features, parse_packed_vector(features.get("grpo_x"))
```

In `detect_feature_schema`, replace

```python
    for idx, row in enumerate(rows):
        features = _as_feature_map(row.get("item_features"))
        parsed = parse_packed_vector(features.get("grpo_x"))
        if parsed is None:
            continue
```

with

```python
    for idx, (_, _, parsed) in enumerate(_parse_rows(rows)):
        if parsed is None:
            continue
```

In `build_scored_rows`, replace

```python
    for row in rows:
        request_id = row.get("request_id")
        if not ope_eval_report.is_test(request_id):
            continue
        features = _as_feature_map(row.get("item_features"))
        parsed = parse_packed_vector(features.get("grpo_x"))
        pred_raw = features.get("prediction_score")
```

with

```python
    for row, features, parsed in _parse_rows(rows):
        request_id = row.get("request_id")
        if not ope_eval_report.is_test(request_id):
            continue
        pred_raw = features.get("prediction_score")
```

and replace

```python
        row_version, vector = parsed
        if row_version != version or len(vector) != dim:
            n_dropped += 1
            continue
```

with

```python
        row_version, vector = parsed
        if (row_version, len(vector)) != (version, dim):
            n_dropped += 1
            continue
```

- [x] **Step 2: Tell the v1 history once**

Replace the comment above `SUPPORTED_FEATURE_VERSION` with:

```python
# The one feature layout this tool supports (GrpoFeatures, java-retrieval-service). Pinned rather
# than inferred: a v1 dataset must be refused, not evaluated (see the module docstring).
```

In `detect_feature_schema`'s docstring, replace the paragraph beginning `The majority schema is then checked against this tool's one supported contract` through `re-collected under v2.` with:

```
    The majority schema must then be the one supported contract, v2/9-wide; a v1 dataset agreeing
    with v1 weights is still refused, because v1's auc_diff is inflated by the served-position
    feature (module docstring).
```

In `build_scored_rows`'s docstring, leave the text as is. In `main`, replace the eight-line comment beginning `# v1's grpo_x carried a served-position feature` with:

```python
        # v2 removed the served-position feature (module docstring), so auc_diff is the flip
        # criterion directly.
```

- [x] **Step 3: Verify tests, parity, and the history count**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_grpo_offline_eval.py -q`
Expected: 36 passed. Observed: 36 passed.

Run: `cd recsys-pipeline && python3 <scratchpad>/grpo_eval_parity.py > <scratchpad>/grpo_eval_parity_after.json && cmp <scratchpad>/grpo_eval_parity_before.json <scratchpad>/grpo_eval_parity_after.json && echo IDENTICAL`
Expected: `IDENTICAL`. Observed: identical.

Run: `grep -c "served-position feature" recsys-pipeline/services/python-modeling/post-training/grpo_offline_eval.py`
Expected: 2 (module docstring, `detect_feature_schema` docstring) or fewer. Observed: 3, at the module docstring, the pinned refusal message, and the one pointer comment in `main`; the `detect_feature_schema` docstring no longer uses the phrase.

- [x] **Step 4: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/grpo_offline_eval.py
git commit -m "refactor: share the GRPO evaluator's row parse and tell the v1 history once"
```

---

### Task 4: Full suites, records, and PR

**Files:**
- Modify: `.superpowers/docs/specs/2026-09-12-grpo-surface-simplification-design.md` (status and verification record)
- Modify: this plan (check boxes, record observations)

**Interfaces:**
- Consumes: the finished work from Tasks 1-3.
- Produces: a PR against `master` from `simplify/grpo-surface`.

- [x] **Step 1: Run the three full suites**

Run in the background, one sbt run at a time:
- `cd recsys-pipeline/services/java-retrieval-service && JAVA_HOME=... mvn test` → expected `BUILD SUCCESS`, `UserProfileIntegrationTest` skipped as always.
- `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=... sbt -batch test` → expected all tests pass (424 on 2026-09-12).
- `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling -q` → expected 501 passed.

If unrelated tests fail, confirm they fail on `origin/master` too before reporting. Observed: Maven 329 run, 0 failures, 1 skipped; Spark 424 succeeded in 5 min 1 s; Python 501 passed.

- [x] **Step 2: Update the spec status and verification record, tick this plan, and commit**

Set the spec's status line to `Implemented and verified; PR pending` and add a `## Verification record` section with the per-task counts, the parity result, the greps, and the three full-suite results.

```bash
git add .superpowers/docs/specs/2026-09-12-grpo-surface-simplification-design.md .superpowers/docs/plans/2026-09-12-grpo-surface-simplification.md
git commit -m "docs: record GRPO surface simplification verification"
```

- [x] **Step 3: Open the PR**

```bash
git push -u origin simplify/grpo-surface
gh pr create --base master --title "refactor: simplify the GRPO scorer, slate gate, and offline evaluator" --body "$(cat <<'EOF'
## Summary
- Follow-up to #222, which simplified the PPO objective; this covers the rest of the GRPO surface.
- Java `GrpoPolicyScorer`: remove `score` and `enabled`, which had no production caller, and sort the re-rank on primitives instead of boxing. The seven tests that used `score` now go through `blendWeight` and `recordShadowSlate`.
- Scala `GrpoSlates`: a pure `classify` returns the first failing gate or the group; `toGroups` folds the results with no mutable counters. Drop reasons are named once.
- Python `grpo_offline_eval.py`: one shared row-parse helper, a single tuple schema check, and the v1 position-feature history told once instead of four times.
- No change to gate semantics, drop-reason names, wire format, mode names, blend weight, printed lines, summary keys, or error messages.

## Test plan
- [x] `GrpoPolicyScorerTest`: 29 tests, 0 failures.
- [x] `testOnly com.demo.grpo.*`: 51 tests, 0 failures, `GrpoSlatesSpec` unchanged.
- [x] `test_grpo_offline_eval.py`: 35 passed unchanged; evaluator stdout and summary byte-identical on a fixed fixture before and after.
- [x] Full retrieval Maven suite, full Spark module suite, full Python modeling suite: <fill from Step 1>.

Spec: `.superpowers/docs/specs/2026-09-12-grpo-surface-simplification-design.md`
Plan: `.superpowers/docs/plans/2026-09-12-grpo-surface-simplification.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Then record the PR link in the spec's status line and commit it with `docs: link GRPO surface simplification PR`.

---

## Self-review

- Spec coverage: Java scorer (Task 1), Scala gate (Task 2), Python evaluator (Task 3), suites and PR (Task 4). Acceptance criteria 1-5 map to Task 1 Step 4, Task 2 Step 2, Task 3 Step 3, Task 3 Step 3, Task 4 Step 1.
- Placeholders: one `<fill from Step 1>` in the PR body supplied by Task 4 Step 1; `<scratchpad>` is the session scratchpad directory.
- Names: `TooSmall`, `BadFeatureVersion`, `ZeroVariance`, `classify`, `_parse_rows`, `twoItemSlate`, `assertLogsNoShadowLine` are consistent between spec and tasks.
