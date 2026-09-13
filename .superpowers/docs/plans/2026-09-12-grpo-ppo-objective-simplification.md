# GRPO PPO Objective Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify the PPO-clipped objective inside online GRPO (`GrpoMath` and `applyBatch`) without changing what it computes.

**Architecture:** `GrpoMath` exposes `logits`, shares policy setup between `loss` and `gradient`, gates the surrogate with one comparison, and drops the KL derivative's dead `+1`. `applyBatch` prepares each group's snapshot logits and advantage once per batch and loops epochs over that prepared list. The existing finite-difference and hand-computed loss tests are the oracle; one new test covers the clip-active, unclipped-selected branch.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17.

**Spec:** [GRPO PPO objective simplification](../specs/2026-09-12-grpo-ppo-objective-simplification-design.md)

## Global Constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, JDK 17 for sbt; add no dependencies.
- Keep `GrpoHyperParams`, `GrpoJobConfig`, `GrpoGroup`, `GrpoWeights`, and every public signature in `GrpoMath` and `GrpoPolicyStreamingJob` unchanged. `logits` may become public.
- Keep `loss` numerically identical. Keep `gradient` equal to its previous value up to floating-point reassociation; the existing finite-difference tolerance of `1e-10` against `loss` remains the acceptance bound.
- Keep `applyBatch` semantics: snapshot frozen for the batch, ratio against the snapshot, KL against the logged policy, the update divided by `groups.size`, groups whose advantage is `None` skipped, and the returned `GrpoWeights` fields unchanged.
- Keep `AdvantageFloor` and its uses as the probability floor in the ratio and KL.

## Execution notes

All paths are repository-relative. Run sbt from `recsys-pipeline/services/spark-streaming-job/` with
`JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home`
(the default JDK 25 aborts Spark-session suites). Per-task verification is
`sbt -batch "testOnly com.demo.grpo.*"`, which finishes in a few minutes. The full module suite takes
about 13.5 minutes and must run in the background; sbt holds a project lock, so never overlap two runs.

The work is on `simplify/grpo-ppo-objective`, based on `origin/master`. Publish a PR against
`master`; never commit to `master`.

Baseline before any change (2026-09-12): `testOnly com.demo.grpo.*` → 50 tests, 50 succeeded.

---

### Task 1: Cover the clip-active, unclipped-selected branch

**Files:**
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala`

**Interfaces:**
- Consumes: `GrpoMath.gradient`, `GrpoMath.loss`, `GrpoMath.softmax`, `GrpoMath.advantages`, and the spec's private `numericGradient` and `gradTol` (existing).
- Produces: a characterization test that must pass before and after Task 2.

- [x] **Step 1: Add the test**

Append inside `GrpoMathSpec`, after the last `"gradient" should "match a finite-difference approximation once the clip band is engaged with a positive advantage"` test and before the class's closing brace:

```scala
  it should "match a finite-difference approximation when the clip is active but min() keeps the raw ratio" in {
    // The branch no other fixture reaches. Candidate 0 has positive advantage and a ratio BELOW
    // the band; candidate 1 has negative advantage and a ratio ABOVE it. In both cases min()
    // keeps the raw ratio, so the surrogate still moves with w even though both ratios are
    // outside the clip range. One-hot features make logits(x, w) == w exactly.
    val x = Array(Array(1.0, 0.0), Array(0.0, 1.0))
    val w = Array(-2.0, 0.0)
    val snapshot = Array(0.0, 0.0)
    val logged = Array(0.3, -0.9)
    val adv = GrpoMath.advantages(Array(1.0, 0.0)).get // exactly [1.0, -1.0]

    val pi = GrpoMath.softmax(w, cfg.temperature)
    val piSnap = GrpoMath.softmax(snapshot, cfg.temperature)
    adv(0) should be > 0.0
    (pi(0) / piSnap(0)) should be < (1.0 - cfg.clipEpsilon)
    adv(1) should be < 0.0
    (pi(1) / piSnap(1)) should be > (1.0 + cfg.clipEpsilon)

    val analytic = GrpoMath.gradient(x, snapshot, logged, w, adv, cfg)
    val numeric = numericGradient(x, snapshot, logged, w, adv, cfg)
    analytic.zip(numeric).foreach { case (a, n) => a shouldBe n +- gradTol }

    // The surrogate is not flat on this branch: with the KL off, the gradient must be nonzero.
    val noKl = cfg.copy(klBeta = 0.0)
    GrpoMath.gradient(x, snapshot, logged, w, adv, noKl).exists(v => math.abs(v) > 1e-6) shouldBe true
  }
```

- [x] **Step 2: Run the GRPO suites to confirm it passes on the current code**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home sbt -batch "testOnly com.demo.grpo.*"`
Expected: 51 tests, 51 succeeded. This is a characterization test of a branch that is already correct. Observed: 51 succeeded.

- [x] **Step 3: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala
git commit -m "test: cover the clip-active, unclipped-selected GRPO gradient branch"
```

---

### Task 2: Simplify `GrpoMath`

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala:56-124`

**Interfaces:**
- Consumes: `softmax`, `kl`, `AdvantageFloor` (existing).
- Produces: `def logits(x: Array[Array[Double]], w: Array[Double]): Array[Double]` (now public, same body); `loss` and `gradient` with unchanged signatures.

- [x] **Step 1: Make `logits` public and add the shared policy helper**

Replace

```scala
  private def logits(x: Array[Array[Double]], w: Array[Double]): Array[Double] =
    x.map(row => row.indices.foldLeft(0.0)((acc, i) => acc + row(i) * w(i)))
```

with

```scala
  /** w . x_i for each candidate. */
  def logits(x: Array[Array[Double]], w: Array[Double]): Array[Double] =
    x.map(row => row.indices.foldLeft(0.0)((acc, i) => acc + row(i) * w(i)))

  /** (pi, piSnap, piOld): the current, batch-snapshot, and logged policies over one group. */
  private def policies(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double],
                       w: Array[Double], cfg: GrpoHyperParams): (Array[Double], Array[Double], Array[Double]) =
    (softmax(logits(x, w), cfg.temperature),
     softmax(snapshotLogits, cfg.temperature),
     softmax(loggedLogits, cfg.temperature))
```

- [x] **Step 2: Use the helper in `loss`**

Replace the three `val piSnap = ...`, `val piOld = ...`, `val pi = ...` lines at the top of `loss` with:

```scala
    val (pi, piSnap, piOld) = policies(x, snapshotLogits, loggedLogits, w, cfg)
```

Leave the surrogate expression and the return line as they are.

- [x] **Step 3: Rewrite `gradient` and its derivation comment**

Replace everything from the `/** Analytic gradient of `loss` with respect to w.` doc comment through the end of `gradient` with:

```scala
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
               w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double] = {
    val dim = w.length
    val (pi, piSnap, piOld) = policies(x, snapshotLogits, loggedLogits, w, cfg)

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
```

- [x] **Step 4: Run the GRPO suites**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home sbt -batch "testOnly com.demo.grpo.*"`
Expected: 51 tests, 51 succeeded. The five finite-difference tests and the hand-computed loss test are what certify the rewrite. Observed: 51 succeeded.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala
git commit -m "refactor: simplify the GRPO clipped-surrogate gradient"
```

---

### Task 3: Prepare each group once per batch in `applyBatch`

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoPolicyStreamingJob.scala:34-58`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoPolicyStreamingJobSpec.scala:80-83`

**Interfaces:**
- Consumes: `GrpoMath.logits` (public from Task 2), `GrpoMath.advantages`, `GrpoMath.gradient`.
- Produces: `applyBatch(current, groups, cfg, batchId): GrpoWeights` with unchanged signature and semantics.

- [x] **Step 1: Replace the body of `applyBatch`**

Replace the method from `val snapshot = current.weights.clone()` through the closing `GrpoWeights(...)` line with:

```scala
    val snapshot = current.weights.clone()
    val w = current.weights.clone()
    // Neither the snapshot-side logits nor the advantage depends on w, so both are fixed for the
    // whole batch: prepare them once here rather than on every inner epoch.
    val prepared = groups.flatMap { g =>
      GrpoMath.advantages(g.rewards).map(adv => (g, GrpoMath.logits(g.x, snapshot), adv))
    }
    (1 to cfg.hyper.innerEpochs).foreach { _ =>
      val total = Array.fill(cfg.dim)(0.0)
      prepared.foreach { case (g, snapshotLogits, adv) =>
        // Ratio against the snapshot; KL against what actually served. GrpoMath.gradient takes
        // both references, so the two cannot be conflated here.
        val grad = GrpoMath.gradient(g.x, snapshotLogits, g.logged, w, adv, cfg.hyper)
        (0 until cfg.dim).foreach(d => total(d) += grad(d))
      }
      (0 until cfg.dim).foreach(d => w(d) -= cfg.hyper.learningRate * total(d) / groups.size)
    }
    GrpoWeights(w, cfg.featureVersion, batchId, current.slatesApplied + groups.size)
```

Keep the `if (groups.isEmpty) return current.copy(batchId = batchId)` guard and the method's doc comment as they are.

- [x] **Step 2: Use `GrpoMath.logits` in the job spec's epoch replay**

In `GrpoPolicyStreamingJobSpec`, inside the test `"keep the ratio anchored to the batch-start snapshot across inner epochs"`, replace

```scala
    val snapshotLogitsByGroup = groups.map { g =>
      g.x.map(row => row.indices.foldLeft(0.0)((a, i) => a + row(i) * snapshot(i)))
    }
```

with

```scala
    val snapshotLogitsByGroup = groups.map(g => GrpoMath.logits(g.x, snapshot))
```

- [x] **Step 3: Run the GRPO suites**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home sbt -batch "testOnly com.demo.grpo.*"`
Expected: 51 tests, 51 succeeded. The snapshot-anchoring replay, the no-leak test, the 200-batch stress test, and the non-mutation test guard this rewrite. Observed: 51 succeeded.

- [x] **Step 4: Confirm no dot product remains outside `GrpoMath`**

Run: `grep -rn "foldLeft(0.0)" recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo`
Expected: only the `logits` and `kl` lines in `GrpoMath.scala`. Observed: exactly those two lines.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoPolicyStreamingJob.scala recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoPolicyStreamingJobSpec.scala
git commit -m "refactor: prepare GRPO groups once per batch and reuse GrpoMath.logits"
```

---

### Task 4: Full suite, records, and PR

**Files:**
- Modify: `.superpowers/docs/specs/2026-09-12-grpo-ppo-objective-simplification-design.md` (status and verification record)
- Modify: this plan (check boxes, record observations)

**Interfaces:**
- Consumes: the finished package from Tasks 1-3.
- Produces: a PR against `master` from `simplify/grpo-ppo-objective`.

- [x] **Step 1: Run the full Spark module suite in the background**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home sbt -batch test > <scratchpad>/spark_full.log 2>&1`
Expected: all tests pass (423 passed on 2026-09-12 for the previous PR; `UserProfileIntegrationTest` always skips because Testcontainers is blocked on Docker 29). If unrelated tests fail, confirm they fail on `origin/master` too before reporting; do not claim a green suite otherwise. Observed: 424 tests, 424 succeeded, 4 min 32 s.

- [x] **Step 2: Update the spec status and verification record, tick this plan, and commit**

Set the spec's status line to `Implemented and verified; PR pending` and add a `## Verification record` section listing the per-task GRPO counts and the full-suite result.

```bash
git add .superpowers/docs/specs/2026-09-12-grpo-ppo-objective-simplification-design.md .superpowers/docs/plans/2026-09-12-grpo-ppo-objective-simplification.md
git commit -m "docs: record GRPO PPO objective simplification verification"
```

- [x] **Step 3: Open the PR**

```bash
git push -u origin simplify/grpo-ppo-objective
gh pr create --base master --title "refactor: simplify the GRPO PPO objective" --body "$(cat <<'EOF'
## Summary
- Investigated PPO: the repository has no standalone PPO; the clipped surrogate is the online GRPO objective in `GrpoMath` and `applyBatch`.
- Drop the KL derivative's `+1`, which multiplies sum_i pi_i (x_i - E[x]) = 0 and so never contributes (max difference 7e-12 over 2,000 random groups).
- Replace the two-boolean clip gate with the single comparison `ratio * adv <= clipped * adv`.
- Share policy setup between `loss` and `gradient`; make `GrpoMath.logits` public and use it in `applyBatch` and the job spec instead of hand-rolled `foldLeft` dot products.
- Prepare each group's snapshot logits and advantage once per batch instead of on every inner epoch.
- No change to hyperparameters, config parsing, the Redis weight layout, the feature layout, or the serving scorer.

## Test plan
- [x] New finite-difference test on the branch no fixture reached: clip active with the unclipped branch selected. Passes before and after.
- [x] `testOnly com.demo.grpo.*` → 51 tests pass (50 baseline + 1 new); the existing finite-difference and hand-computed loss tests certify the gradient rewrite.
- [x] Full Spark module suite under JDK 17: <fill from Step 1>.

Spec: `.superpowers/docs/specs/2026-09-12-grpo-ppo-objective-simplification-design.md`
Plan: `.superpowers/docs/plans/2026-09-12-grpo-ppo-objective-simplification.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Then record the PR link in the spec's status line and commit it with `docs: link GRPO PPO objective simplification PR`.

---

## Self-review

- Spec coverage: public `logits` and shared policy helper (Task 2 Steps 1-2), single-comparison gate and dropped `+1` (Task 2 Step 3), once-per-batch preparation (Task 3 Step 1), job spec using `logits` (Task 3 Step 2), new branch test (Task 1), full suite and PR (Task 4). Acceptance criteria 1-5 map to Task 1, Tasks 2-3 Step 4/3, Task 2 Step 3 plus Task 3 Step 1, Task 3 Step 4, and Task 4 Step 1.
- Placeholders: the PR body has one `<fill from Step 1>` slot supplied by Task 4 Step 1; the log path `<scratchpad>` is the session scratchpad directory.
- Names used across tasks: `logits`, `policies`, `prepared`, `unclippedSelected`, `piSnapFloored`, `numericGradient`, `gradTol` are consistent.
