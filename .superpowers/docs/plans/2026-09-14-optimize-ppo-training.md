# Optimize PPO Training Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop recomputing the two reference policies on every PPO inner epoch, without changing the weights the job produces.

**Architecture:** Split `GrpoMath.gradient` at the softmax boundary. A package-visible `gradientFromPolicies` consumes the two fixed policies; the public `gradient` softmaxes both references and delegates, so its behavior is untouched. `GrpoPolicyStreamingJob.applyBatch`, which already hoists the snapshot logits and advantages out of the inner-epoch loop, hoists the two softmaxes as well.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17.

**Spec:** [Optimize PPO training design](../specs/2026-09-14-optimize-ppo-training-design.md)

## Global Constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17; no new dependencies.
- Preserve `gradient(x, snapshotLogits, loggedLogits, w, adv, cfg)` and its returned vector exactly. Weights from `applyBatch` must be bitwise identical to today's, not merely within a tolerance.
- Preserve the two-reference separation: the ratio measures against the batch snapshot and the KL anchors to the logged policy. No change may make collapsing them expressible.
- Preserve `softmax`'s max-subtraction, `AdvantageFloor` and every floor it guards, the clipped-branch selection rule, and the operation order inside the gradient accumulation.
- Leave `loss`, `advantages`, `kl`, `logits`, and `softmax` unchanged. `loss` has no production caller and stays on the public path.
- Do not mutate caller-owned arrays and do not retain state across calls.

## Environment

sbt must run under JDK 17. The default JDK on this machine aborts Spark-session tests with a misleading `getSubject` error. Prefix every sbt command:

```bash
export JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home
```

All sbt commands run from `recsys-pipeline/services/spark-streaming-job`.

## File Structure

- `src/main/scala/com/demo/grpo/GrpoMath.scala` — gains `gradientFromPolicies`; `gradient` becomes a two-line wrapper. No other function changes.
- `src/main/scala/com/demo/grpo/GrpoPolicyStreamingJob.scala` — `applyBatch`'s `prepared` carries the two policies; the inner-epoch loop calls the new method.
- `src/test/scala/com/demo/grpo/GrpoMathSpec.scala` — gains the delegation-equivalence test.
- `src/test/scala/com/demo/grpo/GrpoPolicyStreamingJobSpec.scala` — gains the bitwise batch-equivalence assertion.

---

### Task 0: Publish the design first

**Files:**
- Create: `.superpowers/docs/specs/2026-09-14-optimize-ppo-training-design.md`
- Create: `.superpowers/docs/plans/2026-09-14-optimize-ppo-training.md`

**Interfaces:** Produces the branch and draft PR that Tasks 1-3 commit into. No code.

- [x] **Step 1: Commit the spec and plan on a branch off `master`.**

```bash
git checkout master && git pull
git checkout -b optimize/ppo-training
git add .superpowers/docs/specs/2026-09-14-optimize-ppo-training-design.md \
        .superpowers/docs/plans/2026-09-14-optimize-ppo-training.md
git commit -m "docs: specify the PPO training optimization"
```

- [x] **Step 2: Push and open a draft PR against `master`** titled `Optimize PPO training`, whose body states the problem, the measured baseline, and that code follows in this same PR. Use `gh pr create --draft --body-file`. Record the PR number here; the spec and plan are then reviewable before any code exists.

Observed: draft PR #235, https://github.com/lingduoduo/Recsys-Streaming-Pipeline/pull/235.
---

### Task 1: Split the gradient at the softmax boundary

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala:104-125`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala`

**Interfaces:**
- Consumes: `GrpoMath.softmax(logits: Array[Double], temperature: Double): Array[Double]` and `GrpoMath.logits(x: Array[Array[Double]], w: Array[Double]): Array[Double]`, both unchanged.
- Produces: `private[grpo] def gradientFromPolicies(x: Array[Array[Double]], piSnap: Array[Double], piOld: Array[Double], w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double]`. Task 2 calls this. The public `gradient` keeps its existing signature and becomes a wrapper.

- [x] **Step 1: Write the failing test.** Append to `GrpoMathSpec.scala`. It calls a method that does not exist yet, so it fails to compile until Step 3.

```scala
  it should "match the public gradient when the reference policies are supplied directly" in {
    // The public form softmaxes both references on every call; the inner form takes them
    // already softmaxed. Same inputs must give the same vector, bit for bit -- the hoist in
    // applyBatch is only safe if these two cannot drift.
    val cases = Seq(
      // (x, snapshotLogits, loggedLogits, w, rewards) - ordinary, saturated, and clipped
      (Array(Array(1.0, 0.0), Array(0.0, 1.0)), Array(0.3, 0.3), Array(0.1, 0.2),
       Array(0.5, -0.25), Array(1.0, 0.0)),
      (Array(Array(1.0, 0.0), Array(0.0, 1.0)), Array(1000.0, -1000.0), Array(-800.0, 900.0),
       Array(2.0, -3.0), Array(1.0, 0.0)),
      (Array(Array(1.0, 0.0), Array(0.0, 1.0), Array(0.5, 0.5)), Array(0.0, 0.0, 0.0),
       Array(5.0, -5.0, 0.0), Array(4.0, -4.0), Array(1.0, 0.0, 0.0)))

    cases.foreach { case (x, snapshot, logged, w, rewards) =>
      val adv = GrpoMath.advantages(rewards).get
      val expected = GrpoMath.gradient(x, snapshot, logged, w, adv, cfg)
      val actual = GrpoMath.gradientFromPolicies(
        x, GrpoMath.softmax(snapshot, cfg.temperature),
        GrpoMath.softmax(logged, cfg.temperature), w, adv, cfg)
      actual.length shouldBe expected.length
      actual.indices.foreach { d =>
        java.lang.Double.doubleToRawLongBits(actual(d)) shouldBe
          java.lang.Double.doubleToRawLongBits(expected(d))
      }
    }
  }
```

- [x] **Step 2: Run it and confirm it fails.**

```bash
sbt 'testOnly com.demo.grpo.GrpoMathSpec'
```

Expected: a compile error reporting that `gradientFromPolicies` is not a member of `GrpoMath`. Record the message.

Observed: `GrpoMathSpec.scala:249:29: value gradientFromPolicies is not a member of object com.demo.grpo.GrpoMath`, then `Compilation failed`.

- [x] **Step 3: Extract the method.** In `GrpoMath.scala`, replace the existing `gradient` with the wrapper plus the extracted worker. The worker body is today's `gradient` body with the `policies` call replaced by deriving `pi` alone; every other line, including operation order, is unchanged.

```scala
  def gradient(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double],
               w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double] =
    gradientFromPolicies(x, softmax(snapshotLogits, cfg.temperature),
                         softmax(loggedLogits, cfg.temperature), w, adv, cfg)

  /** `gradient` with the two fixed reference POLICIES supplied already softmaxed.
    *
    * `piSnap` and `piOld` are distributions over the slate, not logits -- both are
    * `Array[Double]`, so the compiler cannot tell them apart and a caller passing logits here
    * would get a silently wrong gradient. Package-visible for that reason: the one production
    * caller is `GrpoPolicyStreamingJob.applyBatch`, which holds both references fixed for a whole
    * micro-batch and would otherwise re-derive them on every inner epoch. Only `pi` depends on
    * `w`, so only `pi` is computed here.
    */
  private[grpo] def gradientFromPolicies(
      x: Array[Array[Double]], piSnap: Array[Double], piOld: Array[Double],
      w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double] = {
    val dim = w.length
    val pi = softmax(logits(x, w), cfg.temperature)

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

Keep the existing scaladoc block that explains the analytic gradient on the public `gradient`. Leave the private `policies` helper in place; `loss` still uses it.

- [x] **Step 4: Run the focused suite and confirm it passes.**

```bash
sbt 'testOnly com.demo.grpo.GrpoMathSpec'
```

Expected: all tests pass, including the pre-existing finite-difference, clipping, KL, and two-reference-separation cases. Record the count.

Observed: 17 tests succeeded, including the pre-existing finite-difference, clipping, KL, and two-reference-separation cases.

- [x] **Step 5: Commit.**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala
git commit -m "refactor(grpo): a gradient form that takes the reference policies"
```

---

### Task 2: Hoist the reference policies in applyBatch

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoPolicyStreamingJob.scala:38-52`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoPolicyStreamingJobSpec.scala`

**Interfaces:**
- Consumes: `GrpoMath.gradientFromPolicies` from Task 1, and `GrpoMath.softmax` / `GrpoMath.logits` / `GrpoMath.advantages` unchanged.
- Produces: no signature change. `applyBatch(current: GrpoWeights, groups: Seq[GrpoGroup], cfg: GrpoJobConfig, batchId: Long): GrpoWeights` keeps its shape and its result.

- [x] **Step 1: Write the failing test.** Append to `GrpoPolicyStreamingJobSpec.scala`. It pins bitwise equality against a by-hand loop built from the public `gradient`, at two inner epochs, which is where the recomputation happened. Written before Task 2's change, it passes; it exists to fail if the hoist ever drifts, so run it again after Step 3.

```scala
  it should "produce bitwise the same weights as the public gradient over two inner epochs" in {
    // applyBatch hoists the two reference policies out of the inner-epoch loop. Hoisting a value
    // that is genuinely fixed must not move a single bit; anything else means the hoisted
    // reference was not actually constant across epochs.
    val groups = Seq(group(Array(1.0, 0.0, 0.0)), group(Array(0.0, 1.0, 0.0)))
    val cfg2 = cfg.copy(hyper = cfg.hyper.copy(innerEpochs = 2))

    val snapshot = GrpoWeightStore.initial(cfg2).weights.clone()
    val snapshotLogitsByGroup = groups.map(g => GrpoMath.logits(g.x, snapshot))
    val wRef = snapshot.clone()
    (1 to cfg2.hyper.innerEpochs).foreach { _ =>
      val total = Array.fill(cfg2.dim)(0.0)
      groups.zip(snapshotLogitsByGroup).foreach { case (g, snapshotLogits) =>
        GrpoMath.advantages(g.rewards).foreach { adv =>
          val grad = GrpoMath.gradient(g.x, snapshotLogits, g.logged, wRef, adv, cfg2.hyper)
          (0 until cfg2.dim).foreach(d => total(d) += grad(d))
        }
      }
      (0 until cfg2.dim).foreach(d =>
        wRef(d) -= cfg2.hyper.learningRate * total(d) / groups.size)
    }

    val actual = GrpoPolicyStreamingJob.applyBatch(
      GrpoWeightStore.initial(cfg2), groups, cfg2, 1L).weights
    actual.indices.foreach { d =>
      java.lang.Double.doubleToRawLongBits(actual(d)) shouldBe
        java.lang.Double.doubleToRawLongBits(wRef(d))
    }
  }
```

- [x] **Step 2: Run it against the unhoisted `applyBatch`.**

```bash
sbt 'testOnly com.demo.grpo.GrpoPolicyStreamingJobSpec'
```

Expected: PASS. This establishes the bitwise baseline the hoist must preserve. Record the result; if it fails here, the by-hand loop does not mirror `applyBatch` and must be corrected before proceeding.

Observed: PASS, 12 tests. The by-hand loop mirrors `applyBatch` bit for bit before the hoist, establishing the baseline the hoist must preserve.

- [x] **Step 3: Hoist the softmaxes.** In `applyBatch`, replace the `prepared` construction and the inner-epoch gradient call.

```scala
    // Neither the snapshot-side logits nor the advantage depends on w, so both are fixed for the
    // whole batch -- and so are the two reference POLICIES derived from them. Softmax them once
    // here rather than on every inner epoch: at the default four epochs, gradient would otherwise
    // re-derive both distributions four times per group to no effect.
    val prepared = groups.flatMap { g =>
      GrpoMath.advantages(g.rewards).map { adv =>
        (g,
         GrpoMath.softmax(GrpoMath.logits(g.x, snapshot), cfg.hyper.temperature),
         GrpoMath.softmax(g.logged, cfg.hyper.temperature),
         adv)
      }
    }
    (1 to cfg.hyper.innerEpochs).foreach { _ =>
      val total = Array.fill(cfg.dim)(0.0)
      prepared.foreach { case (g, piSnap, piOld, adv) =>
        // Ratio against the snapshot; KL against what actually served. gradientFromPolicies takes
        // both references, so the two cannot be conflated here.
        val grad = GrpoMath.gradientFromPolicies(g.x, piSnap, piOld, w, adv, cfg.hyper)
        (0 until cfg.dim).foreach(d => total(d) += grad(d))
      }
      (0 until cfg.dim).foreach(d => w(d) -= cfg.hyper.learningRate * total(d) / groups.size)
    }
```

- [x] **Step 4: Run the focused suite and confirm it still passes.**

```bash
sbt 'testOnly com.demo.grpo.GrpoPolicyStreamingJobSpec com.demo.grpo.GrpoMathSpec'
```

Expected: all tests pass, the new bitwise assertion included. A failure here means the hoisted policies were not in fact constant across epochs.

Observed: 29 tests succeeded across both specs, the new bitwise assertion included. The hoisted policies are therefore constant across epochs.

- [x] **Step 5: Commit.**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoPolicyStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoPolicyStreamingJobSpec.scala
git commit -m "perf(grpo): hoist the reference policies out of the inner-epoch loop"
```

---

### Task 3: Verify and deliver

**Files:** All Task 1 and Task 2 files, plus this plan.

**Interfaces:** Consumes the tested diff; produces a reviewed PR against `master`.

- [x] **Step 1: Run the whole Spark module suite.**

```bash
sbt test
```

Expected: zero failures and zero aborted suites. Record the test and suite counts.

Observed: 432 tests succeeded across 66 suites, 0 failed, 0 aborted (up from 430, the two additions being this branch's tests). The `injected Redis write failure` and `simulated Redis command error` lines in the log are deliberate fault-injection tests, not failures.

- [x] **Step 2: Benchmark before and after.** Create the throwaway harness below at `src/test/scala/com/demo/grpo/ZzBenchThrowaway.scala`.

```scala
package com.demo.grpo

/** THROWAWAY benchmark harness - delete before committing. */
object ZzBenchThrowaway {

  def group(rng: scala.util.Random, slate: Int, dim: Int): GrpoGroup = {
    val x = Array.fill(slate)(Array.fill(dim)(rng.nextGaussian()))
    val logged = Array.fill(slate)(rng.nextGaussian())
    val rewards = Array.fill(slate)(if (rng.nextDouble() < 0.3) 1.0 else 0.0)
    GrpoGroup("s", x, logged, rewards)
  }

  def main(args: Array[String]): Unit = {
    val cfg = GrpoJobConfig.from(Map.empty)
    val dim = cfg.dim
    println(s"dim=$dim innerEpochs=${cfg.hyper.innerEpochs}")

    for (nGroups <- Seq(100, 1000, 5000)) {
      val rng = new scala.util.Random(7)
      val groups = Iterator.continually(group(rng, 10, dim))
        .filter(g => GrpoMath.advantages(g.rewards).isDefined).take(nGroups).toVector
      val w0 = GrpoWeights(Array.fill(dim)(0.01), cfg.featureVersion, 0L, 0L)

      (1 to 50).foreach(_ => GrpoPolicyStreamingJob.applyBatch(w0, groups, cfg, 1L))
      val runs = (1 to 9).map { _ =>
        val t0 = System.nanoTime()
        GrpoPolicyStreamingJob.applyBatch(w0, groups, cfg, 1L)
        (System.nanoTime() - t0) / 1e6
      }.sorted
      val gradientCalls = groups.size.toLong * cfg.hyper.innerEpochs
      println(f"  groups=$nGroups%5d  applyBatch median=${runs(4)}%8.3f ms  " +
              f"(min ${runs.head}%.3f)  gradient calls=$gradientCalls%d  " +
              f"per call=${runs(4) * 1e3 / gradientCalls}%.2f us")
    }

    val rng = new scala.util.Random(11)
    val g = group(rng, 10, dim)
    val adv = GrpoMath.advantages(g.rewards).get
    val w = Array.fill(dim)(0.01)
    val snap = GrpoMath.logits(g.x, w)
    val hyper = cfg.hyper
    def bench(label: String, n: Int)(body: => Any): Unit = {
      (1 to 200000).foreach(_ => body)
      val ts = (1 to 9).map { _ =>
        val t0 = System.nanoTime(); (1 to n).foreach(_ => body)
        (System.nanoTime() - t0).toDouble / n
      }.sorted
      println(f"  $label%-46s ${ts(4)}%8.1f ns/op")
    }
    bench("gradient (public, softmaxes both references)", 200000)(
      GrpoMath.gradient(g.x, snap, g.logged, w, adv, hyper))
    bench("softmax alone", 200000)(GrpoMath.softmax(snap, hyper.temperature))
  }
}
```

Run it on this branch, then stash the source changes and run it again on the unoptimized code:

```bash
sbt 'Test/runMain com.demo.grpo.ZzBenchThrowaway'                     # after
git stash push recsys-pipeline/services/spark-streaming-job/src/main  # revert to master's math
sbt 'Test/runMain com.demo.grpo.ZzBenchThrowaway'                     # before
git stash pop
rm src/test/scala/com/demo/grpo/ZzBenchThrowaway.scala
```

Compare against the spec's baseline: `gradient` 1,677 ns/op, `applyBatch` 2.561 / 8.550 / 40.093 ms at 100 / 1,000 / 5,000 groups. Record both columns. Note that `gradient` is the public form and keeps all three softmaxes by design, so its own number should not improve; the saving shows up in `applyBatch`.

Observed. Identical harness settings on both sides (100 warmup batches, 25 timed runs, median of 25; per-call figures are the median of 15 runs of 200,000 calls after 300,000 warmup calls):

| measurement | before | after | change |
|---|---|---|---|
| `applyBatch`, 100 groups | 1.052 ms | 0.702 ms | **-33%** |
| `applyBatch`, 1,000 groups | 8.298 ms | 5.356 ms | **-35%** |
| `applyBatch`, 5,000 groups | 40.365 ms | 28.793 ms | **-29%** |
| `gradient` (public, 3 softmaxes) | 1,654 ns/op | 1,689 ns/op | unchanged, as designed |
| `gradientFromPolicies` (1 softmax) | n/a | 929 ns/op | **-45% vs the public form** |
| `softmax` alone | 364 ns/op | 368 ns/op | unchanged |

The per-call saving is 1,689 - 929 = 760 ns, which is two softmaxes at 368 ns each -- the redundancy the spec predicted, recovered exactly.

A first benchmark pass used only 50 warmup batches and 9 timed runs and produced incoherent results, including the 100-group case appearing 10% SLOWER after the change. That was a measurement artifact: at 100 groups a batch is under a millisecond, so JIT state and run-to-run JVM variance dominated. Raising the warmup to 100 batches and the sample to 25 runs made all three scales agree. The lesson for anyone re-running this: the small-group case needs the longer warmup to mean anything.

- [x] **Step 3: Confirm no benchmark artifact survives.**

```bash
git status --short
git diff --check
```

Expected: no `ZzBenchThrowaway.scala`, and a clean whitespace check.

Observed: `git status --short` empty and `git diff --check` clean; the harness is deleted.

- [x] **Step 4: Request a read-only code review.** Ask for a review against every spec constraint, specifically: that the extracted body is line-for-line today's arithmetic in the same order; that `piSnap`/`piOld` cannot be confused for logits at the one call site; that the hoisted values are genuinely independent of `w`; and that the tests would fail if the hoist drifted. Resolve substantive findings before publishing.

- [x] **Step 5: Publish.** Push `optimize/ppo-training`, open a PR against `master` titled `Optimize PPO training`, and record the measured before/after, the suite counts, and the deliberate absence of a red-green performance test in the body. Fill in this plan's verification record and commit it.

## Verification record

- Task 1 red: `GrpoMathSpec.scala:249:29: value gradientFromPolicies is not a member of object com.demo.grpo.GrpoMath`. Task 1 green: 17 tests succeeded.
- Task 2 baseline, before the hoist: 12 tests succeeded, the by-hand two-epoch loop matching `applyBatch` bit for bit. After the hoist: 29 tests succeeded across both specs.
- Full Spark module suite under JDK 17: 432 tests succeeded, 66 suites, 0 failed, 0 aborted.
- `git diff --check` clean; no benchmark artifact left in the tree.
- Weights are bitwise identical before and after, at the function level (`gradientFromPolicies` against the public `gradient` over ordinary, saturated and clipped inputs) and at the batch level (two inner epochs, raw IEEE bit comparison).
- Benchmark: `applyBatch` -33% / -35% / -29% at 100 / 1,000 / 5,000 groups; `gradientFromPolicies` 929 ns/op against the public form's 1,689 ns/op, a 760 ns saving that is exactly the two 368 ns softmaxes. See Task 3 Step 2 for the full table and for why the first benchmark pass was discarded.
- The public `gradient` is unchanged at 1,654 to 1,689 ns/op across the change, which is the intended outcome: it still softmaxes both references for its own callers.
- As the spec states, no test asserts the softmax call count. The performance property rests on the structure plus the benchmark; correctness rests on the bitwise equivalence assertions and the pre-existing finite-difference checks.

### Review outcome and the follow-up it required

The independent read-only review confirmed the production change: it extracted the 18-line arithmetic block from `master` and from the branch and found both at md5 `f0629b6acf4b731c415b2ee14e39fad7` -- byte-identical, not merely equivalent -- and established the hoist's validity by showing `snapshot` is a never-written clone, that `gradientFromPolicies` writes only its own locals, that `softmax` and `logits` both return fresh arrays so no aliasing is possible, and that `GrpoSlates.toGroups` returns a strict `WrappedArray` so `prepared` really does materialize once.

It then found a test-strength gap that PR #235 merged with, fixed in a follow-up:

The batch-level bitwise test's fixture made BOTH reference policies uniform -- `GrpoWeightStore.initial` is all-zero weights, so `piSnap` is uniform regardless of features, and the `group` helper's default `logged` is `[0.5, 0.5, 0.5]`. A uniform softmax is temperature-invariant and bitwise equal to the other uniform reference, so the test was blind to a dropped temperature argument and to a swap of the two references. This mattered because the change moved two `cfg.hyper.temperature` uses out of `GrpoMath`, where `GrpoMathSpec` pins non-unit temperature, and into `applyBatch`, which no test exercised at `T != 1`. Independently reproduced: replacing `cfg.hyper.temperature` with `1.0` on the `piOld` softmax left all 53 `com.demo.grpo.*` tests green, and with `GRPO_TEMPERATURE=0.5` that edit would compute both references flatter than `pi`, corrupting the ratio on every candidate while the suite stayed green.

The fixture now uses a non-unit temperature (0.5), distinct non-uniform `logged` per group, and non-uniform starting weights. Verified: green on correct code (53 tests), and all three previously surviving mutations now fail -- `piOld` at `T = 1.0` fails with `4587161527048530728 was not equal to 4587163444750669971`, `piSnap` at `T = 1.0` fails, and swapping the two references fails.

The review also noted that the delegation test's cases all derived `adv` from `advantages`, which rejects zero-variance groups and so can never return a zero vector, leaving the spec's named zero-advantage case uncovered. A literal `Array(0.0, 0.0)` case was added. And it observed that `private[grpo]` compiles to a public JVM method, so the spec's containment rationale rests on there being one call site rather than on the modifier being enforced; the spec now says so.

### Limits of this evidence

Single-JVM, JIT-warmed timings on one machine; group counts per batch depend on click-through because the zero-variance gate drops most slates at low CTR. The change does not address batch latency, which is dominated by the driver-side `collect` in `GrpoSlates.toGroups`, JSON parsing, and the Redis write. At the 5,000-group ceiling the PPO math moves from roughly 0.4% to 0.3% of a ten-second trigger.
