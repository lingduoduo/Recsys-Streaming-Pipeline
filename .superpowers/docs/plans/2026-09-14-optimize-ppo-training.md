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

- [ ] **Step 1: Commit the spec and plan on a branch off `master`.**

```bash
git checkout master && git pull
git checkout -b optimize/ppo-training
git add .superpowers/docs/specs/2026-09-14-optimize-ppo-training-design.md \
        .superpowers/docs/plans/2026-09-14-optimize-ppo-training.md
git commit -m "docs: specify the PPO training optimization"
```

- [ ] **Step 2: Push and open a draft PR against `master`** titled `Optimize PPO training`, whose body states the problem, the measured baseline, and that code follows in this same PR. Use `gh pr create --draft --body-file`. Record the PR number here; the spec and plan are then reviewable before any code exists.

---

### Task 1: Split the gradient at the softmax boundary

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala:104-125`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala`

**Interfaces:**
- Consumes: `GrpoMath.softmax(logits: Array[Double], temperature: Double): Array[Double]` and `GrpoMath.logits(x: Array[Array[Double]], w: Array[Double]): Array[Double]`, both unchanged.
- Produces: `private[grpo] def gradientFromPolicies(x: Array[Array[Double]], piSnap: Array[Double], piOld: Array[Double], w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double]`. Task 2 calls this. The public `gradient` keeps its existing signature and becomes a wrapper.

- [ ] **Step 1: Write the failing test.** Append to `GrpoMathSpec.scala`. It calls a method that does not exist yet, so it fails to compile until Step 3.

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

- [ ] **Step 2: Run it and confirm it fails.**

```bash
sbt 'testOnly com.demo.grpo.GrpoMathSpec'
```

Expected: a compile error reporting that `gradientFromPolicies` is not a member of `GrpoMath`. Record the message.

- [ ] **Step 3: Extract the method.** In `GrpoMath.scala`, replace the existing `gradient` with the wrapper plus the extracted worker. The worker body is today's `gradient` body with the `policies` call replaced by deriving `pi` alone; every other line, including operation order, is unchanged.

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

- [ ] **Step 4: Run the focused suite and confirm it passes.**

```bash
sbt 'testOnly com.demo.grpo.GrpoMathSpec'
```

Expected: all tests pass, including the pre-existing finite-difference, clipping, KL, and two-reference-separation cases. Record the count.

- [ ] **Step 5: Commit.**

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

- [ ] **Step 1: Write the failing test.** Append to `GrpoPolicyStreamingJobSpec.scala`. It pins bitwise equality against a by-hand loop built from the public `gradient`, at two inner epochs, which is where the recomputation happened. Written before Task 2's change, it passes; it exists to fail if the hoist ever drifts, so run it again after Step 3.

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

- [ ] **Step 2: Run it against the unhoisted `applyBatch`.**

```bash
sbt 'testOnly com.demo.grpo.GrpoPolicyStreamingJobSpec'
```

Expected: PASS. This establishes the bitwise baseline the hoist must preserve. Record the result; if it fails here, the by-hand loop does not mirror `applyBatch` and must be corrected before proceeding.

- [ ] **Step 3: Hoist the softmaxes.** In `applyBatch`, replace the `prepared` construction and the inner-epoch gradient call.

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

- [ ] **Step 4: Run the focused suite and confirm it still passes.**

```bash
sbt 'testOnly com.demo.grpo.GrpoPolicyStreamingJobSpec com.demo.grpo.GrpoMathSpec'
```

Expected: all tests pass, the new bitwise assertion included. A failure here means the hoisted policies were not in fact constant across epochs.

- [ ] **Step 5: Commit.**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoPolicyStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoPolicyStreamingJobSpec.scala
git commit -m "perf(grpo): hoist the reference policies out of the inner-epoch loop"
```

---

### Task 3: Verify and deliver

**Files:** All Task 1 and Task 2 files, plus this plan.

**Interfaces:** Consumes the tested diff; produces a reviewed PR against `master`.

- [ ] **Step 1: Run the whole Spark module suite.**

```bash
sbt test
```

Expected: zero failures and zero aborted suites. Record the test and suite counts.

- [ ] **Step 2: Benchmark before and after.** Create the throwaway harness below at `src/test/scala/com/demo/grpo/ZzBenchThrowaway.scala`.

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

- [ ] **Step 3: Confirm no benchmark artifact survives.**

```bash
git status --short
git diff --check
```

Expected: no `ZzBenchThrowaway.scala`, and a clean whitespace check.

- [ ] **Step 4: Request a read-only code review.** Ask for a review against every spec constraint, specifically: that the extracted body is line-for-line today's arithmetic in the same order; that `piSnap`/`piOld` cannot be confused for logits at the one call site; that the hoisted values are genuinely independent of `w`; and that the tests would fail if the hoist drifted. Resolve substantive findings before publishing.

- [ ] **Step 5: Publish.** Push `optimize/ppo-training`, open a PR against `master` titled `Optimize PPO training`, and record the measured before/after, the suite counts, and the deliberate absence of a red-green performance test in the body. Fill in this plan's verification record and commit it.

## Verification record

Execution pending.
