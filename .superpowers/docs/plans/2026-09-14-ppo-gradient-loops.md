# PPO Gradient Loops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut the cost of `GrpoMath.gradientFromPolicies` by about 3.6x without moving a single bit of its output.

**Architecture:** Replace the body's collection combinators with indexed `while` loops, fusing the logits and softmax computations into one preallocated row buffer that becomes the logits, then the exponentials, then `pi`. Nothing outside that one function body changes.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17.

**Spec:** [PPO gradient loops design](../specs/2026-09-14-ppo-gradient-loops-design.md)

## Global Constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17; no new dependencies.
- `gradientFromPolicies` must return a bitwise identical vector for every input. Weights out of `applyBatch` must be bitwise identical to today's.
- Preserve `gradient`, `loss`, `logits`, `softmax`, `kl`, `advantages` and `policies` exactly as they are. Only the body of `gradientFromPolicies` changes.
- Preserve the operation order: the dot product accumulates over features ascending, the expected-feature and gradient accumulations stay candidate-major, and the softmax normalizes by a total summed over candidates ascending.
- Preserve `AdvantageFloor` and every floor it guards, the clipped-branch selection rule, and the division by the candidate count.
- Allocate no more than the existing function did: one row-sized buffer, one expected-feature vector, one gradient vector.
- Do not mutate `x`, `piSnap`, `piOld`, `w` or `adv`.

## Environment

sbt must run under JDK 17; the default JDK aborts Spark-session tests with a misleading `getSubject` error. All sbt commands run from `recsys-pipeline/services/spark-streaming-job`:

```bash
export JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home
```

## File Structure

- `src/main/scala/com/demo/grpo/GrpoMath.scala` — only `gradientFromPolicies`'s body. Every other member is untouched.
- `src/test/scala/com/demo/grpo/GrpoMathSpec.scala` — gains the randomized bitwise equivalence test against a frozen copy of the current body, the enumerated edge cases, and the NaN test.

---

### Task 0: Publish the design first

**Files:**
- Create: `.superpowers/docs/specs/2026-09-14-ppo-gradient-loops-design.md`
- Create: `.superpowers/docs/plans/2026-09-14-ppo-gradient-loops.md`

**Interfaces:** Produces the branch and draft PR that Tasks 1-2 commit into. No code.

- [x] **Step 1: Commit the spec and plan on a branch off `master`.**

```bash
git checkout master && git pull
git checkout -b perf/ppo-gradient-loops
git add .superpowers/docs/specs/2026-09-14-ppo-gradient-loops-design.md \
        .superpowers/docs/plans/2026-09-14-ppo-gradient-loops.md
git commit -m "docs: specify the PPO gradient loop rewrite"
```

- [x] **Step 2: Push and open a draft PR against `master`** titled `Rewrite the PPO gradient with indexed loops`, whose body carries the measured 1,158 to 322 ns/op prototype figure, the note that this is not a bottleneck, and the NaN caveat. Use `gh pr create --draft --body-file`. Record the PR number here.

---

### Task 1: Rewrite the gradient body with indexed loops

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala` — the body of `gradientFromPolicies` only
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala`

**Interfaces:**
- Consumes: `GrpoMath.AdvantageFloor`, and `GrpoHyperParams`'s `temperature`, `clipEpsilon` and `klBeta`. It no longer calls `logits` or `softmax`, which stay public and unchanged for their other callers.
- Produces: no signature change. `private[grpo] def gradientFromPolicies(x: Array[Array[Double]], piSnap: Array[Double], piOld: Array[Double], w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double]`.

- [x] **Step 1: Add the frozen oracle and the equivalence tests.** Append to `GrpoMathSpec.scala`. `_combinatorGradient` is a frozen copy of today's body; it is the oracle and must not be re-pointed at the production function later.

```scala
  /** A frozen copy of the combinator-based body this rewrite replaces, as the equivalence oracle.
    *
    * Deliberately duplicated rather than delegating: comparing the rewritten loops against a copy
    * of themselves would assert nothing. If GrpoMath.gradientFromPolicies is ever rewritten again,
    * this stays as it is.
    */
  private def combinatorGradient(x: Array[Array[Double]], piSnap: Array[Double],
                                 piOld: Array[Double], w: Array[Double], adv: Array[Double],
                                 c: GrpoHyperParams): Array[Double] = {
    val dim = w.length
    val pi = GrpoMath.softmax(GrpoMath.logits(x, w), c.temperature)
    val expected = Array.fill(dim)(0.0)
    pi.indices.foreach(i => (0 until dim).foreach(d => expected(d) += pi(i) * x(i)(d)))
    val grad = Array.fill(dim)(0.0)
    pi.indices.foreach { i =>
      val piSnapFloored = math.max(piSnap(i), GrpoMath.AdvantageFloor)
      val ratio = pi(i) / piSnapFloored
      val clipped = math.max(1.0 - c.clipEpsilon, math.min(1.0 + c.clipEpsilon, ratio))
      val unclippedSelected = ratio * adv(i) <= clipped * adv(i)
      val surrogateScale = if (unclippedSelected) -adv(i) / (piSnapFloored * pi.length) else 0.0
      val klScale = c.klBeta *
        math.log(math.max(pi(i), GrpoMath.AdvantageFloor) / math.max(piOld(i), GrpoMath.AdvantageFloor))
      val scale = (surrogateScale + klScale) * pi(i) / c.temperature
      (0 until dim).foreach(d => grad(d) += scale * (x(i)(d) - expected(d)))
    }
    grad
  }

  private def sameBits(a: Array[Double], b: Array[Double]): Boolean =
    a.length == b.length && a.indices.forall(d =>
      java.lang.Double.doubleToRawLongBits(a(d)) == java.lang.Double.doubleToRawLongBits(b(d)))

  it should "match the combinator gradient bit for bit across randomized shapes" in {
    // The rewrite's whole claim is that indexed loops in the original order move no bits. Random
    // shapes are the only way to exercise the loop bounds; the enumerated cases below cover the
    // boundaries that random draws will not reliably hit.
    val rng = new scala.util.Random(3)
    var compared = 0
    (1 to 2000).foreach { _ =>
      val slate = 2 + rng.nextInt(12)
      val dim = 9
      val x = Array.fill(slate)(Array.fill(dim)(rng.nextGaussian()))
      val rewards = Array.fill(slate)(if (rng.nextDouble() < 0.3) 1.0 else 0.0)
      GrpoMath.advantages(rewards).foreach { adv =>
        val w = Array.fill(dim)(rng.nextGaussian())
        val hyper = cfg.copy(klBeta = if (rng.nextBoolean()) 0.0 else 0.02,
                             temperature = if (rng.nextBoolean()) 1.0 else 0.5)
        val piSnap = GrpoMath.softmax(GrpoMath.logits(x, Array.fill(dim)(rng.nextGaussian())),
                                      hyper.temperature)
        val piOld = GrpoMath.softmax(Array.fill(slate)(rng.nextGaussian()), hyper.temperature)
        val want = combinatorGradient(x, piSnap, piOld, w, adv, hyper)
        val got = GrpoMath.gradientFromPolicies(x, piSnap, piOld, w, adv, hyper)
        withClue(s"slate=$slate temperature=${hyper.temperature} klBeta=${hyper.klBeta}: ") {
          sameBits(got, want) shouldBe true
        }
        compared += 1
      }
    }
    compared should be > 1000   // the variance gate rejects some draws; most must survive
  }

  it should "match the combinator gradient bit for bit on the boundary cases" in {
    val two = Array(Array(1.0, 0.0), Array(0.0, 1.0))
    val shared = Array(Array(0.5, 0.5), Array(0.5, 0.5), Array(0.5, 0.5))
    val cases = Seq(
      ("two candidates", two, Array(0.3, 0.3), Array(0.1, 0.2), Array(0.5, -0.25),
       Array(1.0, -1.0), cfg),
      ("saturated logits", two, Array(1e6, -1e6), Array(-1e6, 1e6), Array(1e6, -1e6),
       Array(1.0, -1.0), cfg),
      ("zero advantage", two, Array(0.3, 0.3), Array(0.1, 0.2), Array(0.5, -0.25),
       Array(0.0, 0.0), cfg),
      ("non-unit temperature", two, Array(0.3, 0.3), Array(0.1, 0.2), Array(0.5, -0.25),
       Array(1.0, -1.0), cfg.copy(temperature = 0.25)),
      ("klBeta zero", two, Array(0.3, 0.3), Array(0.1, 0.2), Array(0.5, -0.25),
       Array(1.0, -1.0), cfg.copy(klBeta = 0.0)),
      ("identical feature vectors", shared, Array(0.3, 0.3, 0.3), Array(0.2, 0.2, 0.2),
       Array(0.5, -0.25), Array(1.0, -0.5, -0.5), cfg))

    cases.foreach { case (label, x, snapshotLogits, loggedLogits, w, adv, hyper) =>
      val piSnap = GrpoMath.softmax(snapshotLogits, hyper.temperature)
      val piOld = GrpoMath.softmax(loggedLogits, hyper.temperature)
      withClue(s"$label: ") {
        sameBits(GrpoMath.gradientFromPolicies(x, piSnap, piOld, w, adv, hyper),
                 combinatorGradient(x, piSnap, piOld, w, adv, hyper)) shouldBe true
      }
    }
  }

  it should "poison the gradient when a feature is NaN, keeping divergence detectable" in {
    // The fused loop takes its maximum with `>`, which is false against NaN, where the public
    // softmax uses Array.max and its Ordering[Double]. The two can disagree on which element wins
    // when a logit is NaN. Either way the gradient carries NaN, which is what stepBatch keys on:
    // a NaN logit means the weights already diverged and the batch must be discarded.
    val x = Array(Array(Double.NaN, 0.0), Array(0.0, 1.0))
    val piSnap = GrpoMath.softmax(Array(0.3, 0.3), cfg.temperature)
    val piOld = GrpoMath.softmax(Array(0.1, 0.2), cfg.temperature)
    val grad = GrpoMath.gradientFromPolicies(x, piSnap, piOld, Array(1.0, 1.0),
                                             Array(1.0, -1.0), cfg)
    grad.exists(v => java.lang.Double.isNaN(v)) shouldBe true
  }
```

- [x] **Step 2: Run the focused spec and confirm the three new tests pass against the current body.**

```bash
sbt 'testOnly com.demo.grpo.GrpoMathSpec'
```

Expected: all pass. The equivalence tests compare the current body against a copy of itself, so they are green here by construction — that is what makes them a valid oracle once the body changes. Record the count, and record the NaN test's outcome, which establishes the pre-change behavior the rewrite must preserve.

- [x] **Step 3: Rewrite the body.** Replace everything between `private[grpo] def gradientFromPolicies(` ... `): Array[Double] = {` and the closing `grad` with the following. Keep the scaladoc above it, and append the new paragraph shown.

```scala
  private[grpo] def gradientFromPolicies(
      x: Array[Array[Double]], piSnap: Array[Double], piOld: Array[Double],
      w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double] = {
    val dim = w.length
    val n = x.length

    // One row-sized workspace carries the logits, then the exponentials, then pi. Indexed while
    // loops rather than `map`/`foldLeft`/`Range.foreach`: the combinators cost about 72% of this
    // function -- foldLeft boxes the accumulator per feature, softmax allocates three arrays where
    // one suffices, and each `(0 until dim).foreach` builds a Range and calls a Function1 per
    // element. Every loop keeps the original order, so the result is bitwise identical, which is
    // what GrpoMathSpec's randomized bit comparison against the frozen combinator body asserts.
    val pi = new Array[Double](n)
    var i = 0
    while (i < n) {
      val row = x(i)
      var acc = 0.0
      var k = 0
      while (k < dim) { acc += row(k) * w(k); k += 1 }
      pi(i) = acc / cfg.temperature
      i += 1
    }
    // Subtract the max before exp, or large logits overflow. `>` is false against NaN, so a NaN
    // logit leaves the running max in place where the public softmax's Array.max may pick it; both
    // poison the gradient, and stepBatch discards the batch either way.
    var max = pi(0)
    i = 1
    while (i < n) { if (pi(i) > max) max = pi(i); i += 1 }
    var total = 0.0
    i = 0
    while (i < n) { pi(i) = math.exp(pi(i) - max); total += pi(i); i += 1 }
    i = 0
    while (i < n) { pi(i) = pi(i) / total; i += 1 }

    // Expected feature vector under pi -- the term that makes d log pi_i / dw a centred difference.
    val expected = new Array[Double](dim)
    i = 0
    while (i < n) {
      val p = pi(i)
      val row = x(i)
      var d = 0
      while (d < dim) { expected(d) += p * row(d); d += 1 }
      i += 1
    }

    val grad = new Array[Double](dim)
    i = 0
    while (i < n) {
      val piSnapFloored = math.max(piSnap(i), AdvantageFloor)
      val ratio = pi(i) / piSnapFloored                          // ratio: snapshot reference
      val clipped = math.max(1.0 - cfg.clipEpsilon, math.min(1.0 + cfg.clipEpsilon, ratio))
      // The surrogate moves with w only when min() kept the raw ratio; the clip bound is flat.
      val unclippedSelected = ratio * adv(i) <= clipped * adv(i)
      val surrogateScale = if (unclippedSelected) -adv(i) / (piSnapFloored * n) else 0.0
      val klScale = cfg.klBeta *
        math.log(math.max(pi(i), AdvantageFloor) / math.max(piOld(i), AdvantageFloor))
      val scale = (surrogateScale + klScale) * pi(i) / cfg.temperature
      val row = x(i)
      var d = 0
      while (d < dim) { grad(d) += scale * (row(d) - expected(d)); d += 1 }
      i += 1
    }
    grad
  }
```

Append this paragraph to the existing scaladoc, after the sentence ending "so only `pi` is computed here.":

```
    * The body is written as indexed while loops over preallocated arrays rather than through
    * `logits` and `softmax`, which stay public and unchanged for their other callers. This is the
    * one function called per group per inner epoch, and the combinators cost about 72% of it.
```

- [x] **Step 4: Run the focused spec and confirm the equivalence tests still pass.**

```bash
sbt 'testOnly com.demo.grpo.GrpoMathSpec'
```

Expected: all pass, now with the rewritten body compared against the frozen combinator oracle. A failure here names the slate size, temperature and klBeta of the first divergence.

- [x] **Step 5: Run the job spec, whose two-epoch bit comparison covers `applyBatch`.**

```bash
sbt 'testOnly com.demo.grpo.GrpoPolicyStreamingJobSpec'
```

Expected: all pass. That test already runs at temperature 0.5 with non-uniform references.

- [x] **Step 6: Run the whole Spark module suite.**

```bash
sbt test
```

Expected: zero failures, zero aborted suites. Record the counts.

- [x] **Step 7: Commit.**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala
git commit -m "perf(grpo): indexed loops in the PPO gradient"
```

---

### Task 2: Verify and deliver

**Files:** Task 1 files, plus this plan.

**Interfaces:** Consumes the tested diff; produces a reviewed PR against `master`.

- [x] **Step 1: Benchmark before and after.** Create the throwaway harness below at `src/test/scala/com/demo/grpo/ZzBenchThrowaway.scala`, run it on this branch, then restore `GrpoMath.scala` from `master` and run it again. Delete the harness afterwards.

```scala
package com.demo.grpo

/** THROWAWAY benchmark harness - delete before committing. */
object ZzBenchThrowaway {

  def group(rng: scala.util.Random, slate: Int, dim: Int): GrpoGroup = {
    val x = Array.fill(slate)(Array.fill(dim)(rng.nextGaussian()))
    GrpoGroup("s", x, Array.fill(slate)(rng.nextGaussian()),
              Array.fill(slate)(if (rng.nextDouble() < 0.3) 1.0 else 0.0))
  }

  def stats(ts: Seq[Double]): String = {
    val s = ts.sorted
    f"median=${s(s.size / 2)}%9.3f  min=${s.head}%9.3f"
  }

  def main(args: Array[String]): Unit = {
    val cfg = GrpoJobConfig.from(Map.empty)
    val dim = cfg.dim
    val hyper = cfg.hyper
    println(s"dim=$dim innerEpochs=${hyper.innerEpochs}")

    for (nGroups <- Seq(100, 1000, 5000)) {
      val rng = new scala.util.Random(7)
      val groups = Iterator.continually(group(rng, 10, dim))
        .filter(g => GrpoMath.advantages(g.rewards).isDefined).take(nGroups).toVector
      val w0 = GrpoWeights(Array.fill(dim)(0.01), cfg.featureVersion, 0L, 0L)
      (1 to 100).foreach(_ => GrpoPolicyStreamingJob.applyBatch(w0, groups, cfg, 1L))
      val runs = (1 to 25).map { _ =>
        val t0 = System.nanoTime()
        GrpoPolicyStreamingJob.applyBatch(w0, groups, cfg, 1L)
        (System.nanoTime() - t0) / 1e6
      }
      println(f"  applyBatch groups=$nGroups%5d ms  ${stats(runs)}")
    }

    val rng = new scala.util.Random(11)
    val g = group(rng, 10, dim)
    val adv = GrpoMath.advantages(g.rewards).get
    val w = Array.fill(dim)(0.01)
    val piSnap = GrpoMath.softmax(GrpoMath.logits(g.x, w), hyper.temperature)
    val piOld = GrpoMath.softmax(g.logged, hyper.temperature)
    (1 to 300000).foreach(_ => GrpoMath.gradientFromPolicies(g.x, piSnap, piOld, w, adv, hyper))
    val ts = (1 to 15).map { _ =>
      val t0 = System.nanoTime()
      (1 to 200000).foreach(_ => GrpoMath.gradientFromPolicies(g.x, piSnap, piOld, w, adv, hyper))
      (System.nanoTime() - t0).toDouble / 200000
    }
    println(f"  gradientFromPolicies ns/op  ${stats(ts)}")
  }
}
```

```bash
sbt 'Test/runMain com.demo.grpo.ZzBenchThrowaway'                                  # after
git checkout master -- src/main/scala/com/demo/grpo/GrpoMath.scala
sbt 'Test/runMain com.demo.grpo.ZzBenchThrowaway'                                  # before
git checkout HEAD -- src/main/scala/com/demo/grpo/GrpoMath.scala
rm src/test/scala/com/demo/grpo/ZzBenchThrowaway.scala
```

Note the harness only calls public members plus `gradientFromPolicies`, which is package-visible and reachable, so it compiles against both versions. Compare against the spec's baseline: 1,158 ns/op and 0.702 / 5.356 / 28.793 ms. Record both columns. Use at least 100 warmup batches — a prior PPO benchmark with 50 produced incoherent sub-millisecond results.

- [x] **Step 2: Confirm the tree is clean.**

```bash
git status --short
git diff --check
```

Expected: no harness, no stray `GrpoMath.scala` modification, clean whitespace.

- [x] **Step 3: Request a read-only code review.** Ask specifically: that the rewritten arithmetic is the same operations in the same order as the frozen oracle, checked term by term rather than by trusting the tests; that `n` correctly replaces `pi.length` in the surrogate denominator; that no loop reads an index it has already overwritten, given `pi` is written three times in place; that nothing mutates `x`, `piSnap`, `piOld`, `w` or `adv`; and which plausible transcription errors the randomized bit comparison would miss. Resolve substantive findings before publishing.

- [x] **Step 4: Publish.** Push, record the measured before and after plus suite counts in the PR body, fill in this plan's verification record, commit it, and mark the PR ready.

## Verification record

- Task 1 Step 2, the oracle validated against the unchanged body: 20 tests pass, the three new ones included. Green by construction at that point -- the frozen `combinatorGradient` was compared against the body it copies -- which is exactly what makes it a valid oracle once the body changes. The NaN test also passed here, establishing the pre-change behavior.
- Task 1 Step 4 and 5, after the rewrite: 32 tests across `GrpoMathSpec` and `GrpoPolicyStreamingJobSpec`. The randomized comparison found zero bit differences over 2,000 drawn shapes, the six boundary cases matched, and `applyBatch`'s two-epoch bit comparison -- which runs at temperature 0.5 with non-uniform references since PR #237 -- still holds.
- Task 1 Step 6, full Spark module suite under JDK 17: 435 tests succeeded, 66 suites, 0 failed, 0 aborted. The `simulated Redis command error` lines in the log are deliberate fault-injection tests.
- `git diff --check` clean; no benchmark artifact left in the tree.
- Benchmark, identical harness both sides, 100 warmup batches and 25 timed runs for `applyBatch`, 300,000 warmup and 15 runs of 200,000 calls for the per-call figure:

| measurement | before | after | change |
|---|---|---|---|
| `gradientFromPolicies` | 1,046.5 ns/op | 321.6 ns/op | **-69%, a factor of 3.25** |
| `applyBatch`, 100 groups | 0.958 ms | 0.675 ms | -30% |
| `applyBatch`, 1,000 groups | 5.394 ms | 2.955 ms | **-45%** |
| `applyBatch`, 5,000 groups | 29.896 ms | 15.082 ms | **-50%** |

Taken with PR #235, which brought the 5,000-group batch from 40.4 ms to 29.9 ms, the two changes together take it to 15.1 ms -- a factor of 2.7 on the PPO learning rule.

### Review outcome

Independent read-only review: APPROVE. It verified the transcription term by term against `master` rather than by trusting the tests, then re-implemented both bodies in a standalone harness and compared raw bits over 3,412 slates with zero differences. It confirmed the fused division sits at the same sequence position as `softmax`'s separate scaling pass, that `n` and `pi.length` cannot diverge because both `map`s preserved length, that no pass reads an element it has overwritten, and that nothing writes into caller data. Of its mutations, `<` for `>` in the max loop, `total` accumulated before the store, and `dim` substituted for `n` were all caught.

It found one Important gap, since fixed:

**Every temperature in the suite was a power of two, which blinded the bit comparison to the one ordering error this fusion invites.** Division by a power of two is exact, so `(acc / T)` before the max subtraction and `(acc - max) / T` after it agree bit for bit at 1.0, 0.5 and 0.25 — the only temperatures anywhere in the suite, including the `non-unit temperature` boundary case and `GrpoPolicyStreamingJobSpec`'s 0.5. Independently reproduced: that mutation passed all 20 `GrpoMathSpec` tests. The reviewer measured it diverging on 2,593 of 3,412 draws at 0.7. `GrpoJobConfig` accepts any positive `GRPO_TEMPERATURE`, so an operator at 0.7 would have got different weights from a body the suite called bitwise identical.

The randomized draw now picks from 1.0, 0.5 and 0.7, with a 0.7 boundary case beside it. Verified: the mutation now fails with `slate=11 temperature=0.7`.

Two suggestions also adopted. An all-negative-logits boundary case was added, because the reviewer measured the `max = 0.0, i = 0` seeding error catching on only 65 of 3,412 random draws — enough with this seed, fragile to a reseed; it now fails the boundary test too. And the NaN claim was tightened in both directions: the reviewer showed the rewrite agrees with the combinators under NaN far more strongly than documented, which turned out to be almost right. Asserting raw-bit equality under NaN actually FAILED: with a NaN logit beside an infinite one the loops yield `7ff8000000000000` and the combinators `fff8000000000000`, differing in the sign bit, because the two take different maxima and the NaN then propagates through a different subtraction. Both are NaN and nothing can tell them apart, so the NaN test now compares with NaN equal to NaN and additionally asserts every element is NaN — across a NaN feature, a NaN weight, a NaN beside an infinity, and a NaN behind a large finite logit.

A third suggestion was adopted as documentation only: the spec's "candidate-major" constraint read as a correctness requirement, but swapping the nesting leaves each accumulator's order unchanged and measured bit-identical, so it is an allocation and locality choice. The spec now says so.

Two input shapes genuinely change behavior, both unreachable from the job and now recorded in the spec's limits: an empty slate throws a different exception, and a feature row longer than the weight vector is silently truncated where `logits` used to throw. `GrpoSlates` rejects both shapes before they reach here, but `gradient` is public.

### One equivalence question worth recording

The fused path divides by the temperature inside the dot-product loop, where the old path computed raw logits in `logits` and divided in `softmax`'s first pass. The sequence is nonetheless identical: `acc / temperature` produces exactly the value `logits.map(_ / temperature)` produced, the maximum is taken over the divided values in both, `exp(v - max)` follows in both, the total accumulates over candidates ascending in both, and the final division by the total is the same.

The one place the two genuinely differ is the maximum itself. The fused loop compares with a strict inequality; `Array.max` goes through `Ordering[Double]`, which orders by `java.lang.Double.compare` and so ranks `-0.0` below `0.0` and `NaN` above everything. Neither difference reaches the output. For a `-0.0` against `0.0` maximum, `v - max` is `0.0` either way and `exp` of both `0.0` and `-0.0` is `1.0`. For a NaN logit, the old path's maximum is NaN and poisons every element, while the fused path keeps a finite maximum and poisons the one element plus the total, which then poisons every element through the final division -- both end all-NaN, and the batch is discarded by `stepBatch` regardless.

### Limits of this evidence

Single-JVM, JIT-warmed timings on one machine; JIT decisions for hand-rolled loops are more hardware- and version-sensitive than for the combinators they replace. The change also costs readability: indexed loops are harder to read than `pi.indices.foreach`, over the arithmetic in this repository least forgiving of a transcription error. That is why acceptance rests on bitwise comparison against a frozen oracle rather than on review alone. And as the spec insists, this is not batch-latency relief: 15 ms against a ten-second trigger is about 0.15% of the interval, with the driver-side `collect` in `GrpoSlates.toGroups` still dominating a micro-batch.
