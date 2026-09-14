# Optimize PPO training design

## Problem and decision

The PPO stage is `GrpoMath.gradient`, driven by `GrpoPolicyStreamingJob.applyBatch` once per surviving slate per inner epoch. `gradient` derives three policies on every call: `pi` from the live weights, `piSnap` from the batch snapshot, and `piOld` from the logged serving policy. Only `pi` depends on `w`. The other two are fixed for the whole micro-batch, yet they are recomputed on every inner epoch.

`applyBatch` already hoists the snapshot logits and the advantages out of the inner-epoch loop for exactly this reason. It hoists the logits but not the softmax of them, so the saving stops one step short.

Split `gradient` at the softmax boundary: a package-visible `gradientFromPolicies` takes the two fixed policies, and the public `gradient` becomes a thin wrapper that softmaxes both references and delegates. `applyBatch` computes both policies once per group per batch and calls the inner form. This mirrors `CtrRankingModelTrainingJob.splitByDateWithCount`, where a package-visible worker carries the extra result and the public method stays a compatibility wrapper.

Alternatives considered. Changing `gradient`'s own signature to take policies would churn roughly seventeen verified call sites and remove the public form that the finite-difference tests exercise. Memoizing inside `GrpoMath` would make a documented pure function stateful and unsafe under the concurrent access a streaming job can produce. Neither earns its cost against a two-line delegation.

## Global constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17; no new dependencies.
- Preserve `gradient(x, snapshotLogits, loggedLogits, w, adv, cfg)` and its returned vector exactly. Weights from `applyBatch` must be bitwise identical to today's, not merely within a tolerance.
- Preserve the two-reference separation: the ratio measures against the batch snapshot and the KL anchors to the logged policy. Collapsing them is the silent shadow-mode failure the existing comments describe, and no change may make it expressible.
- Preserve `softmax`'s max-subtraction, `AdvantageFloor` and every floor it guards, the clipped-branch selection rule, and the operation order inside the gradient accumulation.
- Leave `loss`, `advantages`, `kl`, `logits`, and `softmax` unchanged. `loss` has no production caller and stays on the public path.
- Do not mutate caller-owned arrays and do not retain state across calls.

## Implementation

Extract the body of `gradient` after the `policies` call into

```scala
private[grpo] def gradientFromPolicies(
    x: Array[Array[Double]], piSnap: Array[Double], piOld: Array[Double],
    w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double]
```

which derives `pi` from `w` itself and consumes the two supplied policies. The public `gradient` softmaxes `snapshotLogits` and `loggedLogits` at `cfg.temperature` and delegates, so its behavior is unchanged. `policies` remains in place for `loss`.

In `applyBatch`, `prepared` carries `(group, piSnap, piOld, advantages)` instead of `(group, snapshotLogits, advantages)`, computing both softmaxes once per group, and the inner-epoch loop calls `gradientFromPolicies`. The existing comment is extended to say the reference policies are hoisted, not only the logits.

`piSnap` and `piOld` are probabilities but carry the same `Array[Double]` type as logits, so a caller can pass the wrong one without a compile error. The mitigations are that the method is package-visible rather than public, that its parameters and scaladoc name them as policies, and that exactly one production call site exists. A wrapper type for a single caller is not justified.

## Validation and acceptance

1. `gradientFromPolicies(x, softmax(snapshotLogits, t), softmax(loggedLogits, t), w, adv, cfg)` returns bitwise the same vector as `gradient(x, snapshotLogits, loggedLogits, w, adv, cfg)`, over ordinary, saturated, zero-advantage, and clipped-branch inputs.
2. `applyBatch` returns bitwise identical weights to a by-hand inner-epoch loop that calls the public `gradient`, at two or more inner epochs, which is where the recomputation occurred. `GrpoPolicyStreamingJobSpec` already performs this comparison at a 1e-9 tolerance; a bitwise assertion is added alongside rather than replacing it.
3. The existing finite-difference checks of the analytic gradient, the two-reference separation tests, the clipping and KL tests, and the non-finite guard in `stepBatch` all continue to pass unchanged.
4. The Spark module suite passes under JDK 17, and `git diff --check` is clean.
5. Benchmark evidence records `gradient` nanoseconds per operation and `applyBatch` milliseconds at 100, 1,000, and 5,000 surviving groups, before and after.

There is deliberately **no red-green test for the performance property**. Counting `softmax` invocations would require an injection seam existing only for the test, against this file's style and the project's simplicity rules. The redundancy's removal is established structurally, because `softmax` is no longer reachable from the per-epoch call, and quantitatively by the benchmark; the equivalence assertions above are the behavioral guard. This is a knowing deviation from the usual red-green cycle and is called out here for review.

## Measured baseline and limits

Baseline on the current implementation, JDK 17.0.12, Scala 2.12.18, `dim` 9, ten-item slates, `innerEpochs` 4, median of nine timed runs after 50 warmup batches: `gradient` 1,677 ns/op, of which each of the three `softmax` calls is 369 ns; `applyBatch` 2.561 ms at 100 groups, 8.550 ms at 1,000, and 40.093 ms at 5,000. Two of the three softmaxes are the recomputed ones, so 44% of `gradient`'s cost is redundant and `applyBatch` should fall to roughly 25 ms at the 5,000-group ceiling.

**This is not a bottleneck.** 40 ms sits against a ten-second default trigger, so the PPO math is about 0.4% of the interval. A micro-batch's wall clock is dominated by the driver-side `collect` in `GrpoSlates.toGroups`, which that method's own comment documents as a known scaling limitation, plus JSON parsing and the Redis write. This change removes redundant work in the learning rule and should not be cited as relieving batch latency. Timings are JIT-warmed single-JVM measurements on one machine and will vary; the group count per batch depends on click-through, because the zero-variance gate drops most slates at low CTR.

## Delivery

Publish this spec and its implementation plan on `optimize/ppo-training` in a draft PR against `master`, then add the reviewed code, tests, and benchmark evidence to the same PR and mark it ready. This follows the SFT optimization's delivery, where the design was published first and the verified code followed in the same PR.
