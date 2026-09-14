# PPO gradient loops design

## Problem and decision

The softmax hoist delivered in PR #235 removed the redundant work from the PPO inner-epoch loop. What remains inside `GrpoMath.gradientFromPolicies` is not redundant computation but the cost of expressing it through Scala's collection combinators. Measured at `dim` 9 on a ten-item slate, JDK 17, median of fifteen runs of 200,000 calls after 300,000 warmup calls: the function costs 1,158 ns/op, and the same arithmetic written with indexed `while` loops over preallocated arrays costs 322 ns/op. The combinators are about 72% of the function.

The cost has four sources. `logits` folds with `row.indices.foldLeft(0.0)`, whose generic `foldLeft` boxes the `Double` accumulator once per feature, and `x.map` allocates an intermediate array. `softmax` allocates three arrays — the scaled logits, the exponentials, and the normalized result — where one buffer suffices. `Array.fill(dim)(0.0)` invokes a closure per element to write zeros that `new Array[Double](dim)` already guarantees. And both accumulation loops build a `Range` per candidate and call a `Function1` per element.

Rewrite the body of `gradientFromPolicies` with indexed `while` loops, fusing the logits and softmax computations into a single row-sized buffer that holds the logits, then the exponentials, then `pi`. This is the same in-place workspace idea the reward-model fit uses, applied to the hot function of the policy arm.

Every loop preserves the original operation order, so results are bitwise identical rather than merely close: a left-to-right `foldLeft` becomes a `while` accumulating in the same order, and the two accumulation loops keep their candidate-major, feature-minor nesting. Verified across 2,000 randomized shapes with zero bit differences.

Alternatives. Rewriting the public `logits` and `softmax` themselves would benefit `loss` and any future caller, but those two functions are directly pinned by `GrpoMathSpec` and also serve the reference-policy path in `applyBatch`; widening the blast radius buys nothing for the hot path, which needs them fused rather than merely faster. Leaving the combinators in place and accepting the cost is defensible on absolute grounds, as the limits section records, but a 3.6x factor on the one function called per group per inner epoch is worth the twenty lines.

## Global constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt under JDK 17; no new dependencies.
- `gradientFromPolicies` must return a bitwise identical vector for every input. Weights out of `applyBatch` must be bitwise identical to today's.
- Preserve `gradient`, `loss`, `logits`, `softmax`, `kl`, `advantages` and `policies` exactly as they are. Only the body of `gradientFromPolicies` changes.
- Preserve the operation order: the dot product accumulates over features ascending, the expected-feature and gradient accumulations stay candidate-major, and the softmax normalizes by a total summed over candidates ascending.
- Preserve `AdvantageFloor` and every floor it guards, the clipped-branch selection rule, and the division by the candidate count.
- Allocate no more than the existing function did: one row-sized buffer, one expected-feature vector, one gradient vector.
- Do not mutate `x`, `piSnap`, `piOld`, `w` or `adv`.

## Implementation

`gradientFromPolicies` allocates `z = new Array[Double](n)` for the row workspace, then in one pass over candidates computes each dot product with an inner `while` over features and divides by the temperature in place. A second pass takes the maximum, a third exponentiates the shifted values while accumulating the total, and a fourth divides through, leaving `pi` in the same buffer. The expected-feature and gradient loops become nested `while` loops over the same indices, reading `x(i)` into a local `row` reference once per candidate. `expected` and `grad` are `new Array[Double](dim)`.

The fused path replicates `softmax`'s max-subtraction with a `while` loop comparing with `>`, where the public `softmax` calls `Array.max`. The two disagree when a logit is NaN: `Array.max` goes through `Ordering[Double]`, while `>` is false against NaN so the loop keeps its running value. A NaN logit means the weights have already diverged, which `stepBatch` detects and discards, and the resulting gradient is NaN-poisoned either way. The behavior is nonetheless pinned by an explicit test rather than left implicit, so a future reader sees it was considered.

## Validation and acceptance

1. `gradientFromPolicies` returns a bitwise identical vector to the pre-change implementation across 2,000 randomized shapes — slate sizes from two to thirteen, random Gaussian features, random weights, both regularized and unregularized KL, and advantages drawn from `advantages`. Compared by raw IEEE bits against a frozen copy of the current body.
2. Bitwise equality also holds for the enumerated edge cases: a two-candidate slate, saturated logits at plus and minus 1e6, zero advantage, a non-unit temperature, `klBeta` of zero, and a slate where every candidate shares one feature vector.
3. A NaN in a feature vector produces a NaN-containing gradient, and `stepBatch` rejects the batch that results. The test states which of the two max semantics the fused loop implements.
4. `applyBatch` weights stay bitwise identical, pinned by the existing two-epoch bit comparison, which already runs at a non-unit temperature with non-uniform references.
5. The existing finite-difference gradient checks, clipping tests, KL tests and two-reference separation tests pass unchanged.
6. The Spark module suite passes under JDK 17 and `git diff --check` is clean.
7. Benchmark evidence records `gradientFromPolicies` ns/op and `applyBatch` milliseconds at 100, 1,000 and 5,000 groups, before and after.

## Measured baseline and limits

Baseline, JDK 17.0.12, Scala 2.12.18, `dim` 9, ten-item slate: `gradientFromPolicies` 1,158 ns/op, `logits` 167 ns/op, `softmax` 393 ns/op. A prototype of the rewritten body measured 322 ns/op, a factor of 3.6, with zero bitwise differences over 2,000 randomized shapes. `applyBatch` after the PR #235 hoist measured 0.702, 5.356 and 28.793 ms at 100, 1,000 and 5,000 groups.

**This is not a bottleneck.** 28.8 ms sits against a ten-second default trigger, so the PPO math is about 0.3% of the interval and this change takes it to roughly 0.1%. A micro-batch's wall clock is dominated by the driver-side `collect` in `GrpoSlates.toGroups`, which that method documents as a known scaling limitation, plus JSON parsing and the Redis write. The justification here is the factor on the hot function, not relief of batch latency, and nothing should cite this design for the latter.

The change also trades readability for speed: hand-rolled indexed loops are harder to read than `pi.indices.foreach`, and the arithmetic they carry is the part of this repository least forgiving of a transcription error. That is why acceptance rests on bitwise comparison against a frozen copy of the current body rather than on review alone. Timings are single-JVM, JIT-warmed measurements on one machine and will vary with hardware and JVM version.

## Delivery

Publish this spec and its implementation plan on `perf/ppo-gradient-loops` in a draft PR against `master`, then add the reviewed code, tests and benchmark evidence to the same PR and mark it ready.
