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
- Preserve the operation order that bitwise equality depends on: the dot product accumulates over features ascending, each feature's expected-value and gradient accumulation runs over candidates ascending, and the softmax normalizes by a total summed over candidates ascending. Candidate-major nesting is how that is written here, but it is an allocation and locality choice rather than a correctness one -- swapping the nesting leaves each accumulator's order unchanged and was measured bit-identical.
- Preserve `AdvantageFloor` and every floor it guards, the clipped-branch selection rule, and the division by the candidate count.
- Allocate no more than the existing function did: one row-sized buffer, one expected-feature vector, one gradient vector.
- Do not mutate `x`, `piSnap`, `piOld`, `w` or `adv`.

## Implementation

`gradientFromPolicies` allocates `z = new Array[Double](n)` for the row workspace, then in one pass over candidates computes each dot product with an inner `while` over features and divides by the temperature in place. A second pass takes the maximum, a third exponentiates the shifted values while accumulating the total, and a fourth divides through, leaving `pi` in the same buffer. The expected-feature and gradient loops become nested `while` loops over the same indices, reading `x(i)` into a local `row` reference once per candidate. `expected` and `grad` are `new Array[Double](dim)`.

The fused path replicates `softmax`'s max-subtraction with a `while` loop comparing with a strict inequality, where the public `softmax` calls `Array.max`. The two pick different maxima when a logit is NaN: `Array.max` goes through `Ordering[Double]`, under which NaN outranks everything, while a strict inequality is false against NaN so the loop keeps its running value. They do not produce different results. A NaN logit exponentiates to NaN, so the total is NaN and every normalized entry is NaN under both paths; the gradient is fully poisoned either way and `stepBatch` discards the batch. The same holds for a `-0.0` against `0.0` maximum, where `v - max` is `0.0` either way and `exp` of both `0.0` and `-0.0` is `1.0`.

The one measurable difference is the NaN payload: with a NaN logit beside an infinite one the loops yield `7ff8000000000000` and the combinators `fff8000000000000`, differing in the sign bit. Raw-bit comparison separates those and nothing else can, so the NaN test compares with NaN treated as equal to NaN and additionally asserts that every element is NaN. Everywhere else the stricter raw-bit equality is used.

Temperature placement deserves its own note, because it is where this fusion invites a mistake. The fused loop divides by the temperature inside the dot-product pass, before the maximum is taken, which is where `softmax`'s separate scaling pass did it. Taking the maximum on raw logits and dividing after the subtraction is the natural slip, and at a power-of-two temperature it is bitwise invisible, because division by a power of two is exact. Verified: that mutation passed the entire suite while every temperature in it was 1.0, 0.5 or 0.25. `GrpoJobConfig` accepts any positive `GRPO_TEMPERATURE`, so the slip is reachable in production, and the randomized test now draws 0.7 as well.

## Validation and acceptance

1. `gradientFromPolicies` returns a bitwise identical vector to the pre-change implementation across 2,000 randomized shapes — slate sizes from two to thirteen, random Gaussian features, random weights, both regularized and unregularized KL, advantages drawn from `advantages`, and a temperature drawn from 1.0, 0.5 and 0.7. The non-power-of-two draw is required, not decorative: with only power-of-two temperatures the comparison cannot see the temperature-placement slip described above. Compared by raw IEEE bits against a frozen copy of the current body.
2. Bitwise equality also holds for the enumerated edge cases: a two-candidate slate, saturated logits at plus and minus 1e6, zero advantage, a non-unit temperature, a non-power-of-two temperature, `klBeta` of zero, a slate where every candidate shares one feature vector, and a slate whose scaled logits are all negative — the last being what a maximum seeded at `0.0` rather than at the first element requires in order to show.
3. With a NaN reaching the logits — through a feature, through a weight, and alongside an infinity — every element of the gradient is NaN, matching the pre-change implementation with NaN treated as equal to NaN. The test records that the two paths choose different maxima and different NaN payloads while producing the same values, and `stepBatch` rejects the batch either way.
4. `applyBatch` weights stay bitwise identical, pinned by the existing two-epoch bit comparison, which already runs at a non-unit temperature with non-uniform references.
5. The existing finite-difference gradient checks, clipping tests, KL tests and two-reference separation tests pass unchanged.
6. The Spark module suite passes under JDK 17 and `git diff --check` is clean.
7. Benchmark evidence records `gradientFromPolicies` ns/op and `applyBatch` milliseconds at 100, 1,000 and 5,000 groups, before and after.

## Measured baseline and limits

Baseline, JDK 17.0.12, Scala 2.12.18, `dim` 9, ten-item slate: `gradientFromPolicies` 1,158 ns/op, `logits` 167 ns/op, `softmax` 393 ns/op. A prototype of the rewritten body measured 322 ns/op, a factor of 3.6, with zero bitwise differences over 2,000 randomized shapes. `applyBatch` after the PR #235 hoist measured 0.702, 5.356 and 28.793 ms at 100, 1,000 and 5,000 groups.

**This is not a bottleneck.** 28.8 ms sits against a ten-second default trigger, so the PPO math is about 0.3% of the interval and this change takes it to roughly 0.1%. A micro-batch's wall clock is dominated by the driver-side `collect` in `GrpoSlates.toGroups`, which that method documents as a known scaling limitation, plus JSON parsing and the Redis write. The justification here is the factor on the hot function, not relief of batch latency, and nothing should cite this design for the latter.

Two input shapes do change behavior, which makes the "bitwise identical for every input" constraint above literally false for them; both are unreachable from the job. An empty slate now throws `ArrayIndexOutOfBoundsException` from seeding the maximum at element 0, where the combinators threw `UnsupportedOperationException("empty.max")` -- `GrpoSlates` drops slates with fewer than two items and `applyBatch` returns early on empty groups, but `gradient` is public. And a feature row LONGER than the weight vector is now silently truncated, where `logits` iterated the row's indices and threw indexing past the end of `w` -- `GrpoSlates.parseFeatureVector` rejects any vector whose length is not `dim`, so only a test or a future caller could construct one. Rows shorter than the weights still throw in both. Neither shape is covered by the randomized test, which fixes `dim` at 9 and builds rows of exactly 9.

The change also trades readability for speed: hand-rolled indexed loops are harder to read than `pi.indices.foreach`, and the arithmetic they carry is the part of this repository least forgiving of a transcription error. That is why acceptance rests on bitwise comparison against a frozen copy of the current body rather than on review alone. Timings are single-JVM, JIT-warmed measurements on one machine and will vary with hardware and JVM version.

## Delivery

Publish this spec and its implementation plan on `perf/ppo-gradient-loops` in a draft PR against `master`, then add the reviewed code, tests and benchmark evidence to the same PR and mark it ready.
