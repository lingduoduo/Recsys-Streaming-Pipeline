# GRPO PPO objective simplification

**Date:** 2026-09-12
**Status:** Approved design; implementation in progress on `simplify/grpo-ppo-objective`

## Problem and scope

The repository has no standalone PPO. The PPO-clipped surrogate is the objective of online GRPO,
implemented as pure array functions in `GrpoMath` (`loss`, `gradient`) and applied by the
inner-epoch loop in `GrpoPolicyStreamingJob.applyBatch`, as designed in
[the online GRPO design](2026-08-30-online-grpo-design.md). The math is correct and pinned by
finite-difference and hand-computed tests. It carries avoidable complexity:

- The KL term's per-candidate derivative is written as `log(pi_i / piOld_i) + 1`. The `+1` is
  multiplied by `pi_i (x_i - E_pi[x])` and summed over candidates, and that sum is identically zero,
  so the term contributes nothing. On 2,000 random groups the gradient with and without it differs
  by at most 7e-12.
- The clip gate is expressed with two booleans (`clippedActive`, `unclippedSelected`) and a
  comment explaining when `min` selects each branch. The unclipped branch is selected exactly when
  `ratio * adv <= clipped * adv`; the same random check confirms the equivalence.
- `loss` and `gradient` each build the three policy distributions and the ratio independently.
- `GrpoMath.logits` is private, so `applyBatch` and `GrpoPolicyStreamingJobSpec` re-implement the
  dot product with `foldLeft`.
- `applyBatch` recomputes `GrpoMath.advantages` for every group on every inner epoch although the
  advantage does not depend on the weights and the gate already computed it once.

This change simplifies that code without changing what it computes. It does not change
hyperparameters, configuration parsing, the Redis weight layout, the feature layout, the serving
scorer, or any documentation of operator-facing behavior.

## Global constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, JDK 17 for sbt; add no dependencies.
- Keep `GrpoHyperParams`, `GrpoJobConfig`, `GrpoGroup`, `GrpoWeights`, and every public signature
  in `GrpoMath` and `GrpoPolicyStreamingJob` unchanged. `logits` may become public.
- Keep `loss` numerically identical. Keep `gradient` equal to its previous value up to
  floating-point reassociation; the existing finite-difference tolerance of `1e-10` against `loss`
  remains the acceptance bound.
- Keep `applyBatch` semantics: snapshot frozen for the batch, ratio against the snapshot, KL
  against the logged policy, the update divided by `groups.size`, groups whose advantage is `None`
  skipped, and the returned `GrpoWeights` fields unchanged.
- Keep `AdvantageFloor` and its uses as the probability floor in the ratio and KL.

## Alternatives and decision

| Approach | Tradeoff |
|---|---|
| Leave the code; the tests prove it correct | No risk, but the dead term and two-boolean gate keep misleading the next reader into thinking they matter. |
| Simplify in place, guarded by the existing tests plus one for the uncovered branch | Selected: smallest diff with the strongest existing oracle (finite differences against an independently pinned loss). |
| Store the advantage on `GrpoGroup` at gate time | Removes the per-epoch recomputation too, but changes a constructor used by four call sites and every test fixture for the same effect as preparing it once inside `applyBatch`. |
| Rename `AdvantageFloor` because it also floors probabilities | Cosmetic; touches comments in three files for no behavior gain. Rejected. |

## Design

### GrpoMath

- `logits(x, w)` becomes public. Same body.
- A private helper `policies(x, snapshotLogits, loggedLogits, w, cfg)` returns
  `(pi, piSnap, piOld)` so `loss` and `gradient` share the setup.
- `gradient` computes, per candidate, `ratio`, `clipped`, and a single boolean
  `unclippedSelected = ratio * adv(i) <= clipped * adv(i)`. The surrogate scale is
  `-adv(i) / (max(piSnap(i), AdvantageFloor) * n)` when selected, else `0.0`. The KL scale is
  `klBeta * log(max(pi(i), floor) / max(piOld(i), floor))`, with no `+1`. The rest of the
  accumulation (`scale * pi(i) / temperature * (x(i)(d) - expected(d))`) is unchanged.
- The derivation comment states the two facts a reader needs: the softmax Jacobian gives
  `d pi_i / dw = (pi_i / temperature) (x_i - E_pi[x])`, and any term constant across candidates
  in `dL/dpi_i` sums to zero against it, which is why the KL derivative carries no `+1`.

### applyBatch

Before the epoch loop, build `prepared: Seq[(GrpoGroup, Array[Double], Array[Double])]` of
`(group, snapshotLogits, advantage)` by mapping each group through `GrpoMath.logits(g.x, snapshot)`
and `GrpoMath.advantages(g.rewards)`, dropping groups whose advantage is `None`. The epoch loop
then accumulates `GrpoMath.gradient(g.x, snapshotLogits, g.logged, w, adv, cfg.hyper)` over
`prepared` and applies `w(d) -= lr * total(d) / groups.size`. The early return on empty input,
the clone of the input weights, and the returned fields are unchanged.

### Tests

All existing tests in `GrpoMathSpec`, `GrpoPolicyStreamingJobSpec`, `GrpoSlatesSpec`,
`GrpoJobConfigSpec`, and `GrpoWeightStoreSpec` stay as written, except that the job spec's
hand-rolled epoch replay calls `GrpoMath.logits` instead of its own `foldLeft`.

One new finite-difference test in `GrpoMathSpec` targets the branch no current fixture reaches:
clip active with the unclipped branch selected. With one-hot features and `w = (-2, 0)`,
`snapshot = (0, 0)`, `adv = (1, -1)`, candidate 0 has positive advantage with ratio below
`1 - eps` and candidate 1 has negative advantage with ratio above `1 + eps`, so `min` selects the
raw ratio for both and the gradient must flow. The test asserts the fixture reaches those
conditions and that the analytic gradient matches the numeric one within `1e-10`.

## Files

Paths below are relative to the repository root.

| File | Responsibility |
|---|---|
| `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala` | Public `logits`, shared policy setup, single-comparison clip gate, KL derivative without the dead term. |
| `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoPolicyStreamingJob.scala` | Per-group preparation once per batch; epoch loop over prepared groups. |
| `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala` | New finite-difference test on the clip-active, unclipped-selected branch. |
| `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoPolicyStreamingJobSpec.scala` | Epoch replay uses `GrpoMath.logits`. |
| `.superpowers/docs/plans/2026-09-12-grpo-ppo-objective-simplification.md` | Reproducible implementation and verification steps. |

## Acceptance criteria

1. The new finite-difference test asserts `ratio0 < 1 - eps` with `adv0 > 0`, `ratio1 > 1 + eps`
   with `adv1 < 0`, and analytic-versus-numeric agreement within `1e-10`. It passes before and
   after the change, which shows the branch was already correct and stays so.
2. Every pre-existing GRPO test passes unchanged: 50 tests across the five suites, plus the new one.
3. `GrpoMath.gradient` no longer contains `clippedActive` or the `+ 1.0` KL term, and
   `applyBatch` calls `GrpoMath.advantages` once per group per batch.
4. The job spec's replay and `applyBatch` both use `GrpoMath.logits`; no `foldLeft` dot product
   remains in the package outside `GrpoMath`.
5. The full Spark module suite passes under JDK 17 before the PR, with unrelated failures reported
   against a baseline rather than claimed green.

## Risks and limits

Reassociating the gradient arithmetic can move results at the `1e-12` level; the finite-difference
tolerance and the job spec's `1e-9` weight comparisons absorb that. The change does not alter the
known driver-side collection in `GrpoSlates.toGroups`, the `GRPO_INNER_EPOCHS` floor, or the
served-position exclusion from the feature vector. Rollback is a revert; no stored weights change.
