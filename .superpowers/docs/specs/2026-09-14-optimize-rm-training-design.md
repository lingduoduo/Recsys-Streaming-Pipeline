# Optimize reward-model training design

## Problem and decision

The offline RM stage is `ope_eval_report.fit_reward_model`, backed by `ope_support.fit`. Each of its 500 default gradient steps allocates row-sized arrays for logits, sigmoid intermediates, probabilities, and residuals. Policy scoring and bootstrap reuse were optimized separately.

Reuse one row-sized buffer for logits, clipped probabilities, and residuals and one feature-sized gradient buffer. NumPy `out=` operations preserve the calculation order. Keep the intercept-augmented design and existing weight update.

Fewer iterations or early stopping alter weights; a new solver adds numerical or dependency risk. Buffer reuse addresses allocations with the existing optimizer.

## Global constraints

- Python 3 and NumPy only; add no project dependencies.
- Preserve `fit(X, y, l2=1.0, lr=0.5, iters=500)` and its weight-vector return shape.
- Preserve float64 conversion, zero initialization, clipping to [-30, 30], soft targets, and L2 exclusion of the intercept.
- Preserve the 500 default iterations and arithmetic order within rtol/atol 1e-12.
- Do not mutate caller-owned X or y or retain workspaces across calls.
- Keep RM splitting, feature schema, policy-score exclusions, calibration, prediction, CLI, and report formats unchanged.

## Implementation

Allocate `residual = np.empty(n, dtype=float)` and `grad = np.empty_like(w)` before the loop. Each step overwrites residual with `D @ w`, clips, negates, exponentiates, adds one, reciprocates, and subtracts y in place. Write `D.T @ residual` into grad, divide by n, and apply existing regularization and weight update. `_sigmoid` remains the inference helper; parity tests pin matching clipping semantics.

## Acceptance

1. Weights match the original allocating optimizer within 1e-12 for soft/binary targets, regularized/unregularized fits, constant features, saturated logits, intercept-only inputs, and zero iterations.
2. Read-only inputs remain unchanged.
3. A 20,000-row, three-feature fit stays below a traced allocation budget of the design matrix plus two row-sized arrays plus 64 KiB. The old fit must fail and the new fit pass.
4. The Python modeling suite passes, including OPE, Q/DPO, and dashboard consumers.
5. Architecture documentation explains workspace reuse; benchmark evidence reports runtime, traced allocations, and weight differences.

## Preliminary evidence and limits

Prototype, Python 3.12 / NumPy 2.4.4, seed 7, eight features, 500 steps, median of three fits: 10,000 rows took 0.119 s before and 0.095 s after, with traced peaks 1,041,288 and 801,448 bytes. At 100,000 rows: 1.026 and 1.008 s, with peaks 10,401,264 and 8,001,448 bytes. Weights were identical.

This is synthetic fitting time, not end-to-end RM throughput. Tracemalloc is not process RSS and does not cover every BLAS allocation. Runtime depends on hardware and NumPy/BLAS. The design matrix remains O(rows × features).

## Delivery

Publish spec/plan in a draft PR against master, then add reviewed code, tests, and architecture docs. Use an isolated checkout so the unrelated local documentation-move commit is preserved and excluded from this PR.
