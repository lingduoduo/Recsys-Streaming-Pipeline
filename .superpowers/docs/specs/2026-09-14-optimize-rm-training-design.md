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

## Evidence and limits

Measured on the delivered implementation, Python 3.12.2 / NumPy 2.5.1 / Accelerate BLAS, seed 7, eight features, 500 steps: traced peaks fall from 961,504 to 801,448 bytes at 10,000 rows and from 9,601,504 to 8,001,448 bytes at 100,000 rows, a reduction of 16.6% and 16.7%. Weights are bitwise identical to the allocating optimizer, not merely within the 1e-12 this design requires.

**This is an allocation reduction, not a speedup.** Runtime is effectively unchanged: over nine fits, 0.0977 s to 0.0958 s at 10,000 rows (2.0%) and 0.9711 s to 0.9649 s at 100,000 rows (0.6%, inside the 1.3% noise band). The cost is dominated by the BLAS matrix-vector products, not by allocation.

The prototype figures this design was written against reported 0.119 s to 0.095 s at 10,000 rows, a 20% gain, and traced peaks of 1,041,288 and 10,401,264 bytes before. That runtime gain did not reproduce. The delivered "after" peaks match the prototype exactly while the "before" peaks are lower, consistent with the NumPy version difference (2.5.1 versus 2.4.4). Anything citing this design should claim reduced temporary allocation only, which is what the architecture note in `Analysis_Report.md` says.

This is synthetic fitting time, not end-to-end RM throughput. Tracemalloc is not process RSS and does not cover every BLAS allocation. Runtime depends on hardware and NumPy/BLAS. The design matrix remains O(rows × features).

## Delivery

Publish spec/plan in a draft PR against master, then add reviewed code, tests, and architecture docs. Use an isolated checkout so the unrelated local documentation-move commit is preserved and excluded from this PR.
