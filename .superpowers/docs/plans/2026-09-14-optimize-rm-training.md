# Optimize RM Training Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce RM fitting allocations without changing optimizer behavior.

**Architecture:** Allocate a local row workspace and gradient workspace once per fit and reuse them in the existing gradient loop.

**Tech Stack:** Python 3, NumPy, pytest.

**Spec:** [RM training design](../specs/2026-09-14-optimize-rm-training-design.md)

## Global Constraints

- Python 3 and NumPy only; add no project dependencies.
- Preserve `fit(X, y, l2=1.0, lr=0.5, iters=500)` and its weight-vector return shape.
- Preserve float64 conversion, zero initialization, clipping to [-30, 30], soft targets, and L2 exclusion of the intercept.
- Preserve the 500 default iterations and arithmetic order within rtol/atol 1e-12.
- Do not mutate caller-owned X or y or retain workspaces across calls.
- Keep RM splitting, feature schema, policy-score exclusions, calibration, prediction, CLI, and report formats unchanged.

## Files and execution

- `recsys-pipeline/services/python-modeling/ope_support.py`: fitting loop.
- `recsys-pipeline/integration-tests/python_modeling/test_logistic.py`: numerical and allocation regressions.
- `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md`: workspace note.
- Worktree: `/tmp/recsys-rm-training-worktree`, branch `optimize/rm-training-workspaces`, base `origin/master` at `8185168`.
- Interpreter: `/Users/linghuang/miniconda3/bin/python3` with existing modeling dependencies.

### Task 1: Reuse fitting workspaces

**Interfaces:** Consume numeric X (rows × features), one-dimensional targets y, and existing optimizer parameters; return fresh float64 weights with intercept first.

- [x] **Step 1: Append these tests to `test_logistic.py`.**

```python
def _reference_fit(X, y, l2=1.0, lr=0.5, iters=500):
    """Original allocating optimizer; pins the update and clipping semantics."""
    D = np.hstack([np.ones((X.shape[0], 1)), np.asarray(X, dtype=float)])
    y = np.asarray(y, dtype=float)
    w = np.zeros(D.shape[1])
    for _ in range(iters):
        p = 1.0 / (1.0 + np.exp(-np.clip(D @ w, -30, 30)))
        grad = D.T @ (p - y) / len(D)
        grad[1:] += (l2 / len(D)) * w[1:]
        w -= lr * grad
    return w


def test_fit_preserves_reference_updates_and_inputs():
    rng = np.random.default_rng(17)
    cases = [
        (rng.normal(size=(37, 4)), rng.random(37), 1.0, 0.5, 500),
        (rng.normal(size=(19, 3)), rng.integers(0, 2, 19), 0.0, 0.2, 100),
        (np.ones((9, 2)), np.ones(9), 2.0, 0.5, 40),
        (np.array([[-1e6], [1e6]]), np.array([0., 1.]), 1.0, 0.5, 10),
        (np.zeros((1, 0)), np.array([0.3]), 5.0, 0.1, 20),
        (np.ones((3, 2), dtype=np.float32), np.zeros(3), 1.0, 0.5, 0),
    ]
    for X, y, l2, lr, iters in cases:
        expected = _reference_fit(X, y, l2, lr, iters)
        original_X, original_y = X.copy(), y.copy()
        X.setflags(write=False)
        y.setflags(write=False)
        actual = logistic.fit(X, y, l2, lr, iters)
        np.testing.assert_allclose(actual, expected, rtol=1e-12, atol=1e-12)
        np.testing.assert_array_equal(X, original_X)
        np.testing.assert_array_equal(y, original_y)


def test_fit_bounds_temporary_allocation():
    import tracemalloc

    # Fit owns the intercept-augmented design plus one row-sized workspace.
    # Allow a second row-sized allocation and 64 KiB of allocator overhead;
    # retaining/recreating the original sigmoid temporaries exceeds this budget.
    n, features = 20000, 3
    X = np.zeros((n, features))
    y = np.full(n, 0.3)
    logistic.fit(X[:2], y[:2], iters=2)  # warm up numpy outside measurement
    tracemalloc.start()
    try:
        logistic.fit(X, y, iters=20)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    budget = n * (features + 1 + 2) * np.dtype(float).itemsize + 65536
    assert peak <= budget, f"fit allocated {peak} bytes; budget is {budget}"
```

- [x] **Step 2: Confirm the allocation regression fails.** From `recsys-pipeline`, run `/Users/linghuang/miniconda3/bin/python3 -m pytest integration-tests/python_modeling/test_logistic.py -q`. Expect numerical parity to pass and the old allocation peak to exceed the budget.

Observed: 1 failed, 3 passed. Parity passed against the allocating reference; the allocation assertion failed with `fit allocated 1281512 bytes; budget is 1025536`.
- [x] **Step 3: Replace `fit` with the following implementation.**

```python
def fit(X, y, l2=1.0, lr=0.5, iters=500):
    D = _design(X)
    y = np.asarray(y, dtype=float)
    w = np.zeros(D.shape[1])
    n = D.shape[0]
    # Reuse row-sized workspace across iterations: logits become probabilities,
    # then residuals. D, X, and y remain unchanged.
    residual = np.empty(n, dtype=float)
    grad = np.empty_like(w)
    for _ in range(iters):
        np.matmul(D, w, out=residual)
        np.clip(residual, -30, 30, out=residual)
        np.negative(residual, out=residual)
        np.exp(residual, out=residual)
        residual += 1.0
        np.reciprocal(residual, out=residual)
        residual -= y
        np.matmul(D.T, residual, out=grad)
        grad /= n
        grad[1:] += (l2 / n) * w[1:]  # L2 excludes intercept
        w -= lr * grad
    return w
```

- [x] **Step 4: Run the focused suite again.** Require all tests in `test_logistic.py` to pass.

Observed: 4 passed.
- [x] **Step 5: Add this OPE architecture note.** "Reward-model fitting reuses per-fit NumPy workspaces across gradient steps, preserving the existing iteration count, clipping, and regularization while reducing temporary allocation."

### Task 2: Review and deliver

**Interfaces:** Consume the tested diff and spec; produce a reviewed PR against master.

- [x] **Step 1: Publish the design first.** Commit the spec/plan, push `optimize/rm-training-workspaces`, and open a draft PR titled `Optimize reward-model training allocations` with a prepared `--body-file`.

Observed: done as PR #231, which merged to master carrying the spec and plan only (no code). The code therefore lands in a follow-up PR rather than the same one; per the user's instruction it is added to PR #233, which already carries the unrelated documentation move that this spec's Delivery section expected to keep separate.
- [x] **Step 2: Request read-only review.** Check numerical ordering, ownership, tests, and spec compliance; resolve substantive findings.

Observed: an independent read-only review found no issue in the optimizer itself — it verified bitwise parity over ~40 randomized shapes plus the 100,000-row benchmark, confirmed `np.reciprocal` matches `1.0 / x` for float64 over the post-`exp` domain, confirmed `residual`/`grad` cannot alias `D`, `w`, `y`, or the caller's `X`, and caught 7 of 7 seeded mutations of the loop with the parity test. It raised two substantive test defects, both reproduced and fixed:

1. `tracemalloc.start()` is a no-op when tracing is already active and does not clear the peak, so under `PYTHONTRACEMALLOC=1` the budget assertion saw the process-wide peak and failed with `44428177 bytes`; the `finally` also tore down an outer session's tracing. The test now measures fit's own growth against a baseline, restores tracing only if it enabled it, and adds a floor assertion so it cannot pass while tracemalloc is measuring nothing. Verified: passes plain, under `PYTHONTRACEMALLOC=1`, and under `-X tracemalloc`, and still rejects the old allocating fit in both modes (1,283,312 and 1,283,376 bytes against the 1,025,536 budget).
2. Nothing tied the sigmoid inlined in `fit` to the `_sigmoid` that `predict_proba` uses. Widening `_sigmoid` to clip at +/-50 left all 507 tests green while `fit_reward_model` would fit weights under one link and report calibration under another. Added `test_predict_proba_uses_the_same_clipped_link_as_fit`, which fails on that mutation; the copy inside `fit` remains pinned by the reference-parity test. The review's alternative -- having `_reference_fit` call `logistic._sigmoid` -- was rejected: the reference exists to be an oracle independent of the code under test.

One review suggestion was adopted with a corrected rationale: the float32 case ran `iters=0` and so exercised no arithmetic, and now runs `iters=200` (still bitwise identical) with a separate case retaining `iters=0` coverage. The stated reason for it does not hold, though -- dropping `dtype=float` from `_design` is still not detectable, because `np.ones` is float64 and `np.hstack` promotes the design matrix to float64 regardless. A second suggestion, routing `_reference_fit` through `logistic._design`, was rejected for the same oracle-independence reason as above.
- [x] **Step 3: Run consumers.** From `recsys-pipeline`, run `/Users/linghuang/miniconda3/bin/python3 -m pytest integration-tests/python_modeling -q` and record passed/failed/skipped counts.

Observed: 507 passed, 0 failed, 0 skipped (8 pre-existing warnings from `next_item_model`).
- [x] **Step 4: Benchmark.** Seed 7; normally distributed X with eight features, uniform [0,1) y; 10,000 and 100,000 rows; default parameters; three timed fits per implementation plus separate tracemalloc measurements. Compare against the original allocating reference and record weight error.

Observed (Python 3.12.2, NumPy 2.5.1, Accelerate BLAS; median of three fits; tracemalloc measured separately):

| rows | impl | median runtime | traced peak | weights vs reference |
|---|---|---|---|---|
| 10,000 | reference | 0.099 s | 961,504 B | -- |
| 10,000 | workspace | 0.096 s | 801,448 B | bitwise identical |
| 100,000 | reference | 0.993 s | 9,601,504 B | -- |
| 100,000 | workspace | 0.983 s | 8,001,448 B | bitwise identical |

Weight error is exactly 0.0, not merely within the 1e-12 tolerance the spec requires, across all eight acceptance scenarios (soft and binary targets, regularized and unregularized, constant features, saturated logits, intercept-only, zero iterations).

The runtime claim in the spec's Preliminary evidence does not reproduce here. The spec reports 0.119 s -> 0.095 s at 10,000 rows, a 20% gain; measured over nine fits the gain is 2.0% at 10,000 rows (0.0977 -> 0.0958, reference stdev 0.5% of median) and 0.6% at 100,000 rows (0.9711 -> 0.9649), the latter inside the 1.3% noise band. The cost is dominated by the BLAS matrix-vector products, not by allocation. Traced allocations reduce by 16.6% and 16.7%; the "after" peaks match the spec's prototype exactly while the "before" peaks are lower than its 1,041,288 / 10,401,264, consistent with the NumPy version difference (2.5.1 here versus 2.4.4 in the prototype). This change should be described as an allocation reduction, not a speedup; `Analysis_Report.md` claims only reduced temporary allocation, which is accurate.
- [x] **Step 5: Publish verified code.** Run `git diff --check`, update evidence, commit code/tests/docs as `perf: reuse reward-model training workspaces`, push, update the PR description, mark ready, and verify remote contents and local status.

Observed: `git diff --check` clean. Delivered on `docs/relocate-superpowers-docs` as PR #233 (see Task 2 Step 1), which was already open and rebased onto master at `db24ec6` first.

## Verification record

- Focused baseline (allocating `fit`): 1 failed, 3 passed -- `fit allocated 1281512 bytes; budget is 1025536`.
- Focused final: 5 passed, including the added link-function pin. Green plain, under `PYTHONTRACEMALLOC=1`, and under `-X tracemalloc`.
- Consumers: 508 passed, 0 failed, 0 skipped (8 pre-existing `next_item_model` FutureWarnings).
- Weight parity: bitwise identical to the allocating optimizer in all eight acceptance scenarios and at both benchmark sizes.
- Input ownership: read-only `X` and `y` accepted and unmodified; repeat calls reproduce weights; the returned vector is fresh and writable, and mutating it does not affect later fits.
- `n = 0` raises `ZeroDivisionError` from `l2 / n` in both the old and new implementations. Pre-existing and unreachable through `ope_eval_report.main`, which exits on an empty replay; left unchanged.
- Independent read-only review: two substantive test defects, both fixed and re-verified (Task 2 Step 2). No finding against the optimizer.
- Allocation reduction 16.6% (10,000 rows) and 16.7% (100,000 rows); runtime effectively flat. See Task 2 Step 4.
