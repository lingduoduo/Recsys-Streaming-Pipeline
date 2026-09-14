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

- [ ] **Step 1: Append these tests to `test_logistic.py`.**

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

- [ ] **Step 2: Confirm the allocation regression fails.** From `recsys-pipeline`, run `/Users/linghuang/miniconda3/bin/python3 -m pytest integration-tests/python_modeling/test_logistic.py -q`. Expect numerical parity to pass and the old allocation peak to exceed the budget.
- [ ] **Step 3: Replace `fit` with the following implementation.**

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

- [ ] **Step 4: Run the focused suite again.** Require all tests in `test_logistic.py` to pass.
- [ ] **Step 5: Add this OPE architecture note.** "Reward-model fitting reuses per-fit NumPy workspaces across gradient steps, preserving the existing iteration count, clipping, and regularization while reducing temporary allocation."

### Task 2: Review and deliver

**Interfaces:** Consume the tested diff and spec; produce a reviewed PR against master.

- [ ] **Step 1: Publish the design first.** Commit the spec/plan, push `optimize/rm-training-workspaces`, and open a draft PR titled `Optimize reward-model training allocations` with a prepared `--body-file`.
- [ ] **Step 2: Request read-only review.** Check numerical ordering, ownership, tests, and spec compliance; resolve substantive findings.
- [ ] **Step 3: Run consumers.** From `recsys-pipeline`, run `/Users/linghuang/miniconda3/bin/python3 -m pytest integration-tests/python_modeling -q` and record passed/failed/skipped counts.
- [ ] **Step 4: Benchmark.** Seed 7; normally distributed X with eight features, uniform [0,1) y; 10,000 and 100,000 rows; default parameters; three timed fits per implementation plus separate tracemalloc measurements. Compare against the original allocating reference and record weight error.
- [ ] **Step 5: Publish verified code.** Run `git diff --check`, update evidence, commit code/tests/docs as `perf: reuse reward-model training workspaces`, push, update the PR description, mark ready, and verify remote contents and local status.

## Verification record

Execution pending.
