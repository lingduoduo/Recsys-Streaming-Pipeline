import sys
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))
import ope_support as logistic


def test_separable_data_learns_perfect_ranking():
    X = np.array([[-3.0], [-2.0], [-1.0], [1.0], [2.0], [3.0]])
    y = np.array([0, 0, 0, 1, 1, 1], dtype=float)
    Xs, mean, std = logistic.standardize(X)
    w = logistic.fit(Xs, y, l2=0.0, lr=0.5, iters=2000)
    p = logistic.predict_proba(Xs, w)
    # positives score above negatives
    assert p[3:].min() > p[:3].max()


def test_predict_proba_bounded():
    X = np.array([[10.0], [-10.0]])
    Xs, mean, std = logistic.standardize(X)
    w = logistic.fit(Xs, np.array([1.0, 0.0]), iters=100)
    p = logistic.predict_proba(Xs, w)
    assert np.all((p >= 0) & (p <= 1))


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
        (np.ones((3, 2), dtype=np.float32), np.zeros(3), 1.0, 0.5, 200),
        (np.ones((3, 2)), np.zeros(3), 1.0, 0.5, 0),
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
    # Measure fit's own growth, not the process-wide peak: start() is a no-op when tracing is
    # already on (PYTHONTRACEMALLOC=1, -X tracemalloc, memory plugins) and the peak then
    # carries everything already alive. Leave an outer session's tracing as we found it.
    was_tracing = tracemalloc.is_tracing()
    tracemalloc.start()
    tracemalloc.reset_peak()
    try:
        baseline, _ = tracemalloc.get_traced_memory()
        logistic.fit(X, y, iters=20)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        if not was_tracing:
            tracemalloc.stop()
    growth = peak - baseline
    budget = n * (features + 1 + 2) * np.dtype(float).itemsize + 65536
    assert growth <= budget, f"fit allocated {growth} bytes; budget is {budget}"
    # The design matrix alone must be visible, or numpy is not routing these allocations
    # through tracemalloc and the budget above would pass while measuring nothing.
    floor = n * (features + 1) * np.dtype(float).itemsize
    assert growth >= floor, f"traced only {growth} bytes; the design matrix alone is {floor}"


def test_predict_proba_uses_the_same_clipped_link_as_fit():
    """Fitting and scoring must share one link function.

    fit() spells the sigmoid inline over its workspace; predict_proba() calls _sigmoid. If the
    two clip differently, fit_reward_model optimizes weights under one link and then reports
    calibration and policy values under another. Fails if _sigmoid's clipping changes alone
    (the reference-parity test above covers the copy inside fit).
    """
    X = np.array([[-1e6], [1e6], [0.5]])
    w = logistic.fit(X, np.array([0.0, 1.0, 0.5]), iters=50)
    D = np.hstack([np.ones((3, 1)), X])
    expected = 1.0 / (1.0 + np.exp(-np.clip(D @ w, -30, 30)))
    np.testing.assert_array_equal(logistic.predict_proba(X, w), expected)
