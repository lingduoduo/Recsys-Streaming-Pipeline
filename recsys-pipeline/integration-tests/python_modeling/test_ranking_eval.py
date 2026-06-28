import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import ranking_eval_report as r  # noqa: E402


def test_auc_matches_known_example():
    # classic example: AUC = 0.75
    assert r.auc([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]) == 0.75


def test_auc_perfect_reversed_and_ties():
    assert r.auc([0.1, 0.2, 0.3, 0.4], [0, 0, 1, 1]) == 1.0     # perfect
    assert r.auc([0.4, 0.3, 0.2, 0.1], [0, 0, 1, 1]) == 0.0     # reversed
    assert r.auc([0.5, 0.5, 0.5, 0.5], [0, 0, 1, 1]) == 0.5     # all ties
    assert r.auc([0.1, 0.2], [1, 1]) is None                    # one class → undefined


def test_logloss_known_value():
    assert r.logloss([0.9, 0.1], [1, 0]) == pytest.approx(-__import__("math").log(0.9), abs=1e-6)


def test_zsigmoid_bounds_and_constant():
    ps = r.zsigmoid([1.0, 2.0, 3.0])
    assert all(0.0 < p < 1.0 for p in ps)
    assert r.zsigmoid([5.0, 5.0]) == [0.5, 0.5]   # zero variance → 0.5


def test_evaluate_signal():
    m = r.evaluate_signal([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
    assert m["auc"] == 0.75
    assert m["n"] == 4 and m["positives"] == 2
    assert m["logloss"] is not None
