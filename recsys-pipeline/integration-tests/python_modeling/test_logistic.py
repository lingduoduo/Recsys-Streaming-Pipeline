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
