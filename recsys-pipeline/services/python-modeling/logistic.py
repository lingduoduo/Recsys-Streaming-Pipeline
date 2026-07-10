"""Dependency-light logistic regression (numpy only). Supports soft targets in [0,1]."""
from __future__ import annotations

import numpy as np


def standardize(X):
    X = np.asarray(X, dtype=float)
    mean = X.mean(axis=0)
    std = X.std(axis=0)
    std = np.where(std == 0.0, 1.0, std)
    return (X - mean) / std, mean, std


def apply_standardize(X, mean, std):
    return (np.asarray(X, dtype=float) - mean) / std


def _design(X):
    X = np.asarray(X, dtype=float)
    return np.hstack([np.ones((X.shape[0], 1)), X])


def _sigmoid(z):
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))


def fit(X, y, l2=1.0, lr=0.5, iters=500):
    D = _design(X)
    y = np.asarray(y, dtype=float)
    w = np.zeros(D.shape[1])
    n = D.shape[0]
    for _ in range(iters):
        p = _sigmoid(D @ w)
        grad = D.T @ (p - y) / n
        grad[1:] += (l2 / n) * w[1:]  # L2 excludes intercept
        w -= lr * grad
    return w


def predict_proba(X, w):
    return _sigmoid(_design(X) @ w)
