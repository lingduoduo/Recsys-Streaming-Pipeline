"""Support libraries for off-policy evaluation (OPE) and replay export.

Consolidates two small dependency-light helpers:

  • logistic regression (numpy only, supports soft targets in [0,1]) — the DM reward model
  • replay-buffer loader — reads the RL replay buffer (Redis list or Parquet dump), owning the
    Redis key and JSON decoding so replay_export.py and ope_eval_report.py agree on the schema.
"""
from __future__ import annotations

import json

import numpy as np


# ── logistic regression (numpy) ───────────────────────────────────────────────────
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


# ── replay buffer loader (Redis list or Parquet) ──────────────────────────────────
DEFAULT_KEY = "replay:recommendations"


def _parse(item) -> dict:
    if isinstance(item, bytes):
        item = item.decode("utf-8")
    return json.loads(item)


def load_from_redis(client, key: str = DEFAULT_KEY, limit: int = -1) -> list[dict]:
    return [_parse(x) for x in client.lrange(key, 0, limit)]


def load_from_parquet(path) -> list[dict]:
    import pandas as pd
    return pd.read_parquet(path).to_dict(orient="records")
