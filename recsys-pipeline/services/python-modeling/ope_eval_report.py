#!/usr/bin/env python3
"""Off-policy evaluation (Direct Method) for bandit policies over the replay buffer.

Fits a dependency-light logistic reward estimator on logged (taken-action features ->
observed reward), then scores target policies re-picked from each event's actionSpace.
Run with plain python (numpy only):

    REDIS_HOST=localhost python services/python-modeling/ope_eval_report.py
"""
from __future__ import annotations

import argparse
import hashlib
import os

import numpy as np

import logistic
from ranking_eval_report import auc

BASE_FEATURES = ["coldStart", "actionPosition", "impressions", "clicks"]


def _model_pred_keys(events) -> list[str]:
    keys = set()
    for e in events:
        mp = e.get("modelPredictions") or {}
        keys.update(k for k, v in mp.items() if isinstance(v, (int, float)))
        for c in e.get("actionSpace", []):
            cmp = c.get("modelPredictions") or {}
            keys.update(k for k, v in cmp.items() if isinstance(v, (int, float)))
    return sorted(keys)


def feature_names(events) -> list[str]:
    return BASE_FEATURES + _model_pred_keys(events)


def _vec(cand_like: dict, names: list[str]) -> list[float]:
    mp = cand_like.get("modelPredictions") or {}
    out = []
    for n in names:
        if n == "coldStart":
            out.append(1.0 if cand_like.get("coldStart") else 0.0)
        elif n in ("actionPosition", "impressions", "clicks"):
            out.append(float(cand_like.get(n, 0) or 0))
        else:
            out.append(float(mp.get(n, 0.0) or 0.0))
    return out


def taken_features(event: dict, names: list[str]) -> list[float]:
    return _vec(event, names)


def is_test(request_id: str) -> bool:
    return int(hashlib.md5(str(request_id).encode()).hexdigest(), 16) % 5 == 0


class RewardModel:
    def __init__(self, names, mean, std, w, calibration):
        self.names = names
        self._mean = mean
        self._std = std
        self._w = w
        self.calibration = calibration

    def predict_one(self, cand_like: dict) -> float:
        X = np.array([_vec(cand_like, self.names)], dtype=float)
        Xs = logistic.apply_standardize(X, self._mean, self._std)
        return float(logistic.predict_proba(Xs, self._w)[0])


def fit_reward_model(events: list[dict]) -> RewardModel:
    names = feature_names(events)
    train = [e for e in events if not is_test(e.get("requestId", ""))]
    test = [e for e in events if is_test(e.get("requestId", ""))]
    if not train:
        train, test = events, events
    Xtr = np.array([taken_features(e, names) for e in train], dtype=float)
    ytr = np.array([float(e.get("reward", 0.0)) for e in train], dtype=float)
    Xs, mean, std = logistic.standardize(Xtr)
    w = logistic.fit(Xs, ytr, l2=1.0, lr=0.5, iters=500)

    Xte = np.array([taken_features(e, names) for e in test], dtype=float)
    pte = logistic.predict_proba(logistic.apply_standardize(Xte, mean, std), w)
    rte = np.array([float(e.get("reward", 0.0)) for e in test], dtype=float)
    clicked = [int(e.get("clicked", e.get("reward", 0.0) > 0)) for e in test]
    calibration = {
        "n_test": len(test),
        "mse": round(float(np.mean((pte - rte) ** 2)), 4) if len(test) else None,
        "auc": auc(list(pte), clicked) if len(test) else None,
    }
    return RewardModel(names, mean, std, w, calibration)
