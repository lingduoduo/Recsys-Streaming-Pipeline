#!/usr/bin/env python3
"""Off-policy evaluation (Direct Method) for bandit policies over the replay buffer.

Fits a dependency-light logistic reward estimator on logged (taken-action features ->
observed reward), then scores target policies re-picked from each event's actionSpace.
Run with plain python (numpy only):

    REDIS_HOST=localhost python services/python-modeling/ope_eval_report.py
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os

import numpy as np
import redis

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


def candidates_of(event: dict) -> list[dict]:
    return event.get("actionSpace") or []


def _ctr(c: dict) -> float:
    imp = float(c.get("impressions", 0) or 0)
    return (float(c.get("clicks", 0) or 0) / imp) if imp > 0 else 0.0


def _rng_index(request_id: str, n: int) -> int:
    h = int(hashlib.md5((str(request_id) + "random").encode()).hexdigest(), 16)
    return h % n


def pick(policy: str, event: dict) -> dict | None:
    cands = candidates_of(event)
    if not cands:
        return None
    if policy == "popularity":
        return max(cands, key=lambda c: float(c.get("impressions", 0) or 0))
    if policy == "ctr":
        return max(cands, key=_ctr)
    if policy == "random":
        return cands[_rng_index(event.get("requestId", ""), len(cands))]
    if policy.startswith("model:"):
        key = policy.split(":", 1)[1]
        return max(cands, key=lambda c: float((c.get("modelPredictions") or {}).get(key, 0.0) or 0.0))
    raise ValueError(f"unknown policy {policy}")


def policy_names(events) -> list[str]:
    return ["logging", "popularity", "ctr", "random"] + [f"model:{k}" for k in _model_pred_keys(events)]


def evaluate(events: list[dict], model: RewardModel) -> list[dict]:
    n = len(events)
    logging_value = sum(float(e.get("reward", 0.0)) for e in events) / n if n else 0.0
    rows = []
    for name in policy_names(events):
        if name == "logging":
            value = logging_value
        else:
            picks = [pick(name, e) for e in events]
            scored = [model.predict_one(c) for c in picks if c is not None]
            value = sum(scored) / len(scored) if scored else 0.0
        lift = (value / logging_value - 1.0) if logging_value else 0.0
        rows.append({
            "policy": name,
            "value": round(value, 4),
            "lift_vs_logging": round(lift, 4),
            "n_events": n,
            "estimator_auc": model.calibration["auc"],
            "estimator_mse": model.calibration["mse"],
        })
    rows.sort(key=lambda r: r["value"], reverse=True)
    return rows


def main(argv=None) -> list[dict]:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--key", default="replay:recommendations")
    ap.add_argument("--parquet", default=None)
    ap.add_argument("--output", default=None)
    ap.add_argument("--limit", type=int, default=-1)
    args = ap.parse_args(argv)
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))

    import replay_buffer
    if args.parquet:
        events = replay_buffer.load_from_parquet(args.parquet)
    else:
        client = redis.Redis(host=host, port=port, decode_responses=False)
        events = replay_buffer.load_from_redis(client, args.key, args.limit)

    events = [e for e in events if e.get("reward") is not None]
    if not events:
        raise SystemExit("no feedback-completed replay events (with reward) — nothing to evaluate")

    model = fit_reward_model(events)
    rows = evaluate(events, model)

    cal = model.calibration
    print(f"reward estimator calibration: auc={cal['auc']} mse={cal['mse']} (n_test={cal['n_test']})")
    for r in rows:
        print(f"  {r['policy']:16s} value={r['value']:.4f}  lift={r['lift_vs_logging']:+.4f}  n={r['n_events']}")
    if args.output:
        with open(args.output, "w", newline="") as fh:
            wr = csv.DictWriter(fh, fieldnames=["policy", "value", "lift_vs_logging",
                                                "n_events", "estimator_auc", "estimator_mse"])
            wr.writeheader()
            wr.writerows(rows)
        print(f"wrote {args.output}")
    return rows


if __name__ == "__main__":
    main()
