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

import ope_support as logistic
from ranking_eval_report import auc

# Features are sourced from actionSpace candidate dicts (item, coldStart, impressions,
# clicks, modelPredictions) so training (the taken action) and scoring (policy picks)
# read the identical feature schema. actionPosition/top-level-only fields are excluded
# because they are not present per-candidate and would be dead weight at scoring time.
BASE_FEATURES = ["coldStart", "impressions", "clicks"]

# modelPredictions keys that are POLICY SCORES rather than serve-time observations: the offline
# post-training trainer (post-training/post_train_q.py) writes its fitted Q back into every
# candidate as `tabQ`/`fqiQ` so that `model:tabQ` and `model:fqiQ` become evaluable policies.
# A score produced by a policy must never become an input feature of the reward model that
# judges it. Were they features, the estimator would be fit on the very values it is then asked
# to grade -- it can recover the reward from the policy's own output and report lift for a
# policy that learned nothing (measured: +17.4% on a pure-noise fixture) -- and, because the
# feature schema is shared, merely scoring a replay would shift every OTHER policy's number too.
# They are excluded from feature_names() and retained in policy_names().
#
# The trainer imports these names rather than repeating the strings, so a rename cannot leave the
# two sides disagreeing -- which would silently reintroduce the circularity with a green suite.
# A new arm (DPO next) adds its key here in the same commit that starts writing it.
TABULAR_Q_PRED_KEY = "tabQ"
FQI_Q_PRED_KEY = "fqiQ"
DPO_PRED_KEY = "dpoScore"
#: The online GRPO policy's score, written by GrpoPolicyScorer in the serving path.
GRPO_PRED_KEY = "grpoScore"

POLICY_ONLY_PRED_KEYS = (TABULAR_Q_PRED_KEY, FQI_Q_PRED_KEY, DPO_PRED_KEY, GRPO_PRED_KEY)


def candidates_of(event: dict) -> list[dict]:
    # `or []` would raise on a Parquet-loaded event: pandas returns nested lists as ndarrays,
    # whose truth value is ambiguous for 2+ elements. Test explicitly for None instead.
    raw = event.get("actionSpace")
    return [] if raw is None else list(raw)


def _model_pred_keys(events) -> list[str]:
    keys = set()
    for e in events:
        mp = e.get("modelPredictions") or {}
        keys.update(k for k, v in mp.items() if isinstance(v, (int, float)))
        for c in candidates_of(e):
            cmp = c.get("modelPredictions") or {}
            keys.update(k for k, v in cmp.items() if isinstance(v, (int, float)))
    return sorted(keys)


def feature_names(events) -> list[str]:
    return BASE_FEATURES + [k for k in _model_pred_keys(events)
                            if k not in POLICY_ONLY_PRED_KEYS]


def _vec(cand_like: dict, names: list[str]) -> list[float]:
    mp = cand_like.get("modelPredictions") or {}
    out = []
    for n in names:
        if n == "coldStart":
            out.append(1.0 if cand_like.get("coldStart") else 0.0)
        elif n in ("impressions", "clicks"):
            out.append(float(cand_like.get(n, 0) or 0))
        else:
            out.append(float(mp.get(n, 0.0) or 0.0))
    return out


def _taken_candidate(event: dict) -> dict:
    """The taken action's own actionSpace entry (which carries impressions/clicks);
    falls back to the event itself if the action is absent from the snapshot."""
    action = event.get("action")
    for c in candidates_of(event):
        if c.get("item") == action:
            return c
    return event


def taken_features(event: dict, names: list[str]) -> list[float]:
    return _vec(_taken_candidate(event), names)


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

    if test:
        Xte = np.array([taken_features(e, names) for e in test], dtype=float)
        pte = logistic.predict_proba(logistic.apply_standardize(Xte, mean, std), w)
        rte = np.array([float(e.get("reward", 0.0)) for e in test], dtype=float)
        clicked = [int(e.get("clicked", e.get("reward", 0.0) > 0)) for e in test]
        mse = round(float(np.mean((pte - rte) ** 2)), 4)
        auc_value = auc(list(pte), clicked)
    else:
        mse = None
        auc_value = None
    calibration = {"n_test": len(test), "mse": mse, "auc": auc_value}
    return RewardModel(names, mean, std, w, calibration)


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


def _evaluate_statistics(events: list[dict], model: RewardModel,
                         policies: list[str]) -> list[dict]:
    n = len(events)
    logging_value = sum(float(e.get("reward", 0.0)) for e in events) / n if n else 0.0
    rows = []
    for name in policies:
        if name == "logging":
            value = logging_value
            count = n
        else:
            picks = [pick(name, e) for e in events]
            scored = [model.predict_one(c) for c in picks if c is not None]
            count = len(scored)
            value = sum(scored) / count if scored else 0.0
        lift = (value / logging_value - 1.0) if logging_value > 0.0 else None
        rows.append({
            "policy": name,
            "value": value,
            "lift_vs_logging": lift,
            "n_events": count,
            "estimator_auc": model.calibration["auc"],
            "estimator_mse": model.calibration["mse"],
        })
    rows.sort(key=lambda r: r["value"], reverse=True)
    return rows


def evaluate(events: list[dict], model: RewardModel) -> list[dict]:
    rows = _evaluate_statistics(events, model, policy_names(events))
    return [{**row,
             "value": round(row["value"], 4),
             "lift_vs_logging": (round(row["lift_vs_logging"], 4)
                                 if row["lift_vs_logging"] is not None else None)}
            for row in rows]


INTERVAL_FIELDS = ["value_ci_low", "value_ci_high", "lift_ci_low", "lift_ci_high"]


def _percentile_bounds(values):
    if not values:
        return None, None
    low, high = np.percentile(np.asarray(values, dtype=float), [2.5, 97.5])
    return round(float(low), 4), round(float(high), 4)


def bootstrap_intervals(events, model, point_rows, samples=1000, seed=20260716):
    """Bootstrap event-sampling uncertainty conditional on the fixed fitted model.

    These intervals do not include uncertainty from fitting the reward model.
    """
    if samples < 0:
        raise ValueError("bootstrap samples must be nonnegative")
    enriched = [{**row, **{field: None for field in INTERVAL_FIELDS}}
                for row in point_rows]
    if samples == 0 or not events:
        return enriched
    policies = [row["policy"] for row in point_rows]
    stats = {policy: {"value": [], "lift": []} for policy in policies}
    rng = np.random.default_rng(seed)
    for _ in range(samples):
        indexes = rng.integers(0, len(events), size=len(events))
        sampled = [events[int(index)] for index in indexes]
        for row in _evaluate_statistics(sampled, model, policies):
            stats[row["policy"]]["value"].append(float(row["value"]))
            if row["lift_vs_logging"] is not None:
                stats[row["policy"]]["lift"].append(float(row["lift_vs_logging"]))
    for row in enriched:
        policy_stats = stats[row["policy"]]
        row["value_ci_low"], row["value_ci_high"] = _percentile_bounds(policy_stats["value"])
        row["lift_ci_low"], row["lift_ci_high"] = _percentile_bounds(policy_stats["lift"])
    return enriched


def _fmt_number(value, signed=False):
    if value is None:
        return "N/A"
    return f"{value:+.4f}" if signed else f"{value:.4f}"


def _fmt_interval(low, high, signed=False):
    if low is None or high is None:
        return "N/A"
    return f"[{_fmt_number(low, signed)}, {_fmt_number(high, signed)}]"


def main(argv=None) -> list[dict]:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--key", default="replay:recommendations")
    ap.add_argument("--parquet", default=None)
    ap.add_argument("--output", default=None)
    ap.add_argument("--limit", type=int, default=-1)
    ap.add_argument("--bootstrap-samples", type=int, default=1000,
                    help="event bootstrap resamples; 0 disables intervals (default: 1000)")
    ap.add_argument("--bootstrap-seed", type=int, default=20260716,
                    help="deterministic bootstrap seed (default: 20260716)")
    args = ap.parse_args(argv)
    if args.bootstrap_samples < 0:
        ap.error("--bootstrap-samples must be nonnegative")
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))

    import ope_support as replay_buffer
    if args.parquet:
        events = replay_buffer.load_from_parquet(args.parquet)
    else:
        client = redis.Redis(host=host, port=port, decode_responses=False)
        events = replay_buffer.load_from_redis(client, args.key, args.limit)

    events = [e for e in events if e.get("reward") is not None]
    if not events:
        raise SystemExit("no feedback-completed replay events (with reward) — nothing to evaluate")

    model = fit_reward_model(events)
    point_rows = evaluate(events, model)
    rows = bootstrap_intervals(events, model, point_rows,
                               samples=args.bootstrap_samples,
                               seed=args.bootstrap_seed)

    cal = model.calibration
    print(f"reward estimator calibration: auc={cal['auc']} mse={cal['mse']} (n_test={cal['n_test']})")
    print("95% event-bootstrap CIs are conditional on fixed reward model; model-fit uncertainty excluded")
    for r in rows:
        value_ci = _fmt_interval(r["value_ci_low"], r["value_ci_high"])
        lift_ci = _fmt_interval(r["lift_ci_low"], r["lift_ci_high"], signed=True)
        print(f"  {r['policy']:16s} value={_fmt_number(r['value'])}  95% CI={value_ci}  "
              f"lift={_fmt_number(r['lift_vs_logging'], signed=True)}  "
              f"95% lift CI={lift_ci}  n={r['n_events']}")
    if args.output:
        with open(args.output, "w", newline="") as fh:
            wr = csv.DictWriter(fh, fieldnames=["policy", "value", "lift_vs_logging",
                                                "n_events", "estimator_auc", "estimator_mse",
                                                *INTERVAL_FIELDS])
            wr.writeheader()
            wr.writerows(rows)
        print(f"wrote {args.output}")
    return rows


if __name__ == "__main__":
    main()
