#!/usr/bin/env python3
"""Ranking-performance evaluation — logloss + ROC-AUC of ranking signals.

Compares how well several ranking signals predict the **click** label (positive = clicked,
label ≥ 1), per impression in training_samples:

  - popularity : the item's global popularity (Redis ZSET `global:item_popularity`)
  - position   : the impression position (lower position = higher rank); score = -position
  - embedding  : dot(uEmb[user], i2vEmb[item])  (only rows with both embeddings present)

Metrics per signal:
  ROC-AUC  : rank-based (Mann–Whitney), calibration-free — the headline ranking metric.
  logloss  : on a per-signal calibration of the score (z-score → sigmoid → probability).
  coverage : fraction of impressions the signal could score (esp. embedding).

Self-contained pandas/Python (hand-rolled AUC + logloss, no new deps). Reads training_samples
Parquet + Redis. Run with plain `python` (no Spark):
    REDIS_HOST=localhost python services/python-modeling/ranking_eval_report.py --input <parquet>
"""
from __future__ import annotations

import argparse
import math
import os

SIGNALS = ["popularity", "position", "embedding"]


# ── metrics (pure) ──────────────────────────────────────────────────────────────
def auc(scores: list[float], labels: list[int]) -> float | None:
    """Rank-based ROC-AUC with average ranks for ties. None if only one class."""
    n_pos = sum(1 for y in labels if y == 1)
    n_neg = len(labels) - n_pos
    if n_pos == 0 or n_neg == 0:
        return None
    order = sorted(range(len(scores)), key=lambda i: scores[i])
    ranks = [0.0] * len(scores)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and scores[order[j + 1]] == scores[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0  # 1-based average rank for the tie group
        for t in range(i, j + 1):
            ranks[order[t]] = avg
        i = j + 1
    sum_pos = sum(ranks[i] for i in range(len(labels)) if labels[i] == 1)
    return (sum_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def logloss(probs: list[float], labels: list[int], eps: float = 1e-15) -> float:
    total = 0.0
    for p, y in zip(probs, labels):
        p = min(1 - eps, max(eps, p))
        total += -(y * math.log(p) + (1 - y) * math.log(1 - p))
    return total / len(labels) if labels else 0.0


def zsigmoid(scores: list[float]) -> list[float]:
    """Calibrate arbitrary scores to (0,1): z-score then sigmoid."""
    n = len(scores)
    if n == 0:
        return []
    mean = sum(scores) / n
    var = sum((s - mean) ** 2 for s in scores) / n
    std = math.sqrt(var)
    if std == 0:
        return [0.5] * n
    return [1.0 / (1.0 + math.exp(-(s - mean) / std)) for s in scores]


def evaluate_signal(scores: list[float], labels: list[int]) -> dict:
    """AUC + logloss over the (already filtered) score/label pairs."""
    return {
        "n": len(labels),
        "positives": sum(labels),
        "auc": auc(scores, labels),
        "logloss": round(logloss(zsigmoid(scores), labels), 4) if labels else None,
    }


# ── data loading (Redis + Parquet) ──────────────────────────────────────────────
def _redis(host, port):
    import redis
    return redis.Redis(host=host, port=port, decode_responses=True)


def fetch_popularity(host, port) -> dict:
    try:
        c = _redis(host, port)
        return {m: float(s) for m, s in c.zrange("global:item_popularity", 0, -1, withscores=True)}
    except Exception as e:  # noqa: BLE001
        print(f"[warn] popularity fetch failed: {e}")
        return {}


def fetch_embeddings(host, port):
    def grab(prefix):
        out = {}
        try:
            c = _redis(host, port)
            for key in c.scan_iter(match=f"{prefix}:*"):
                raw = c.get(key)
                if raw:
                    out[key.split(":", 1)[1]] = [float(x) for x in raw.split()]
        except Exception as e:  # noqa: BLE001
            print(f"[warn] {prefix} fetch failed: {e}")
        return out
    return grab("uEmb"), grab("i2vEmb")


def _dot(a, b):
    return sum(x * y for x, y in zip(a, b)) if a and b else None


def main(argv=None) -> None:
    import pandas as pd
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/movie-category-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    args = ap.parse_args(argv)
    outdir = args.outdir or f"{args.input}/../report-ranking-eval"

    df = pd.read_parquet(args.input)
    labels_all = ((df["clicked"] if "clicked" in df.columns else (df["label"] >= 1)).astype(int)).tolist()
    items = df["item_id"].tolist()
    users = df["user_id"].tolist() if "user_id" in df.columns else [None] * len(items)
    positions = df["position"].tolist() if "position" in df.columns else [0] * len(items)

    host, port = os.environ.get("REDIS_HOST", "localhost"), int(os.environ.get("REDIS_PORT", "6379"))
    pop = fetch_popularity(host, port)
    uemb, iemb = fetch_embeddings(host, port)

    signal_scores = {
        "popularity": [(float(pop.get(it, 0.0)), True) for it in items],
        "position": [(-float(p), True) for p in positions],
        "embedding": [((_dot(uemb.get(u), iemb.get(it)), _dot(uemb.get(u), iemb.get(it)) is not None))
                      for u, it in zip(users, items)],
    }

    rows = []
    total = len(labels_all)
    for name in SIGNALS:
        sl = [(s, labels_all[i]) for i, (s, ok) in enumerate(signal_scores[name]) if ok and s is not None]
        coverage = round(len(sl) / total, 4) if total else 0.0
        if not sl:
            rows.append({"signal": name, "n": 0, "positives": 0, "coverage": coverage,
                         "auc": None, "logloss": None})
            continue
        scores, labels = [s for s, _ in sl], [y for _, y in sl]
        m = evaluate_signal(scores, labels)
        rows.append({"signal": name, "coverage": coverage, **m})

    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "ranking_eval.csv")
    pd.DataFrame(rows)[["signal", "n", "positives", "coverage", "auc", "logloss"]].to_csv(out, index=False)
    for r in rows:
        print(f"  {r['signal']:11s} auc={r['auc']}  logloss={r['logloss']}  "
              f"(n={r['n']}, pos={r['positives']}, coverage={r['coverage']})")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
