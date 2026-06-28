#!/usr/bin/env python3
"""Recall-task evaluation — BM25 vs embedding vs hybrid retrieval (recall@k / hitrate@k).

Compares three retrieval methods on the engagement data with a leave-one-out (LOO) protocol:

  - query unit  : a user; relevant = the movies the user clicked/ordered (from training_samples)
  - protocol    : leave-one-out — for each clicked item h, build the query from the user's OTHER
                  clicks and check whether h is retrieved in the top-k from the movie corpus
  - query repr  : BM25 query = the other clicks' genres+titles; embedding query = mean of their
                  i2vEmb vectors (both derived from clicks, no extra deps)
  - methods     : bm25 (lexical), embedding (cosine), hybrid (Reciprocal Rank Fusion of the two)

Metrics (averaged over evaluated users, per method × k):
  recall@k   = mean over users of (held-out clicks recovered in top-k) / (#clicks)
  hitrate@k  = mean over users of 1[at least one held-out click recovered in top-k]

Corpus (movie title+genres) and item embeddings come from Redis (`movie:{id}:features`,
`i2vEmb:{id}`). Self-contained pandas/Python — run with plain `python` (no Spark needed):
    REDIS_HOST=localhost python services/python-modeling/recall_eval_report.py --input <parquet>
"""
from __future__ import annotations

import argparse
import math
import os
import re
from collections import Counter, defaultdict

METHODS = ["bm25", "embedding", "hybrid"]


# ── retrieval primitives (pure) ─────────────────────────────────────────────────
def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", (text or "").lower())


def build_bm25(corpus: dict[str, list[str]]) -> dict:
    n = len(corpus)
    df: Counter = Counter()
    tf, dl = {}, {}
    for doc, toks in corpus.items():
        c = Counter(toks)
        tf[doc], dl[doc] = c, len(toks)
        for t in c:
            df[t] += 1
    avgdl = (sum(dl.values()) / n) if n else 0.0
    idf = {t: math.log(1 + (n - d + 0.5) / (d + 0.5)) for t, d in df.items()}
    return {"tf": tf, "dl": dl, "avgdl": avgdl or 1.0, "idf": idf}


def bm25_score(bm: dict, doc: str, qterms, k1: float = 1.5, b: float = 0.75) -> float:
    tf, dl = bm["tf"].get(doc, {}), bm["dl"].get(doc, 0)
    s = 0.0
    for t in qterms:
        f = tf.get(t, 0)
        if f:
            s += bm["idf"].get(t, 0.0) * (f * (k1 + 1)) / (f + k1 * (1 - b + b * dl / bm["avgdl"]))
    return s


def cosine(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na, nb = math.sqrt(sum(x * x for x in a)), math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


def mean_vec(vecs: list[list[float]]) -> list[float]:
    if not vecs:
        return []
    dim = len(vecs[0])
    return [sum(v[i] for v in vecs) / len(vecs) for i in range(dim)]


def rank_topk(scores: dict[str, float], k: int) -> list[str]:
    return [item for item, _ in sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))[:k]]


def rrf(rankings: list[list[str]], k0: int = 60) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for ranking in rankings:
        for pos, item in enumerate(ranking):
            out[item] += 1.0 / (k0 + pos + 1)
    return out


# ── leave-one-out evaluation (pure) ─────────────────────────────────────────────
def evaluate(clicks_by_user: dict[str, list[str]],
             corpus: dict[str, list[str]],
             item_vecs: dict[str, list[float]],
             ks: list[int]) -> list[dict]:
    bm = build_bm25(corpus)
    all_items = list(corpus.keys())
    kmax = max(ks)
    sum_recall = {(m, k): 0.0 for m in METHODS for k in ks}
    sum_hit = {(m, k): 0.0 for m in METHODS for k in ks}
    users_eval = instances = 0

    for user, raw in clicks_by_user.items():
        clicks = [c for c in dict.fromkeys(raw) if c in corpus]   # dedup, must be in corpus
        if len(clicks) < 2:
            continue
        users_eval += 1
        folds = 0
        hits = {(m, k): 0 for m in METHODS for k in ks}
        for h in clicks:
            others = [c for c in clicks if c != h]
            seen = set(others)
            candidates = [it for it in all_items if it not in seen]   # includes h
            qterms = {t for o in others for t in corpus[o]}
            bm_rank = rank_topk({c: bm25_score(bm, c, qterms) for c in candidates}, kmax)
            ovecs = [item_vecs[o] for o in others if o in item_vecs]
            if ovecs:
                qv = mean_vec(ovecs)
                emb_rank = rank_topk({c: cosine(qv, item_vecs[c]) for c in candidates if c in item_vecs}, kmax)
            else:
                emb_rank = []
            hyb_rank = rank_topk(rrf([bm_rank, emb_rank]), kmax)
            ranks = {"bm25": bm_rank, "embedding": emb_rank, "hybrid": hyb_rank}
            folds += 1
            instances += 1
            for m in METHODS:
                for k in ks:
                    if h in ranks[m][:k]:
                        hits[(m, k)] += 1
        for m in METHODS:
            for k in ks:
                sum_recall[(m, k)] += hits[(m, k)] / folds
                sum_hit[(m, k)] += 1.0 if hits[(m, k)] > 0 else 0.0

    rows = []
    for m in METHODS:
        for k in ks:
            rows.append({
                "method": m, "k": k,
                "recall_at_k": round(sum_recall[(m, k)] / users_eval, 4) if users_eval else 0.0,
                "hitrate_at_k": round(sum_hit[(m, k)] / users_eval, 4) if users_eval else 0.0,
                "users_evaluated": users_eval, "instances": instances,
            })
    return rows


# ── data loading (Redis + Parquet) ──────────────────────────────────────────────
def fetch_corpus_and_vecs(host: str, port: int):
    import redis
    c = redis.Redis(host=host, port=port, decode_responses=True)
    corpus, vecs = {}, {}
    for key in c.scan_iter(match="movie:*:features"):
        h = c.hgetall(key)
        if not h:
            continue
        item = key.split(":")[1]
        corpus[item] = tokenize(f"{h.get('title', '')} {(h.get('genres') or '').replace(',', ' ')}")
    for key in c.scan_iter(match="i2vEmb:*"):
        raw = c.get(key)
        if raw:
            vecs[key.split(":", 1)[1]] = [float(x) for x in raw.split()]
    return corpus, vecs


def main(argv=None) -> None:
    import pandas as pd
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/movie-category-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--ks", default="5,10,20")
    args = ap.parse_args(argv)
    ks = [int(x) for x in args.ks.split(",")]
    outdir = args.outdir or f"{args.input}/../report-recall-eval"

    df = pd.read_parquet(args.input)
    rel = df[df["clicked"] == 1] if "clicked" in df.columns else df[df["label"] > 0]
    clicks_by_user = rel.groupby("user_id")["item_id"].apply(list).to_dict()

    corpus, vecs = fetch_corpus_and_vecs(os.environ.get("REDIS_HOST", "localhost"),
                                         int(os.environ.get("REDIS_PORT", "6379")))
    if not corpus:
        print("no movie corpus in Redis (movie:*:features) — cannot evaluate; exiting")
        return

    rows = evaluate(clicks_by_user, corpus, vecs, ks)
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "recall_eval.csv")
    pd.DataFrame(rows).to_csv(out, index=False)
    for r in rows:
        print(f"  {r['method']:9s} @{r['k']:<3d} recall={r['recall_at_k']:.4f} "
              f"hitrate={r['hitrate_at_k']:.4f}  (users={r['users_evaluated']}, instances={r['instances']})")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
