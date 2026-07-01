#!/usr/bin/env python3
"""Consolidated analysis dashboard — keyword / query / relevance / recall / ranking as one HTML page.

Standalone pandas/Python (no Spark). Recomputes the metrics from a run's training_samples Parquet
+ Redis and writes a single self-contained index.html. Recall/ranking reuse the pure functions in
recall_eval_report.py / ranking_eval_report.py; genres/categories via genre_meta / movie_categories.

    REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py --input <parquet>
"""
from __future__ import annotations

import argparse
import os


def query_of(genres: list[str]) -> str:
    return " ".join(genres) if genres else "unknown"


def load_samples(input_dir: str, host: str = "localhost", port: int = 6379):
    import pandas as pd
    df = pd.read_parquet(input_dir)
    if "label" not in df.columns:
        df["label"] = (df["clicked"] if "clicked" in df.columns else 0).astype(float)
    df["label"] = df["label"].astype(float)
    if "clicked" not in df.columns:
        df["clicked"] = (df["label"] >= 1).astype(int)
    df["clicked"] = df["clicked"].astype(int)
    if "genres" not in df.columns:
        from genre_meta import fetch_movie_meta
        meta = {m["item_id"]: m["genres"] for m in fetch_movie_meta(host, port)}
        df["genres"] = df["item_id"].astype(str).map(lambda i: meta.get(i, []))
    df["genres"] = df["genres"].apply(lambda g: list(g) if g is not None else [])
    return df


def compute_relevance(df) -> dict:
    n = len(df)
    clicks = int((df["label"] >= 1).sum())
    orders = int((df["label"] >= 2).sum())
    ctr = round(clicks / n, 4) if n else 0.0
    cvr = round(orders / n, 4) if n else 0.0
    q = df.assign(query=df["genres"].apply(query_of))
    by_query = (q.groupby("query")
                 .agg(impressions=("label", "size"), mean_score=("label", "mean"))
                 .reset_index()
                 .sort_values(["mean_score", "impressions"], ascending=[False, False]))
    ex = df.explode("genres").dropna(subset=["genres"])
    by_genre = (ex.groupby("genres")
                  .agg(impressions=("label", "size"), mean_score=("label", "mean"))
                  .reset_index().rename(columns={"genres": "genre"})
                  .sort_values(["mean_score", "impressions"], ascending=[False, False]))
    return {
        "headline": f"impressions {n} · CTR {ctr:.0%} · CVR {cvr:.0%}",
        "ctr": ctr, "cvr": cvr,
        "funnel": {"impression": n, "click": clicks, "order": orders},
        "by_query": by_query, "by_genre": by_genre,
    }


def compute_keyword(df) -> dict:
    import movie_categories as mc
    import pandas as pd

    d = df.assign(keyword=df["genres"].apply(mc.primary_genre),
                  subkeyword=df["genres"].apply(mc.secondary_genre))

    def dist(col):
        movie = d.groupby(col).size().rename("movie_impressions")
        query = d[d["clicked"] == 1].groupby(col).size().rename("query_clicks")
        out = pd.concat([movie, query], axis=1).fillna(0).reset_index()
        out[["movie_impressions", "query_clicks"]] = out[["movie_impressions", "query_clicks"]].astype(int)
        tot_m = out["movie_impressions"].sum() or 1
        tot_q = out["query_clicks"].sum() or 1
        out["movie_share"] = (out["movie_impressions"] / tot_m).round(4)
        out["query_share"] = (out["query_clicks"] / tot_q).round(4)
        out["divergence"] = (out["query_share"] - out["movie_share"]).round(4)
        return out.sort_values("movie_impressions", ascending=False)

    by_keyword = dist("keyword")
    by_subkeyword = dist("subkeyword")

    year = df["release_year"] if "release_year" in df.columns else [None] * len(df)
    lv = df.assign(
        l1=df["genres"].apply(mc.l1),
        l2=df["genres"].apply(mc.l2),
        l3=[mc.l3(g, y) for g, y in zip(df["genres"], year)],
    )

    def top_keywords(level):
        ex = lv[[level, "genres", "clicked"]].explode("genres").dropna(subset=["genres"])
        g = (ex.groupby([level, "genres"])
               .agg(movie_impressions=("clicked", "size"), query_clicks=("clicked", "sum"))
               .reset_index().rename(columns={"genres": "keyword"}))
        g["rank"] = (g.groupby(level)["movie_impressions"]
                       .rank(method="first", ascending=False).astype(int))
        return g[g["rank"] <= 10].sort_values([level, "rank"])

    tops = {lvl: top_keywords(lvl) for lvl in ("l1", "l2", "l3")}
    top_div = by_keyword.reindex(by_keyword["divergence"].abs().sort_values(ascending=False).index)
    lead = top_div.iloc[0] if len(top_div) else None
    headline = ("no keywords" if lead is None else
                f"'{lead['keyword']}' diverges most: shown {lead['movie_share']:.0%} vs clicked {lead['query_share']:.0%}")
    return {"headline": headline, "by_keyword": by_keyword,
            "by_subkeyword": by_subkeyword, "tops": tops}


SHORT_MAX_CHARS = 10


def _rates(g):
    g["ctr"] = (g["clicks"] / g["impressions"]).round(4)
    g["cvr"] = (g["orders"] / g["impressions"]).round(4)
    return g


def compute_query(df) -> dict:
    d = df.assign(query=df["genres"].apply(query_of))
    d["query_len"] = d["query"].str.len()
    d["clk"] = (d["label"] >= 1).astype(int)
    d["ord"] = (d["label"] >= 2).astype(int)

    top = (d.groupby("query")
             .agg(impressions=("label", "size"), clicks=("clk", "sum"),
                  orders=("ord", "sum"), query_len=("query_len", "first"))
             .reset_index())
    top = _rates(top).sort_values("impressions", ascending=False)

    d["bucket"] = d["query_len"].apply(
        lambda n: "short (<=10)" if n <= SHORT_MAX_CHARS else "long (>10)")
    bylen = (d.groupby("bucket")
               .agg(impressions=("label", "size"), clicks=("clk", "sum"), orders=("ord", "sum"))
               .reset_index())
    bylen = _rates(bylen).sort_values("bucket")

    lead = top.iloc[0] if len(top) else None
    headline = ("no queries" if lead is None else
                f"top query '{lead['query']}' ({int(lead['impressions'])} impr, CTR {lead['ctr']:.0%})")
    return {"headline": headline, "top_queries": top, "by_length": bylen}


def compute_recall(df, host: str, port: int, ks=(5, 10, 20)):
    from recall_eval_report import evaluate, fetch_corpus_and_vecs
    try:
        corpus, vecs = fetch_corpus_and_vecs(host, port)
    except Exception:  # noqa: BLE001 — Redis unreachable
        corpus, vecs = {}, {}
    if not corpus:
        return None
    rel = df[df["clicked"] == 1] if "clicked" in df.columns else df[df["label"] > 0]
    clicks_by_user = (rel.assign(item_id=rel["item_id"].astype(str))
                         .groupby(rel["user_id"].astype(str))["item_id"].apply(list).to_dict())
    rows = evaluate(clicks_by_user, corpus, vecs, list(ks))
    hy = next((r for r in rows if r["method"] == "hybrid" and r["k"] == 10), None)
    bm = next((r for r in rows if r["method"] == "bm25" and r["k"] == 10), None)
    headline = ("no evaluable users" if not hy else
                f"hybrid recall@10 {hy['recall_at_k']:.3f} vs BM25 {bm['recall_at_k']:.3f}")
    return {"headline": headline, "rows": rows}


def compute_ranking(df, host: str, port: int):
    from ranking_eval_report import (SIGNALS, _dot, evaluate_signal,
                                     fetch_embeddings, fetch_popularity)
    pop = fetch_popularity(host, port)
    uemb, iemb = fetch_embeddings(host, port)
    if not pop and not iemb:
        return None

    items = df["item_id"].astype(str).tolist()
    users = df["user_id"].astype(str).tolist() if "user_id" in df.columns else [None] * len(items)
    positions = df["position"].tolist() if "position" in df.columns else [0] * len(items)
    labels_all = (df["label"] >= 1).astype(int).tolist()

    signal_scores = {
        "popularity": [(float(pop.get(it, 0.0)), True) for it in items],
        "position": [(-float(p), True) for p in positions],
        "embedding": [((_dot(uemb.get(u), iemb.get(it)), _dot(uemb.get(u), iemb.get(it)) is not None))
                      for u, it in zip(users, items)],
    }
    rows, total = [], len(labels_all)
    for name in SIGNALS:
        sl = [(s, labels_all[i]) for i, (s, ok) in enumerate(signal_scores[name]) if ok and s is not None]
        coverage = round(len(sl) / total, 4) if total else 0.0
        if not sl:
            rows.append({"signal": name, "n": 0, "positives": 0, "coverage": coverage,
                         "auc": None, "logloss": None})
            continue
        m = evaluate_signal([s for s, _ in sl], [y for _, y in sl])
        rows.append({"signal": name, "coverage": coverage, **m})
    best = max((r for r in rows if r["auc"] is not None), key=lambda r: r["auc"], default=None)
    headline = ("no scorable signal" if best is None else
                f"best signal '{best['signal']}' AUC {best['auc']:.3f}")
    return {"headline": headline, "rows": rows}
