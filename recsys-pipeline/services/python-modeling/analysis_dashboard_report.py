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
