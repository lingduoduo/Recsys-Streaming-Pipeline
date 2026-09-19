#!/usr/bin/env python3
"""Compute layer behind the analysis dashboard — the metrics, not their presentation.

Standalone pandas/Python (no Spark). Recomputes the metrics from a run's training_samples Parquet
+ Redis and returns them as plain dicts and DataFrames. Recall/ranking reuse the pure functions in
recall_eval_report.py / ranking_eval_report.py; off-policy evaluation reuses ope_eval_report.py;
genres/categories via feature_derivations.

The only consumer is frontend/export_dashboard_json.py, which writes frontend/data/dashboard.json
for the Next.js dashboard. This module renders nothing and has no command line.
"""
from __future__ import annotations

import os


def query_of(genres: list[str]) -> str:
    return " ".join(genres) if genres else "unknown"


def load_samples(input_dir: str, host: str = "localhost", port: int = 6379):
    import pandas as pd
    df = pd.read_parquet(input_dir)
    if df.empty:
        raise ValueError(
            f"No training samples found at {input_dir}. "
            "Wait for the streaming simulation to finish before exporting the dashboard."
        )
    if "label" not in df.columns:
        df["label"] = (df["clicked"] if "clicked" in df.columns else 0).astype(float)
    df["label"] = df["label"].astype(float)
    if "clicked" not in df.columns:
        df["clicked"] = (df["label"] >= 1).astype(int)
    df["clicked"] = df["clicked"].astype(int)
    if "genres" not in df.columns:
        df["genres"] = [[] for _ in range(len(df))]
    df["genres"] = df["genres"].apply(lambda g: list(g) if g is not None else [])
    missing_genres = ~df["genres"].map(bool)
    if missing_genres.any():
        from feature_derivations import fetch_movie_meta
        meta = {m["item_id"]: m["genres"] for m in fetch_movie_meta(host, port)}
        enriched = df.loc[missing_genres, "item_id"].astype(str).map(lambda i: meta.get(i, []))
        for idx, genres in enriched.items():
            df.at[idx, "genres"] = list(genres)
    return df


def load_slates(path: str | None, host: str = "localhost", port: int = 6379):
    """Load slate experiences (Parquet dir or JSON file) with catalog signals attached.

    Slate items carry rankings and labels but not genres/popularity, so the
    diversity measures read those from the same Redis catalog the other
    sections use. Absent catalog data leaves the signals missing, never zero.
    """
    if not path:
        return None
    import pandas as pd
    if os.path.isfile(path) and path.endswith((".json", ".jsonl")):
        slates = pd.read_json(path, lines=path.endswith(".jsonl"))
    else:
        slates = pd.read_parquet(path)
    if "items" not in slates.columns:
        return slates
    from feature_derivations import fetch_movie_meta
    from ranking_eval_report import fetch_popularity
    genres = {str(m["item_id"]): list(m["genres"]) for m in fetch_movie_meta(host, port)}
    popularity = fetch_popularity(host, port)
    slates["items"] = slates["items"].apply(
        lambda items: [_slate_item(item, genres, popularity) for item in (items if items is not None else [])])
    return slates


def _slate_item(item, genres: dict, popularity: dict) -> dict:
    import pandas as pd
    values = dict(item)
    item_id = str(values.get("item_id"))
    observed = values.get("genres")
    values["genres"] = list(observed) if observed is not None and len(observed) else genres.get(item_id, [])
    if pd.isna(values.get("popularity")) and item_id in popularity:
        values["popularity"] = popularity[item_id]
    return values


def compute_relevance(df) -> dict:
    n = len(df)
    clicks = int((df["label"] >= 1).sum())
    orders = int((df["label"] >= 2).sum())
    ctr = round(clicks / n, 4) if n else 0.0
    cvr = round(orders / n, 4) if n else 0.0
    d = df.assign(query=df["genres"].apply(query_of),
                  clk=(df["label"] >= 1).astype(int),
                  ord=(df["label"] >= 2).astype(int))
    by_query = (d.groupby("query")
                 .agg(impressions=("label", "size"), clicks=("clk", "sum"),
                      orders=("ord", "sum"), mean_score=("label", "mean"))
                 .reset_index())
    by_query = _rates(by_query).sort_values(["mean_score", "impressions"],
                                            ascending=[False, False])
    ex = d.explode("genres").dropna(subset=["genres"])
    by_genre = (ex.groupby("genres")
                  .agg(impressions=("label", "size"), clicks=("clk", "sum"),
                       orders=("ord", "sum"), mean_score=("label", "mean"))
                  .reset_index().rename(columns={"genres": "genre"}))
    by_genre = _rates(by_genre).sort_values(["mean_score", "impressions"],
                                            ascending=[False, False])
    return {
        "headline": f"impressions {n} · CTR {ctr:.0%} · CVR {cvr:.0%}",
        "ctr": ctr, "cvr": cvr,
        "funnel": {"impression": n, "click": clicks, "order": orders},
        "by_query": by_query, "by_genre": by_genre,
    }


def compute_keyword(df) -> dict:
    import feature_derivations as mc

    d = df.assign(keyword=df["genres"].apply(mc.primary_genre),
                  subkeyword=df["genres"].apply(mc.secondary_genre))

    def dist(col):
        agg = (d.assign(clk=(d["label"] >= 1).astype(int),
                        ord=(d["label"] >= 2).astype(int))
                .groupby(col)
                .agg(movie_impressions=("label", "size"), query_clicks=("clk", "sum"),
                     query_orders=("ord", "sum"), mean_score=("label", "mean"))
                .reset_index())
        counts = ["movie_impressions", "query_clicks", "query_orders"]
        agg[counts] = agg[counts].astype(int)
        agg["mean_score"] = agg["mean_score"].round(4)
        tot_m = agg["movie_impressions"].sum() or 1
        tot_q = agg["query_clicks"].sum() or 1
        agg["movie_share"] = (agg["movie_impressions"] / tot_m).round(4)
        agg["query_share"] = (agg["query_clicks"] / tot_q).round(4)
        agg["divergence"] = (agg["query_share"] - agg["movie_share"]).round(4)
        agg["ctr"] = (agg["query_clicks"] / agg["movie_impressions"]).round(4)
        agg["cvr"] = (agg["query_orders"] / agg["movie_impressions"]).round(4)
        return agg.sort_values("movie_impressions", ascending=False)

    by_keyword = dist("keyword")
    by_subkeyword = dist("subkeyword")

    year = df["release_year"] if "release_year" in df.columns else [None] * len(df)
    lv = df.assign(
        l1=df["genres"].apply(mc.l1),
        l2=df["genres"].apply(mc.l2),
        l3=[mc.l3(g, y) for g, y in zip(df["genres"], year)],
    )

    def top_keywords(level):
        ex = lv[[level, "genres", "label"]].explode("genres").dropna(subset=["genres"])
        ex = ex.assign(clk=(ex["label"] >= 1).astype(int))
        g = (ex.groupby([level, "genres"])
               .agg(movie_impressions=("clk", "size"), query_clicks=("clk", "sum"))
               .reset_index().rename(columns={"genres": "keyword"}))
        g["ctr"] = (g["query_clicks"] / g["movie_impressions"]).round(4)
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
               .agg(impressions=("label", "size"), clicks=("clk", "sum"), orders=("ord", "sum"), queries=("query", "nunique"))
               .reset_index())
    bylen = _rates(bylen).sort_values("bucket")

    lead = top.iloc[0] if len(top) else None
    headline = ("no queries" if lead is None else
                f"top query '{lead['query']}' ({int(lead['impressions'])} impr, CTR {lead['ctr']:.0%})")
    return {"headline": headline, "top_queries": top, "by_length": bylen,
            "average_query_length": round(float(d["query_len"].mean()), 2) if len(d) else None}


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
    k10 = 10 if 10 in ks else max(ks)
    hy = next((r for r in rows if r["method"] == "hybrid" and r["k"] == k10), None)
    bm = next((r for r in rows if r["method"] == "bm25" and r["k"] == k10), None)
    headline = ("no evaluable users" if not hy else
                f"hybrid recall@{k10} {hy['recall_at_k']:.3f} vs BM25 {bm['recall_at_k']:.3f}")
    return {"headline": headline, "rows": rows}


def compute_ranking(df, host: str, port: int):
    from ranking_eval_report import (SIGNALS, _dot, evaluate_signal,
                                     fetch_embeddings, fetch_popularity)
    pop = fetch_popularity(host, port)
    uemb, iemb = fetch_embeddings(host, port)

    items = df["item_id"].astype(str).tolist()
    users = df["user_id"].astype(str).tolist() if "user_id" in df.columns else [None] * len(items)
    positions = df["position"].tolist() if "position" in df.columns else [0] * len(items)
    labels_all = (df["label"] >= 1).astype(int).tolist()

    signal_scores = {
        "popularity": [(float(pop[it]), True) if it in pop else (None, False)
                       for it in items],
        "position": [(-float(p), True) for p in positions],
        "embedding": [(d, d is not None)
                      for d in (_dot(uemb.get(u), iemb.get(it)) for u, it in zip(users, items))],
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
    for row in rows:
        row["positive_rate"] = round(row["positives"] / row["n"], 4) if row["n"] else None
    best = max((r for r in rows if r["auc"] is not None), key=lambda r: r["auc"], default=None)
    headline = ("no scorable signal" if best is None else
                f"best signal '{best['signal']}' AUC {best['auc']:.3f}")
    return {"headline": headline, "rows": rows}


def compute_ope(host, port, key="replay:recommendations", limit=-1, bootstrap_samples=1000):
    """Direct-Method off-policy evaluation over the Redis replay buffer (reuses ope_eval_report)."""
    try:
        import redis
        import ope_support as replay_buffer
        client = redis.Redis(host=host, port=port, decode_responses=False)
        events = replay_buffer.load_from_redis(client, key, limit)
    except Exception:  # noqa: BLE001 — Redis unreachable / no buffer
        return None
    events = [e for e in events if e.get("reward") is not None]
    if not events:
        return None
    import ope_eval_report as ope
    try:
        model = ope.fit_reward_model(events)
        rows = ope.bootstrap_intervals(events, model, ope.evaluate(events, model),
                                       samples=bootstrap_samples)
    except Exception:  # noqa: BLE001 — too few events to fit / evaluate
        return None
    cal = model.calibration
    best = max(rows, key=lambda r: r["value"])
    log_val = next((r["value"] for r in rows if r["policy"] == "logging"), None)
    auc = "N/A" if cal["auc"] is None else f"{cal['auc']:.3f}"
    headline = (f"best '{best['policy']}' value {best['value']:.3f}"
                + (f" vs logging {log_val:.3f}" if log_val is not None else "")
                + f" · est AUC {auc}")
    return {"headline": headline, "rows": rows, "calibration": cal}


MEASUREMENT_SCHEMA_VERSION = "2.0"

MEASUREMENT_DEFAULTS = {
    "fairness_min_support": 100,
    "freshness_window_days": 30,
    "long_tail_percentile": 0.80,
    "safety_policy_version": "catalog-filter-v1",
    "now": None,
}

# Live feedback signals published by the Java service, mapped onto the offline column names.
_LIVE_FEEDBACK_COLUMNS = {
    "request_id": "request_id_coverage",
    "rating": "rating_coverage",
    "negative_feedback_reason": "negative_feedback_coverage",
    "dwell_millis": "dwell_coverage",
    "completion_rate": "completion_coverage",
}


def measurement_config(config: dict | None = None) -> dict:
    """Validate configured measurement ranges before any calculation runs."""
    cfg = {**MEASUREMENT_DEFAULTS, **(config or {})}
    if int(cfg["fairness_min_support"]) <= 0:
        raise ValueError("fairness-min-support must be a positive integer")
    if int(cfg["freshness_window_days"]) < 0:
        raise ValueError("freshness-window-days must be non-negative")
    if not 0.0 < float(cfg["long_tail_percentile"]) < 1.0:
        raise ValueError("long-tail-percentile must be between zero and one")
    if not str(cfg["safety_policy_version"]).strip():
        raise ValueError("safety-policy-version must not be blank")
    return cfg


def build_measurement_dashboard(samples, slates, live, config: dict | None = None) -> dict:
    """Consolidate offline calculators and the live snapshot into the seven measurement sections.

    Pure: every section is a measurement envelope, and a missing input yields an
    explicit unavailable reason rather than a zero.
    """
    from datetime import datetime, timezone

    import governance_measurements as governance
    import quality_measurements as quality
    from measurement_contract import unavailable

    cfg = measurement_config(config)
    now = cfg["now"] or datetime.now(timezone.utc)
    measured = _with_demographic_columns(_with_published_timestamps(samples))
    live_measurements = (live or {}).get("measurements", live) or {}
    no_slates = unavailable("missing slate experiences")

    return {
        "relevance": quality.compute_relevance(slates) if slates is not None else no_slates,
        "satisfaction": _merge_live_row(
            quality.compute_satisfaction(measured), _live_feedback(live_measurements)),
        "freshness": _merge_live_row(
            quality.compute_freshness(measured, now, int(cfg["freshness_window_days"])),
            _live_freshness(live_measurements)),
        "diversity": (quality.compute_diversity(slates, float(cfg["long_tail_percentile"]))
                      if slates is not None else no_slates),
        "fairness": governance.compute_fairness(measured, int(cfg["fairness_min_support"])),
        "safety": _merge_live_row(
            governance.compute_safety(measured, str(cfg["safety_policy_version"])),
            _live_safety(live_measurements)),
        "latency": _live_latency(live_measurements),
    }


def _with_published_timestamps(samples):
    """Interpret the pipeline's epoch-second published_at as UTC instants."""
    import pandas as pd
    if "published_at" not in samples.columns or not pd.api.types.is_numeric_dtype(samples["published_at"]):
        return samples
    converted = samples.copy()
    converted["published_at"] = pd.to_datetime(samples["published_at"], unit="s", utc=True, errors="coerce")
    return converted


def _feature_map(value) -> dict:
    """Read a features column entry as a dict; Parquet maps decode as key/value pairs."""
    if isinstance(value, dict):
        return value
    if isinstance(value, (list, tuple)):
        return {pair[0]: pair[1] for pair in value if len(pair) == 2}
    return {}


def _with_demographic_columns(samples):
    """Hoist allowlisted demographics into fairness columns.

    A dimension already present as a typed column is left alone. Otherwise it is read
    from user_features, which is where user attributes live until they are typed. The
    allowlist is the cardinality guard: a key outside DEFAULT_DIMENSIONS is never
    promoted, so no arbitrary user attribute can become a published group.
    """
    from governance_measurements import DEFAULT_DIMENSIONS
    if "user_features" not in samples.columns:
        return samples
    features = [_feature_map(value) for value in samples["user_features"]]
    missing = [dimension for dimension in DEFAULT_DIMENSIONS
               if dimension not in samples.columns and any(dimension in entry for entry in features)]
    if not missing:
        return samples
    hoisted = samples.copy()
    for dimension in missing:
        hoisted[dimension] = [entry.get(dimension) for entry in features]
    return hoisted


def _merge_live_row(offline: dict, live_entry) -> dict:
    """Append the live row to the offline rows; never overwrite an offline measurement."""
    from measurement_contract import available
    if live_entry is None:
        return offline
    row, sample_size, coverage, headline = live_entry
    if offline["status"] != "available":
        return available(headline, [row], sample_size, coverage, warnings=offline["warnings"])
    return {**offline, "rows": [{"scope": "offline", **entry} for entry in offline["rows"]] + [row]}


def _live_freshness(live: dict):
    freshness = live.get("freshness") or {}
    exposures = int(freshness.get("exposures") or 0)
    # The service reports "available" from startup; without exposures there is nothing measured.
    if freshness.get("availability") != "available" or exposures <= 0:
        return None
    row = {
        "scope": "live_service",
        "freshness_source": freshness.get("source"),
        "fresh_share": freshness.get("freshShare"),
        "freshness_coverage": freshness.get("coverage"),
        "exposures": freshness.get("exposures"),
    }
    return row, exposures, float(freshness.get("coverage") or 0.0), "Live fresh-item exposure"


def _live_safety(live: dict):
    from measurement_contract import safe_ratio
    safety = live.get("safety") or {}
    evaluated = int(safety.get("evaluatedCandidates") or 0)
    if safety.get("availability") != "available" or evaluated <= 0:
        return None
    decisions = safety.get("totalDecisions")
    row = {
        "scope": "live_service",
        "policy_version": safety.get("policyVersion"),
        "evaluated_candidates": evaluated,
        "filter_decisions": decisions,
        "filter_decision_rate": safe_ratio(decisions, evaluated) if decisions is not None else None,
        "reason_counts": {reason: values.get("count")
                          for reason, values in sorted((safety.get("reasons") or {}).items())},
        "unknown_share": safety.get("unknownShare"),
    }
    # The live service logs filter decisions but no unsafe labels: one of the two safety signals.
    return row, evaluated, 0.5, "Live candidate safety policy accounting"


def _live_feedback(live: dict):
    feedback = live.get("feedbackCoverage") or {}
    total = int(feedback.get("total") or 0)
    if feedback.get("availability") != "available" or total <= 0:
        return None
    signals = feedback.get("signals") or {}
    coverages = {column: (signals.get(signal) or {}).get("coverage")
                 for signal, column in _LIVE_FEEDBACK_COLUMNS.items()}
    row = {"scope": "live_service", "feedback_events": total, **coverages}
    observed = [value for value in coverages.values() if isinstance(value, (int, float))]
    return row, total, (sum(observed) / len(observed) if observed else 0.0), "Live feedback signal coverage"


def _live_latency(live: dict) -> dict:
    from measurement_contract import available, safe_ratio, unavailable
    latency = live.get("latency") or {}
    if not latency:
        return unavailable("missing live measurement snapshot")
    if latency.get("availability") != "available":
        return unavailable("live latency measurement unavailable")

    unit = latency.get("unit", "milliseconds")

    def row(scope, name, values, endpoint):
        return {
            "scope": scope, "name": name, "unit": unit,
            "p50": values.get("p50"), "p95": values.get("p95"), "p99": values.get("p99"),
            "count": values.get("count"),
            "error_rate": values.get("errorRate") if endpoint else None,
            "timeout_rate": values.get("timeoutRate") if endpoint else None,
        }

    rows = ([row("endpoint", name, values, True)
             for name, values in sorted((latency.get("endpoints") or {}).items())]
            + [row("stage", name, values, False)
               for name, values in sorted((latency.get("stages") or {}).items())])
    if not rows:
        return unavailable("missing live latency measurements")
    requests = sum(entry["count"] or 0 for entry in rows if entry["scope"] == "endpoint")
    if requests <= 0:
        return unavailable("no live requests recorded")
    coverage = safe_ratio(sum(bool(entry["count"]) for entry in rows), len(rows)) or 0.0
    return available(f"Live request and stage latency ({unit})", rows, requests, coverage)


import html as _html
