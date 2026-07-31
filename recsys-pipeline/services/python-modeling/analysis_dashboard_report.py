#!/usr/bin/env python3
"""Consolidated analysis dashboard — keyword / query / relevance / recall / ranking as one HTML page.

Standalone pandas/Python (no Spark). Recomputes the metrics from a run's training_samples Parquet
+ Redis and writes a single self-contained index.html. Recall/ranking reuse the pure functions in
recall_eval_report.py / ranking_eval_report.py; genres/categories via feature_derivations.

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


def compute_mdp(csv_path):
    """Load a MovieLensPolicyEvaluation CSV (policy,episodes,mean_return,...) if the Java CLI wrote one."""
    if not csv_path or not os.path.exists(csv_path):
        return None
    import pandas as pd
    df = pd.read_csv(csv_path)
    if df.empty:
        return None
    best = df.loc[df["mean_return"].idxmax()]
    worst = df.loc[df["mean_return"].idxmin()]
    headline = (f"{best['policy']} return {best['mean_return']:.3f} vs "
                f"{worst['policy']} {worst['mean_return']:.3f} over {int(best['episodes'])} episodes")
    return {"headline": headline, "df": df, "path": csv_path}


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
    """Hoist allowlisted demographics out of user_features into fairness columns.

    The allowlist is the cardinality guard: a key outside DEFAULT_DIMENSIONS is
    never promoted, so no arbitrary user attribute can become a published group.
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


def _esc(x) -> str:
    return _html.escape(str(x))


def svg_bar(labels, values, title="", width=520, bar_h=22, gap=6) -> str:
    vmax = max(values) if values else 1.0
    vmax = vmax or 1.0
    rows = []
    for i, (lab, v) in enumerate(zip(labels, values)):
        y = i * (bar_h + gap)
        w = int((v / vmax) * (width - 160))
        rows.append(
            f'<g><title>{_esc(lab)}: {_esc(round(v, 4))}</title>'
            f'<text x="0" y="{y + bar_h - 6}" font-size="12">{_esc(lab)}</text>'
            f'<rect x="150" y="{y}" width="{w}" height="{bar_h}" rx="6" fill="#4f46e5"/>'
            f'<text x="{155 + w}" y="{y + bar_h - 6}" font-size="11">{_esc(round(v, 4))}</text></g>')
    h = max(len(labels), 1) * (bar_h + gap)
    cap = f'<text x="0" y="-6" font-size="13" font-weight="bold">{_esc(title)}</text>' if title else ""
    return (f'<svg class="chart" role="img" viewBox="0 -20 {width} {h + 24}" width="{width}" '
            f'font-family="sans-serif">{cap}{"".join(rows)}</svg>')


def svg_line(xs, series, title="", width=520, height=220) -> str:
    colors = ["#4f46e5", "#0d9488", "#f59e0b", "#e11d48"]
    allv = [v for vals in series.values() for v in vals] or [0.0, 1.0]
    vmin, vmax = min(allv), max(allv)
    span = (vmax - vmin) or 1.0
    xspan = (max(xs) - min(xs)) or 1
    def px(x): return 40 + (x - min(xs)) / xspan * (width - 60)
    def py(v): return height - 30 - (v - vmin) / span * (height - 50)
    lines, legend = [], []
    for idx, (name, vals) in enumerate(series.items()):
        c = colors[idx % len(colors)]
        pts = " ".join(f"{px(x):.1f},{py(v):.1f}" for x, v in zip(xs, vals))
        lines.append(f'<polyline fill="none" stroke="{c}" stroke-width="3" '
                     f'stroke-linecap="round" stroke-linejoin="round" points="{pts}">'
                     f'<title>{_esc(name)}</title></polyline>')
        legend.append(f'<text x="{60 + idx * 110}" y="16" fill="{c}" font-size="12">{_esc(name)}</text>')
    cap = f'<text x="0" y="16" font-size="13" font-weight="bold">{_esc(title)}</text>' if title else ""
    xlab = "".join(f'<text x="{px(x):.1f}" y="{height - 10}" font-size="10" '
                   f'text-anchor="middle">{_esc(x)}</text>' for x in xs)
    return (f'<svg class="chart" role="img" viewBox="0 0 {width} {height}" '
            f'width="{width}" font-family="sans-serif">'
            f'{cap}{"".join(legend)}{"".join(lines)}{xlab}</svg>')


def html_table(df, columns=None) -> str:
    cols = columns or list(df.columns)
    head = "".join(f"<th>{_esc(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{_esc(r[c])}</td>" for c in cols) + "</tr>"
                   for _, r in df.iterrows())
    table = f'<table class="rpt"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'
    return f'<div class="table-shell">{table}</div>'


def section(title, headline, body_html) -> str:
    return (f'<section class="report-card"><div class="section-heading">'
            f'<h2>{_esc(title)}</h2><p class="insight">{_esc(headline)}</p>'
            f'</div><div class="section-body">{body_html}</div></section>')


def na_card(title, reason) -> str:
    return (f'<section class="report-card status-card"><div class="section-heading">'
            f'<h2>{_esc(title)}</h2><p class="na">N/A — {_esc(reason)}</p>'
            f'</div></section>')


def render_html(title, sections) -> str:
    style = (
        ':root{--canvas:#f5f7fb;--surface:#fff;--ink:#111827;--muted:#64748b;'
        '--line:#e2e8f0;--indigo:#4f46e5;--indigo-soft:#eef2ff;--amber:#b45309;'
        '--amber-soft:#fffbeb;--shadow:0 12px 30px rgba(15,23,42,.07)}'
        '*{box-sizing:border-box}'
        'body{margin:0;background:var(--canvas);color:var(--ink);font-family:Inter,'
        'ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;line-height:1.5}'
        '.page-shell{width:min(1180px,calc(100% - 40px));margin:0 auto;padding:40px 0 64px}'
        '.hero{display:flex;justify-content:space-between;gap:32px;align-items:flex-start;'
        'padding:36px 40px;margin-bottom:24px;border-radius:24px;color:#fff;'
        'background:linear-gradient(135deg,#312e81 0%,#4f46e5 58%,#6366f1 100%);'
        'box-shadow:0 20px 50px rgba(79,70,229,.24)}'
        '.hero h1{margin:6px 0 8px;font-size:clamp(2rem,5vw,3.5rem);line-height:1.05;'
        'letter-spacing:-.04em}.hero p{margin:0;color:#e0e7ff;max-width:650px}'
        '.eyebrow{font-size:.72rem;font-weight:800;letter-spacing:.16em;color:#c7d2fe}'
        '.report-badge{flex:none;padding:8px 12px;border:1px solid rgba(255,255,255,.3);'
        'border-radius:999px;background:rgba(255,255,255,.12);font-size:.7rem;'
        'font-weight:800;letter-spacing:.08em}'
        '.report-grid{display:grid;gap:20px}'
        '.report-card{overflow:hidden;background:var(--surface);border:1px solid var(--line);'
        'border-radius:20px;box-shadow:var(--shadow)}'
        '.section-heading{padding:24px 28px 18px;border-bottom:1px solid var(--line)}'
        '.section-heading h2{margin:0 0 10px;font-size:1.3rem;letter-spacing:-.02em}'
        '.insight{display:inline-block;margin:0;padding:7px 11px;border-radius:9px;'
        'background:var(--indigo-soft);color:#3730a3;font-size:.92rem;font-weight:700}'
        '.section-body{display:grid;gap:20px;padding:24px 28px 28px}'
        '.chart{display:block;width:min(100%,720px);height:auto;overflow:visible}'
        '.table-shell{width:100%;overflow-x:auto;border:1px solid var(--line);border-radius:12px}'
        'table.rpt{width:100%;border-collapse:separate;border-spacing:0;font-size:.82rem;'
        'font-variant-numeric:tabular-nums}'
        'table.rpt th{position:sticky;top:0;padding:11px 13px;background:#f8fafc;color:#475569;'
        'text-align:left;text-transform:uppercase;letter-spacing:.045em;font-size:.68rem}'
        'table.rpt td{padding:10px 13px;border-top:1px solid var(--line);white-space:nowrap}'
        'table.rpt tbody tr:nth-child(even){background:#fafbff}'
        'table.rpt tbody tr:hover{background:var(--indigo-soft)}'
        '.status-card{border-color:#fde68a;background:var(--amber-soft)}'
        '.status-card .section-heading{border:0}.na{margin:0;color:var(--amber);font-weight:700}'
        '@media (max-width:700px){.page-shell{width:min(100% - 24px,1180px);padding-top:20px}'
        '.hero{padding:28px 24px;border-radius:18px;flex-direction:column}'
        '.section-heading,.section-body{padding-left:20px;padding-right:20px}'
        '.report-badge{align-self:flex-start}}'
        '@media (prefers-reduced-motion:reduce){*{scroll-behavior:auto!important;'
        'transition:none!important}}'
    )
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width, initial-scale=1">'
            f'<title>{_esc(title)}</title><style>{style}</style></head><body>'
            f'<main class="page-shell"><header class="hero"><div>'
            f'<span class="eyebrow">RECOMMENDER ANALYTICS</span><h1>{_esc(title)}</h1>'
            f'<p>Engagement, intent, retrieval, and ranking performance in one report.</p>'
            f'</div><span class="report-badge">OFFLINE REPORT</span></header>'
            f'<div class="report-grid">{"".join(sections)}</div></main></body></html>')


def _relevance_section(r) -> str:
    labels = ["impression", "click", "order"]
    body = svg_bar(labels, [r["funnel"][k] for k in labels], title="Funnel")
    body += html_table(r["by_query"].head(10), ["query", "impressions", "mean_score"])
    body += html_table(r["by_genre"].head(10), ["genre", "impressions", "mean_score"])
    return section("Engagement funnel", r["headline"], body)


def _keyword_section(r) -> str:
    bk = r["by_keyword"].head(10)
    body = svg_bar(list(bk["keyword"]), list(bk["movie_impressions"]), title="Impressions by keyword")
    body += html_table(bk, ["keyword", "movie_impressions", "query_clicks", "divergence"])
    for lvl in ("l1", "l2", "l3"):
        body += html_table(r["tops"][lvl].head(10), [lvl, "keyword", "movie_impressions", "query_clicks"])
    return section("Keyword gap", r["headline"], body)


def _query_section(r) -> str:
    tq = r["top_queries"].head(10)
    body = svg_bar(list(tq["query"]), list(tq["impressions"]), title="Top queries")
    body += html_table(tq, ["query", "impressions", "clicks", "orders", "query_len", "ctr", "cvr"])
    body += html_table(r["by_length"], ["bucket", "impressions", "clicks", "orders", "ctr", "cvr"])
    return section("Query intent", r["headline"], body)


def _recall_section(r) -> str:
    import pandas as pd
    rows = pd.DataFrame(r["rows"])
    ks = sorted(rows["k"].unique())
    series = {m: [float(rows[(rows.method == m) & (rows.k == k)]["recall_at_k"].iloc[0]) for k in ks]
              for m in rows["method"].unique()}
    body = svg_line(list(ks), series, title="recall@k")
    body += html_table(rows, ["method", "k", "recall_at_k", "hitrate_at_k", "users_evaluated"])
    return section("Recall", r["headline"], body)


def _ranking_section(r) -> str:
    import pandas as pd
    rows = pd.DataFrame(r["rows"])
    scored = rows[rows["auc"].notna()]
    body = svg_bar(list(scored["signal"]), list(scored["auc"]), title="AUC by signal") if len(scored) else ""
    body += html_table(rows, ["signal", "n", "positives", "coverage", "auc", "logloss"])
    return section("Ranking", r["headline"], body)


def _ci(low, high, pct=False) -> str:
    if low is None or high is None:
        return "N/A"
    return f"[{low:+.1%}, {high:+.1%}]" if pct else f"[{low:.4f}, {high:.4f}]"


_FINE_PRINT = 'margin:0;color:#64748b;font-size:.78rem'


def _ope_section(r) -> str:
    import pandas as pd
    rows = r["rows"]
    disp = pd.DataFrame([{
        "policy": x["policy"],
        "value": round(x["value"], 4),
        "value_95ci": _ci(x["value_ci_low"], x["value_ci_high"]),
        "lift_vs_logging": "N/A" if x["lift_vs_logging"] is None else f"{x['lift_vs_logging']:+.1%}",
        "lift_95ci": _ci(x["lift_ci_low"], x["lift_ci_high"], pct=True),
        "n": x["n_events"],
    } for x in rows])
    body = svg_bar([x["policy"] for x in rows], [x["value"] for x in rows],
                   title="Estimated policy value")
    body += html_table(disp, ["policy", "value", "value_95ci", "lift_vs_logging", "lift_95ci", "n"])
    cal = r["calibration"]
    auc = "N/A" if cal["auc"] is None else round(cal["auc"], 4)
    mse = "N/A" if cal["mse"] is None else round(cal["mse"], 4)
    body += (f'<p style="{_FINE_PRINT}">Direct Method · reward estimator AUC {_esc(auc)} '
             f'MSE {_esc(mse)} (n_test {_esc(cal["n_test"])}). 95% event-bootstrap '
             f'CIs are conditional on the fixed reward model; model-fit uncertainty excluded.</p>')
    return section("Off-policy evaluation", r["headline"], body)


def _mdp_section(r) -> str:
    df = r["df"].copy()
    df["ci95"] = [f"[{lo:.3f}, {hi:.3f}]" for lo, hi in zip(df["ci95_low"], df["ci95_high"])]
    for c in ("mean_return", "mean_steps", "standard_error"):
        df[c] = df[c].round(4)
    body = html_table(df, ["policy", "episodes", "mean_return", "mean_steps", "standard_error", "ci95"])
    body += (f'<p style="{_FINE_PRINT}">Finite-horizon discounted return over seeded episodes; '
             f'95% bootstrap CIs quantify episode-sampling uncertainty for this fixed dataset. '
             f'Source: {_esc(os.path.basename(r["path"]))}.</p>')
    return section("MDP policy evaluation", r["headline"], body)


def main(argv=None) -> str:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/movie-category-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--ks", default="5,10,20")
    ap.add_argument("--ope-key", default="replay:recommendations")
    ap.add_argument("--ope-bootstrap-samples", type=int, default=1000)
    ap.add_argument("--mdp-csv", default=None,
                    help="MovieLensPolicyEvaluation CSV; default <input>/../mdp_eval.csv")
    args = ap.parse_args(argv)
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    ks = tuple(int(x) for x in args.ks.split(","))
    outdir = args.outdir or f"{args.input}/../report-dashboard"

    df = load_samples(args.input, host, port)
    if len(df) == 0:
        raise SystemExit(f"no rows in {args.input} — nothing to report")

    sections = [
        _relevance_section(compute_relevance(df)),
        _keyword_section(compute_keyword(df)),
        _query_section(compute_query(df)),
    ]
    recall = compute_recall(df, host, port, ks)
    sections.append(_recall_section(recall) if recall
                    else na_card("Recall", "no movie:*:features in Redis"))
    ranking = compute_ranking(df, host, port)
    sections.append(_ranking_section(ranking) if ranking
                    else na_card("Ranking", "no popularity or i2vEmb:* signals in Redis"))
    ope = compute_ope(host, port, args.ope_key, bootstrap_samples=args.ope_bootstrap_samples)
    sections.append(_ope_section(ope) if ope
                    else na_card("Off-policy evaluation",
                                 "no replay-buffer events with reward in Redis"))
    mdp_csv = args.mdp_csv or os.path.join(args.input, "..", "mdp_eval.csv")
    mdp = compute_mdp(mdp_csv)
    sections.append(_mdp_section(mdp) if mdp
                    else na_card("MDP policy evaluation", f"no mdp_eval.csv at {mdp_csv}"))

    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "index.html")
    with open(out, "w") as fh:
        fh.write(render_html("Analysis Dashboard", sections))
    print(f"wrote {out}")
    return out


if __name__ == "__main__":
    main()
