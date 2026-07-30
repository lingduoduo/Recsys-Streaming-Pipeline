#!/usr/bin/env python3
"""Export the analysis-dashboard data as JSON for the Next.js frontend.

Reuses the pure compute_* functions in analysis_dashboard_report.py so the React
dashboard renders exactly what the Python HTML dashboard would. Sections whose
inputs are unavailable (e.g. no Redis corpus) serialize as null and the UI shows
an N/A card.

    REDIS_HOST=localhost python frontend/export_dashboard_json.py \
        --input /tmp/spark-recsys/training-samples \
        --output frontend/data/dashboard.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "recsys-pipeline" / "services" / "python-modeling"))

import analysis_dashboard_report as dash  # noqa: E402


def _records(df):
    return df.to_dict(orient="records")


def _json_default(o):
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        return float(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    raise TypeError(f"not serializable: {type(o)}")


def _json_safe(value):
    """Replace non-finite floats with null so the snapshot is strict JSON."""
    import math
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (float, np.floating)) and not math.isfinite(value):
        return None
    return value


SLATE_ROW_LIMIT = 10


def _bounded_slate_rows(diversity: dict) -> dict:
    """Publish the diversity aggregate plus a bounded sample of per-slate rows.

    One row per slate would put every request id in the committed snapshot; the
    aggregate keeps the full support and the warning states what was dropped.
    """
    rows = diversity.get("rows") or []
    if diversity["status"] != "available" or len(rows) <= SLATE_ROW_LIMIT + 1:
        return diversity
    kept = rows[: SLATE_ROW_LIMIT + 1]  # rows[0] is the aggregate over every slate
    warning = f"showing {len(kept) - 1} of {len(rows) - 1} slate rows; the aggregate covers all"
    return {**diversity, "rows": kept, "warnings": [*diversity["warnings"], warning]}


def build(input_dir: str, host: str, port: int, mdp_csv: str | None,
          experiences: str | None = None, live_metrics: str | None = None,
          config: dict | None = None) -> dict:
    df = dash.load_samples(input_dir, host, port)
    slates = dash.load_slates(experiences, host, port)
    live = json.loads(Path(live_metrics).read_text()) if live_metrics else None
    measurements = dash.build_measurement_dashboard(df, slates, live, config)
    measurements["diversity"] = _bounded_slate_rows(measurements["diversity"])

    rel = dash.compute_relevance(df)
    engagement = {
        "headline": rel["headline"], "ctr": rel["ctr"], "cvr": rel["cvr"],
        "funnel": rel["funnel"],
        "by_query": _records(rel["by_query"].head(10)),
        "by_genre": _records(rel["by_genre"].head(10)),
    }

    kw = dash.compute_keyword(df)
    keyword = {
        "headline": kw["headline"],
        "by_keyword": _records(kw["by_keyword"].head(10)),
        "by_subkeyword": _records(kw["by_subkeyword"].head(10)),
        "tops": {lvl: _records(kw["tops"][lvl].head(10)) for lvl in ("l1", "l2", "l3")},
    }

    qy = dash.compute_query(df)
    query = {
        "headline": qy["headline"],
        "top_queries": _records(qy["top_queries"].head(10)),
        "by_length": _records(qy["by_length"]),
    }

    recall = dash.compute_recall(df, host, port)
    ranking = dash.compute_ranking(df, host, port)
    ope = dash.compute_ope(host, port)
    mdp = dash.compute_mdp(mdp_csv)

    return _json_safe({
        "schemaVersion": dash.MEASUREMENT_SCHEMA_VERSION,
        "input": input_dir,
        "rows": int(len(df)),
        **measurements,  # relevance, satisfaction, freshness, diversity, fairness, safety, latency
        "engagement": engagement,
        "keyword": keyword,
        "query": query,
        "recall": recall,  # {headline, rows} or None
        "ranking": {"headline": ranking["headline"], "rows": ranking["rows"]} if ranking else None,
        "ope": ope,        # {headline, rows, calibration} or None
        "mdp": {"headline": mdp["headline"], "rows": _records(mdp["df"])} if mdp else None,
    })


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/training-samples")
    ap.add_argument("--output", default=str(_REPO / "frontend" / "data" / "dashboard.json"))
    ap.add_argument("--mdp-csv", default=None)
    ap.add_argument("--experiences", default=None,
                    help="slate experiences Parquet directory or JSON file (relevance/diversity)")
    ap.add_argument("--live-metrics", default=None,
                    help="JSON captured from the retrieval service /metrics endpoint (latency)")
    ap.add_argument("--fairness-min-support", type=int, default=100)
    ap.add_argument("--freshness-window-days", type=int, default=30)
    ap.add_argument("--long-tail-percentile", type=float, default=0.80)
    ap.add_argument("--safety-policy-version", default="catalog-filter-v1")
    args = ap.parse_args(argv)
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))

    data = build(args.input, host, port, args.mdp_csv, args.experiences, args.live_metrics, {
        "fairness_min_support": args.fairness_min_support,
        "freshness_window_days": args.freshness_window_days,
        "long_tail_percentile": args.long_tail_percentile,
        "safety_policy_version": args.safety_policy_version,
    })
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as fh:
        json.dump(data, fh, indent=2, default=_json_default, allow_nan=False)
    print(f"wrote {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
