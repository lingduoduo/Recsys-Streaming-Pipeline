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


def build(input_dir: str, host: str, port: int, mdp_csv: str | None) -> dict:
    df = dash.load_samples(input_dir, host, port)

    rel = dash.compute_relevance(df)
    relevance = {
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

    return {
        "input": input_dir,
        "rows": int(len(df)),
        "relevance": relevance,
        "keyword": keyword,
        "query": query,
        "recall": recall,  # {headline, rows} or None
        "ranking": {"headline": ranking["headline"], "rows": ranking["rows"]} if ranking else None,
        "ope": ope,        # {headline, rows, calibration} or None
        "mdp": {"headline": mdp["headline"], "rows": _records(mdp["df"])} if mdp else None,
    }


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/training-samples")
    ap.add_argument("--output", default=str(_REPO / "frontend" / "data" / "dashboard.json"))
    ap.add_argument("--mdp-csv", default=None)
    args = ap.parse_args(argv)
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))

    data = build(args.input, host, port, args.mdp_csv)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as fh:
        json.dump(data, fh, indent=2, default=_json_default)
    print(f"wrote {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
