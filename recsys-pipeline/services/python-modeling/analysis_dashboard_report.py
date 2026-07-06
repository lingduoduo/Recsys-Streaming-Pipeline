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
    if not pop and not iemb:
        return None

    items = df["item_id"].astype(str).tolist()
    users = df["user_id"].astype(str).tolist() if "user_id" in df.columns else [None] * len(items)
    positions = df["position"].tolist() if "position" in df.columns else [0] * len(items)
    labels_all = (df["label"] >= 1).astype(int).tolist()

    signal_scores = {
        "popularity": [(float(pop.get(it, 0.0)), True) for it in items],
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


def main(argv=None) -> str:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/spark-recsys/movie-category-sim/training-samples")
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--ks", default="5,10,20")
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

    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "index.html")
    with open(out, "w") as fh:
        fh.write(render_html("Analysis Dashboard", sections))
    print(f"wrote {out}")
    return out


if __name__ == "__main__":
    main()
