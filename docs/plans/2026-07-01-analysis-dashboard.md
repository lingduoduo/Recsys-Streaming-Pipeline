# Analysis Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `analysis_dashboard_report.py`, a standalone script that recomputes the keyword / query / relevance / recall / ranking analyses for one sim run and renders them as a single self-contained HTML page.

**Architecture:** Plain-Python (pandas + pyarrow + redis-py), no Spark. Pure `compute_*` functions return plain data; pure renderers turn that data into inline-SVG charts + HTML tables; `main()` wires load → compute → render → write. Keyword/query/relevance metrics are reimplemented in pandas; recall/ranking **import** the already-pure functions from `recall_eval_report.py` / `ranking_eval_report.py` (no changes to those files). Genres/categories come from the importable `genre_meta` and `movie_categories` helpers.

**Tech Stack:** Python 3.12, pandas, pyarrow, redis-py, pytest. Inline SVG (no JS/CDN charting lib).

## Global Constraints

- Working dir for all commands: `recsys-pipeline/` (repo subdir).
- New script lives in `services/python-modeling/`; it imports sibling modules by name, so tests add that dir to `sys.path` (see Task 7 pattern), matching existing tests.
- No modification to `recall_eval_report.py`, `ranking_eval_report.py`, `genre_meta.py`, `movie_categories.py`, or the three PySpark reports.
- Redis host/port from env `REDIS_HOST` (default `localhost`) / `REDIS_PORT` (default `6379`), same as the eval reports.
- Output path default: `<input>/../report-dashboard/index.html`.
- `--input` default: `/tmp/spark-recsys/movie-category-sim/training-samples` (match the other reports).
- Engagement label: `0.0/1.0/2.0` = impression / click / order. `clicked` = `label >= 1`.
- **query** = `" ".join(genres)` (`"unknown"` if no genres). Short = `len <= 10`, long = `> 10`.
- All code targets `from __future__ import annotations` at module top (repo style).
- Commit after each task with the shown message.

---

### Task 1: Scaffold + `load_samples` (data normalization)

**Files:**
- Create: `services/python-modeling/analysis_dashboard_report.py`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Produces: `load_samples(input_dir: str, host: str = "localhost", port: int = 6379) -> pandas.DataFrame` — returns a DataFrame guaranteed to have columns `user_id, session_id, item_id, label (float), clicked (int), genres (list[str])`. `query_of(genres: list[str]) -> str`.

- [ ] **Step 1: Write the failing test**

```python
# integration-tests/python_modeling/test_analysis_dashboard.py
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))


def _df(pd):
    # 3 impressions: item_2 shown twice (1 click), item_1 shown once (clicked+ordered)
    return pd.DataFrame({
        "user_id":    ["u1", "u2", "u1"],
        "session_id": ["s1", "s2", "s1"],
        "item_id":    ["item_2", "item_2", "item_1"],
        "label":      [1.0, 0.0, 2.0],
        "genres":     [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })


def test_load_samples_normalizes_columns(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash

    parquet = tmp_path / "samples"
    _df(pd).to_parquet(parquet, index=False)

    out = dash.load_samples(str(parquet))
    assert list(out["clicked"]) == [1, 0, 1]          # derived from label >= 1
    assert out["genres"].apply(type).eq(list).all()
    assert dash.query_of(["Sci-Fi", "Action"]) == "Sci-Fi Action"
    assert dash.query_of([]) == "unknown"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_load_samples_normalizes_columns -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'analysis_dashboard_report'`.

- [ ] **Step 3: Write minimal implementation**

```python
# services/python-modeling/analysis_dashboard_report.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_load_samples_normalizes_columns -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): scaffold analysis_dashboard_report with load_samples"
```

---

### Task 2: `compute_relevance` (funnel + mean score)

**Files:**
- Modify: `services/python-modeling/analysis_dashboard_report.py`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `load_samples`, `query_of` (Task 1).
- Produces: `compute_relevance(df) -> dict` with keys `headline: str`, `ctr: float`, `cvr: float`, `funnel: dict[str,int]` (`impression`/`click`/`order`), `by_query: DataFrame` (`query, impressions, mean_score`), `by_genre: DataFrame` (`genre, impressions, mean_score`), both sorted by `mean_score` desc then `impressions` desc.

- [ ] **Step 1: Write the failing test**

```python
def test_compute_relevance_funnel_and_means(tmp_path):
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    frame = pd.DataFrame({
        "user_id": ["u1", "u2", "u1"], "session_id": ["s1", "s2", "s1"],
        "item_id": ["item_2", "item_2", "item_1"], "label": [1.0, 0.0, 2.0],
        "clicked": [1, 0, 1], "genres": [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })
    r = dash.compute_relevance(frame)
    assert r["funnel"] == {"impression": 3, "click": 2, "order": 1}
    assert r["ctr"] == round(2 / 3, 4) and r["cvr"] == round(1 / 3, 4)
    bq = {row["query"]: row["mean_score"] for _, row in r["by_query"].iterrows()}
    assert bq["Sci-Fi Action"] == 2.0            # single ordered impression
    assert bq["Drama"] == 0.5                     # labels 1.0 and 0.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_relevance_funnel_and_means -v`
Expected: FAIL — `AttributeError: module ... has no attribute 'compute_relevance'`.

- [ ] **Step 3: Write minimal implementation** (append to the module)

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_relevance_funnel_and_means -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): compute_relevance funnel + mean score by query/genre"
```

---

### Task 3: `compute_keyword` (movies-vs-queries + top keywords l1/l2/l3)

**Files:**
- Modify: `services/python-modeling/analysis_dashboard_report.py`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `movie_categories` (`primary_genre`, `secondary_genre`, `l1`, `l2`, `l3`).
- Produces: `compute_keyword(df) -> dict` with keys `headline: str`, `by_keyword: DataFrame` (`keyword, movie_impressions, query_clicks, movie_share, query_share, divergence`), `by_subkeyword: DataFrame` (same shape, `subkeyword`), `tops: dict[str, DataFrame]` keyed `"l1"/"l2"/"l3"` (`<level>, keyword, movie_impressions, query_clicks, rank`).

- [ ] **Step 1: Write the failing test**

```python
def test_compute_keyword_distribution_and_divergence():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    frame = pd.DataFrame({
        "user_id": ["u1", "u2", "u1"], "session_id": ["s1", "s2", "s1"],
        "item_id": ["item_2", "item_2", "item_1"], "label": [1.0, 0.0, 2.0],
        "clicked": [1, 0, 1], "genres": [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })
    r = dash.compute_keyword(frame)
    bk = {row["keyword"]: row for _, row in r["by_keyword"].iterrows()}
    assert bk["Drama"]["movie_impressions"] == 2     # two Drama impressions
    assert bk["Drama"]["query_clicks"] == 1          # one Drama click
    assert bk["Sci-Fi"]["query_clicks"] == 1         # item_1 clicked
    assert set(r["tops"]) == {"l1", "l2", "l3"}
    assert (r["tops"]["l2"]["rank"] >= 1).all()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_keyword_distribution_and_divergence -v`
Expected: FAIL — no attribute `compute_keyword`.

- [ ] **Step 3: Write minimal implementation** (append)

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_keyword_distribution_and_divergence -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): compute_keyword movies-vs-queries + l1/l2/l3 top keywords"
```

---

### Task 4: `compute_query` (top queries + short vs long)

**Files:**
- Modify: `services/python-modeling/analysis_dashboard_report.py`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `query_of` (Task 1).
- Produces: `compute_query(df) -> dict` with keys `headline: str`, `top_queries: DataFrame` (`query, impressions, clicks, orders, query_len, ctr, cvr`, sorted by impressions desc), `by_length: DataFrame` (`bucket, impressions, clicks, orders, ctr, cvr`).

- [ ] **Step 1: Write the failing test**

```python
def test_compute_query_top_and_length_buckets():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    frame = pd.DataFrame({
        "user_id": ["u1", "u2", "u1"], "session_id": ["s1", "s2", "s1"],
        "item_id": ["item_2", "item_2", "item_1"], "label": [1.0, 0.0, 2.0],
        "clicked": [1, 0, 1], "genres": [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })
    r = dash.compute_query(frame)
    tq = {row["query"]: row for _, row in r["top_queries"].iterrows()}
    assert tq["Drama"]["impressions"] == 2 and tq["Drama"]["ctr"] == 0.5
    assert tq["Sci-Fi Action"]["query_len"] == 13
    buckets = {row["bucket"]: row for _, row in r["by_length"].iterrows()}
    assert buckets["short (<=10)"]["impressions"] == 2   # "Drama" is 5 chars
    assert buckets["long (>10)"]["impressions"] == 1     # "Sci-Fi Action" is 13 chars
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_query_top_and_length_buckets -v`
Expected: FAIL — no attribute `compute_query`.

- [ ] **Step 3: Write minimal implementation** (append)

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_query_top_and_length_buckets -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): compute_query top queries + short/long buckets"
```

---

### Task 5: `compute_recall` + `compute_ranking` (import the eval functions)

**Files:**
- Modify: `services/python-modeling/analysis_dashboard_report.py`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `recall_eval_report.evaluate`, `recall_eval_report.fetch_corpus_and_vecs`; `ranking_eval_report.evaluate_signal`, `ranking_eval_report.fetch_popularity`, `ranking_eval_report.fetch_embeddings`, `ranking_eval_report.SIGNALS`, `ranking_eval_report._dot`.
- Produces:
  - `compute_recall(df, host, port, ks=(5, 10, 20)) -> dict | None` — `None` if Redis corpus empty; else `{headline, rows}` where `rows` is `recall_eval_report.evaluate`'s list of dicts.
  - `compute_ranking(df, host, port) -> dict | None` — `None` if neither popularity nor embeddings present; else `{headline, rows}` (`signal, coverage, n, positives, auc, logloss`).

- [ ] **Step 1: Write the failing test** (Redis-free path → both return `None`)

```python
def test_recall_ranking_return_none_without_redis():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    frame = pd.DataFrame({
        "user_id": ["u1", "u1"], "session_id": ["s1", "s1"],
        "item_id": ["item_1", "item_2"], "label": [2.0, 1.0],
        "clicked": [1, 1], "genres": [["Drama"], ["Comedy"]],
    })
    # port 6399: no Redis listening → empty corpus / no signals → None
    assert dash.compute_recall(frame, "localhost", 6399) is None
    assert dash.compute_ranking(frame, "localhost", 6399) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_recall_ranking_return_none_without_redis -v`
Expected: FAIL — no attribute `compute_recall`.

- [ ] **Step 3: Write minimal implementation** (append)

```python
def compute_recall(df, host: str, port: int, ks=(5, 10, 20)):
    from recall_eval_report import evaluate, fetch_corpus_and_vecs
    corpus, vecs = fetch_corpus_and_vecs(host, port)
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_recall_ranking_return_none_without_redis -v`
Expected: PASS. (`fetch_popularity`/`fetch_embeddings` swallow the connection error and return `{}`, so both computes return `None`.)

- [ ] **Step 5: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): compute_recall + compute_ranking wrapping the eval reports"
```

---

### Task 6: Renderers (inline SVG + HTML)

**Files:**
- Modify: `services/python-modeling/analysis_dashboard_report.py`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Produces (all pure, return `str`):
  - `svg_bar(labels: list[str], values: list[float], title: str = "") -> str`
  - `svg_line(xs: list[int], series: dict[str, list[float]], title: str = "") -> str`
  - `html_table(df, columns: list[str] | None = None) -> str`
  - `section(title: str, headline: str, body_html: str) -> str`
  - `na_card(title: str, reason: str) -> str`
  - `render_html(title: str, sections: list[str]) -> str`

- [ ] **Step 1: Write the failing test**

```python
def test_renderers_emit_svg_and_tables():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    bar = dash.svg_bar(["A", "B"], [1.0, 3.0], title="t")
    assert "<svg" in bar and "<rect" in bar and "<title>" in bar
    line = dash.svg_line([5, 10], {"bm25": [0.1, 0.2]}, title="recall")
    assert "<svg" in line and "<polyline" in line
    tbl = dash.html_table(pd.DataFrame({"k": [1], "v": [2]}))
    assert "<table" in tbl and "<th>k</th>" in tbl and "<td>2</td>" in tbl
    page = dash.render_html("Dashboard", [dash.section("S", "head", "body"),
                                          dash.na_card("Recall", "no corpus")])
    assert "<html" in page and "Dashboard" in page and "no corpus" in page
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_renderers_emit_svg_and_tables -v`
Expected: FAIL — no attribute `svg_bar`.

- [ ] **Step 3: Write minimal implementation** (append)

```python
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
            f'<rect x="150" y="{y}" width="{w}" height="{bar_h}" fill="#4c78a8"/>'
            f'<text x="{155 + w}" y="{y + bar_h - 6}" font-size="11">{_esc(round(v, 4))}</text></g>')
    h = max(len(labels), 1) * (bar_h + gap)
    cap = f'<text x="0" y="-6" font-size="13" font-weight="bold">{_esc(title)}</text>' if title else ""
    return (f'<svg viewBox="0 -20 {width} {h + 24}" width="{width}" '
            f'font-family="sans-serif">{cap}{"".join(rows)}</svg>')


def svg_line(xs, series, title="", width=520, height=220) -> str:
    colors = ["#4c78a8", "#f58518", "#54a24b", "#e45756"]
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
        lines.append(f'<polyline fill="none" stroke="{c}" stroke-width="2" points="{pts}">'
                     f'<title>{_esc(name)}</title></polyline>')
        legend.append(f'<text x="{60 + idx * 110}" y="16" fill="{c}" font-size="12">{_esc(name)}</text>')
    cap = f'<text x="0" y="16" font-size="13" font-weight="bold">{_esc(title)}</text>' if title else ""
    xlab = "".join(f'<text x="{px(x):.1f}" y="{height - 10}" font-size="10" '
                   f'text-anchor="middle">{_esc(x)}</text>' for x in xs)
    return (f'<svg viewBox="0 0 {width} {height}" width="{width}" font-family="sans-serif">'
            f'{cap}{"".join(legend)}{"".join(lines)}{xlab}</svg>')


def html_table(df, columns=None) -> str:
    cols = columns or list(df.columns)
    head = "".join(f"<th>{_esc(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{_esc(r[c])}</td>" for c in cols) + "</tr>"
                   for _, r in df.iterrows())
    return f'<table class="rpt"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'


def section(title, headline, body_html) -> str:
    return (f'<section><h2>{_esc(title)}</h2>'
            f'<p class="headline">{_esc(headline)}</p>{body_html}</section>')


def na_card(title, reason) -> str:
    return f'<section><h2>{_esc(title)}</h2><p class="na">N/A — {_esc(reason)}</p></section>'


def render_html(title, sections) -> str:
    style = ("body{font-family:sans-serif;margin:2rem;max-width:900px}"
             "h2{border-bottom:2px solid #4c78a8;padding-bottom:4px}"
             ".headline{font-size:1.1rem;font-weight:bold;color:#333}"
             ".na{color:#999;font-style:italic}"
             "table.rpt{border-collapse:collapse;margin:8px 0}"
             "table.rpt th,table.rpt td{border:1px solid #ddd;padding:4px 8px;font-size:13px}"
             "table.rpt th{background:#f4f4f4;text-align:left}")
    return (f"<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{_esc(title)}</title><style>{style}</style></head>"
            f"<body><h1>{_esc(title)}</h1>{''.join(sections)}</body></html>")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_renderers_emit_svg_and_tables -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): inline-SVG + HTML renderers"
```

---

### Task 7: `main()` — wire everything, write `index.html`

**Files:**
- Modify: `services/python-modeling/analysis_dashboard_report.py`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: every `compute_*` and renderer above.
- Produces: `main(argv=None) -> str` returning the output HTML path; writes `<outdir>/index.html` (`outdir` default `<input>/../report-dashboard`). Sections in order: funnel, keyword, query, recall, ranking. Recall/ranking → `na_card(...)` when their compute returns `None`. Exits non-zero (`raise SystemExit`) on missing/empty Parquet.

- [ ] **Step 1: Write the failing test** (end-to-end via subprocess, no Redis)

```python
import subprocess


def test_main_writes_html_with_sections_and_na_cards(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id": ["u1", "u2", "u1"], "session_id": ["s1", "s2", "s1"],
        "item_id": ["item_2", "item_2", "item_1"], "label": [1.0, 0.0, 2.0],
        "clicked": [1, 0, 1], "genres": [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    }).to_parquet(parquet, index=False)

    out = tmp_path / "report-dashboard"
    script = Path(__file__).parents[2] / "services/python-modeling/analysis_dashboard_report.py"
    subprocess.run([sys.executable, str(script), "--input", str(parquet), "--outdir", str(out)],
                   check=True, capture_output=True, timeout=120,
                   env={**os.environ, "REDIS_PORT": "6399"})

    page = (out / "index.html").read_text()
    assert "Engagement funnel" in page and "Keyword gap" in page and "Query intent" in page
    assert "N/A — no movie:*:features in Redis" in page   # recall + ranking, Redis unreachable
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_main_writes_html_with_sections_and_na_cards -v`
Expected: FAIL — `main` missing / no `index.html`.

- [ ] **Step 3: Write minimal implementation** (append; this completes the module)

```python
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
                    else na_card("Ranking", "no movie:*:features in Redis"))

    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "index.html")
    with open(out, "w") as fh:
        fh.write(render_html("Analysis Dashboard", sections))
    print(f"wrote {out}")
    return out


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_main_writes_html_with_sections_and_na_cards -v`
Expected: PASS.

- [ ] **Step 5: Run the full new test file + a quick lint sanity**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -v`
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): main() wires sections and writes index.html"
```

---

### Task 8: Documentation — sub-README Analysis Reports + Tests

**Files:**
- Modify: `recsys-pipeline/README.md` (Analysis Reports table + run block ~lines 1180-1203; Tests table ~line 1213)

**Interfaces:** none (docs only).

- [ ] **Step 1: Add the dashboard row to the Analysis Reports table**

In `recsys-pipeline/README.md`, after the `Ranking Performance` row (~line 1186), add:

```markdown
| **Consolidated Dashboard** | `analysis_dashboard_report.py` | All five analyses as one self-contained HTML page (funnel → keyword → query → recall → ranking) | `report-dashboard/index.html` |
```

- [ ] **Step 2: Add the run command to the bash block**

After the ranking-eval line (~line 1202), add:

```bash
# Consolidated HTML dashboard (plain python; recall/ranking sections need Redis corpus)
REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py --input "$IN"
```

- [ ] **Step 3: Update the Python Tests row**

Change the Python row's "Covers" cell (~line 1213) to append `, analysis-dashboard`:

```markdown
| Python | `cd recsys-pipeline && pytest -q` | Producers, MovieLens pipeline, replay export, the simulation harnesses, `session_report`, and the analysis reports (keyword / query / relevance / recall-eval / ranking-eval / analysis-dashboard) |
```

- [ ] **Step 4: Verify the whole suite runs**

Run: `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/README.md
git commit -m "docs(dashboard): document analysis_dashboard_report in sub-README"
```

---

## Self-Review

**Spec coverage:**
- Script/I-O contract → Task 1 (`load_samples`, `--input`, output path) + Task 7 (`--outdir`, `index.html`). ✓
- Internal structure (compute/render/main split) → Tasks 2-7. ✓
- Five report sections + funnel narrative order → Task 7 (`sections` list order). ✓
- Money visual per report → svg_bar/svg_line in Tasks 6-7. ✓
- Recall/ranking import pure functions → Task 5. ✓
- Error handling: missing/empty Parquet (Task 7 `SystemExit`), no Redis corpus → N/A card (Tasks 5+7), empty group / no div-by-zero (guards in Tasks 2-4). ✓
- Testing: unit computes (Tasks 2-5), renderers (Task 6), integration (Task 7), README Tests wiring (Task 8). ✓
- Definitions header note → covered by section headlines; a page-level definitions note is optional and folded into headlines (no separate task needed).

**Placeholder scan:** No TBD/TODO; every code + test step has full content. ✓

**Type consistency:** `compute_*` return dicts with the keys consumed by the matching `_*_section` renderer in Task 7; `evaluate` row keys (`method,k,recall_at_k,hitrate_at_k,users_evaluated`) match `_recall_section`; `evaluate_signal` row keys (`signal,coverage,n,positives,auc,logloss`) match `_ranking_section`. `svg_bar(labels, values, title)` / `svg_line(xs, series, title)` / `html_table(df, columns)` signatures are consistent across Tasks 6-7. ✓

**Known minor deviation from spec:** the spec's "definitions block in a header note" is delivered via per-section headlines rather than a standalone note — acceptable, noted above.
