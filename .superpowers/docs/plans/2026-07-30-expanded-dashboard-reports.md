# Expanded Dashboard Reports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge the sampled expanded dashboard UI into the existing report structure, computing the fields it needs for real instead of fabricating them.

**Architecture:** `export_dashboard_json.py` and `analysis_dashboard_report.py` gain per-query, per-genre and per-keyword click/order/rate columns derived from the `label` column the dataframe already carries. The frontend collapses its two duplicated component sets into one — a single chart engine, a single table — and every section gains a KPI card grid and, where an honest second view exists, a two-up chart layout. The one interactive piece (the Top-K keyword report) moves to its own client component so the rest of the page still prerenders.

**Tech Stack:** Python 3 / pandas (exporter), Next.js 15 app-router / React 19 (frontend), plain CSS custom properties (no chart library).

## Global Constraints

- **Never fabricate a displayed value.** A missing number serializes as `null` in Python and renders `"N/A"` in the UI. Never `?? 0` on a value that reaches the screen. A chart bar for a null value is omitted, not drawn at zero width.
- **`schemaVersion` stays `"2.0"`.** It is emitted from `MEASUREMENT_SCHEMA_VERSION` in `analysis_dashboard_report.py:286` and versions the seven measurement sections, which this plan does not change. `test_dashboard_measurement_contract.py` asserts `"2.0"` at lines 70, 144 and 208.
- **`cvr` is `orders / impressions`**, per `_rates` at `analysis_dashboard_report.py:157-160` — not `orders / clicks`. The UI renders the `ctr` and `cvr` the exporter publishes and never recomputes them client-side.
- **The seven measurement sections keep their semantics.** `HEADLINES`, `maxByField`, `headlineFieldPublished`, `LOW_COVERAGE`, every warning path and every fine-print caveat paragraph survive verbatim. Only presentation changes.
- **CSS names that already exist keep their current meaning.** `.metric-value` and `.metric-label` belong to `MetricTile`; new card styles are scoped under `.metric-card`. `.report-grid` is the page-level wrapper in `page.jsx`; the new two-up chart layout is `.chart-grid`.
- **Chart series colors** are `--series-1: #4f46e5`, `--series-2: #eb6834`, `--series-3: #1baf7a`, validated as a categorical palette against the light surface (all-pairs CVD ΔE 9.2, normal-vision 27.6). `#1baf7a` sits at 2.74:1 contrast, below the 3:1 gate; the relief is that every bar carries a visible value label and every chart is accompanied by its data table. Do not add a fourth series color — fold to "Other" or facet instead.
- **Light mode only.** The existing dashboard defines no dark theme; do not add one.
- **Reference for the sampled code:** `git show 947739a:frontend/components/sections.jsx` and `git show 947739a:frontend/components/ui.jsx`.
- **Branch:** `feature/expanded-dashboard-reports`. Never commit to `master`; the work lands via pull request in Task 13.
- **Test commands:** Python — `cd recsys-pipeline && python -m pytest integration-tests/python_modeling/<file> -v`. Frontend — `cd frontend && npm run build`.

---

## File Structure

**Modified:**
- `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py` — `compute_relevance`, `compute_keyword`, `compute_query`, `compute_ranking` gain columns
- `frontend/export_dashboard_json.py` — export depth and the new `average_query_length` key
- `frontend/validate_measurements.mjs` — asserts the new diagnostic fields
- `frontend/components/ui.jsx` — deduplicated to one primitive set
- `frontend/components/sections.jsx` — deduplicated; all sections adopt cards and chart grids
- `frontend/app/page.jsx` — imports `KeywordSection` from its new module
- `frontend/app/globals.css` — new card, chart, table and heatmap rules
- `frontend/data/dashboard.json` — regenerated
- `frontend/README.md` — layout section
- `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py` — new field tests

**Created:**
- `frontend/components/keyword-report.jsx` — the `"use client"` Top-K keyword report

---

## Task 1: Restore a compiling base

The sampled code is preserved in commit `947739a`. The working tree must return to a state that builds before any feature work begins, so every later task has `npm run build` as a real gate.

**Files:**
- Modify: `frontend/components/sections.jsx` (revert to `master`)
- Modify: `frontend/components/ui.jsx` (revert to `master`)

**Interfaces:**
- Consumes: nothing
- Produces: a `frontend/` that builds, with `ui.jsx` exporting `Section`, `NaCard`, `BarChart`, `GroupedBarChart`, `DataTable`, `MetricTile` exactly once each

- [ ] **Step 1: Confirm the build currently fails**

```bash
cd frontend && npx next build 2>&1 | tail -5
```

Expected: `Build failed because of webpack errors`, citing the mid-file `"use client"` at `components/sections.jsx:510`.

- [ ] **Step 2: Revert both component files to master**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout master -- frontend/components/sections.jsx frontend/components/ui.jsx
```

- [ ] **Step 3: Verify the build passes**

```bash
cd frontend && npm run build 2>&1 | tail -8
```

Expected: `dashboard.json valid: 7 measurement sections, schema 2.0`, then a successful compile with a route listing for `/`.

- [ ] **Step 4: Commit**

```bash
git add frontend/components/sections.jsx frontend/components/ui.jsx
git commit -m "revert: restore the building dashboard components

The sampled UI is preserved in 947739a and is merged in piece by piece
from here, so that every step has a passing build as its gate."
```

---

## Task 2: Align the click predicate across sections

`compute_keyword` counts a click as `clicked == 1`; `compute_query` uses `label >= 1`. `load_samples` only derives `clicked` when the column is absent, so on real data the two can disagree — and a keyword CTR built from one predicate would sit on the same page as a query CTR built from the other.

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py:116` and `:137-139`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `dash.compute_keyword(df)`, `dash.compute_query(df)`
- Produces: `compute_keyword` counts clicks as `label >= 1`, matching `compute_query`

- [ ] **Step 1: Measure the divergence on the real run data**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline && python3 -c "
import pandas as pd
df = pd.read_parquet('/tmp/spark-recsys/movie-category-sim/training-samples')
print('rows        ', len(df))
print('has clicked ', 'clicked' in df.columns)
if 'clicked' in df.columns:
    print('clicked==1  ', int((df[\"clicked\"] == 1).sum()))
    print('label>=1    ', int((df[\"label\"] >= 1).sum()))
    print('disagreeing ', int(((df[\"label\"] >= 1) != (df[\"clicked\"] == 1)).sum()))
"
```

Record the `disagreeing` count. If it is 0, this change moves no published number. If it is non-zero, `keyword.by_keyword.query_clicks` will shift, and Step 5's commit message must state by how much.

- [ ] **Step 2: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`:

```python
def test_keyword_clicks_use_the_same_predicate_as_query_clicks():
    """An order logged without `clicked` still counts as a click everywhere.

    Keyword CTR and query CTR appear on the same page, so they must count the
    same events; `clicked` is an upstream column that may not track `label`.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    frame = pd.DataFrame({
        "user_id": ["u1", "u2"],
        "item_id": ["item_1", "item_2"],
        "label": [2.0, 0.0],
        "clicked": [0, 0],
        "genres": [["Drama"], ["Drama"]],
    })

    keyword_clicks = int(dash.compute_keyword(frame)["by_keyword"]["query_clicks"].sum())
    query_clicks = int(dash.compute_query(frame)["top_queries"]["clicks"].sum())

    assert keyword_clicks == query_clicks == 1
```

- [ ] **Step 3: Run the test to verify it fails**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_keyword_clicks_use_the_same_predicate_as_query_clicks -v
```

Expected: FAIL — `assert 0 == 1`, because `dist()` filters on `clicked == 1`.

- [ ] **Step 4: Change both keyword click counts to the label predicate**

In `compute_keyword`, replace line 116:

```python
        query = d[d["label"] >= 1].groupby(col).size().rename("query_clicks")
```

and replace the body of `top_keywords` at lines 137-140:

```python
    def top_keywords(level):
        ex = lv[[level, "genres", "label"]].explode("genres").dropna(subset=["genres"])
        ex = ex.assign(clk=(ex["label"] >= 1).astype(int))
        g = (ex.groupby([level, "genres"])
               .agg(movie_impressions=("clk", "size"), query_clicks=("clk", "sum"))
               .reset_index().rename(columns={"genres": "keyword"}))
```

`compute_recall` also filters on `clicked == 1`; leave it alone. Its predicate selects relevant items for a retrieval metric, not clicks for a rate, and changing it would move recall numbers for no reason connected to this work.

- [ ] **Step 5: Run the full Python test file**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -v
```

Expected: PASS — 22 tests.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "fix: count keyword clicks with the same predicate as query clicks

compute_keyword filtered on the upstream `clicked` column while compute_query
used `label >= 1`. Both rates render on one page, so they now count the same
events. On the current run data this changes query_clicks by <N> rows."
```

Replace `<N>` with the `disagreeing` count from Step 1.

---

## Task 3: Publish clicks, orders, CTR and CVR per query and genre

The engagement tables show impressions and mean score only, so the sampled UI's CTR and CVR columns had nothing to read.

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py:83-104` (`compute_relevance`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `dash.compute_relevance(df)`
- Produces: `by_query` rows carry `query, impressions, clicks, orders, ctr, cvr, mean_score`; `by_genre` rows carry `genre, impressions, clicks, orders, ctr, cvr, mean_score`

- [ ] **Step 1: Write the failing test**

Append to `test_analysis_dashboard.py`:

```python
def test_compute_relevance_publishes_clicks_orders_and_rates():
    """by_query and by_genre carry the rates the engagement tables display."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    result = dash.compute_relevance(_df(pd))

    by_query = {row["query"]: row for _, row in result["by_query"].iterrows()}
    drama = by_query["Drama"]
    assert (drama["impressions"], drama["clicks"], drama["orders"]) == (2, 1, 0)
    assert (drama["ctr"], drama["cvr"]) == (0.5, 0.0)

    scifi = by_query["Sci-Fi Action"]
    assert (scifi["impressions"], scifi["clicks"], scifi["orders"]) == (1, 1, 1)
    assert (scifi["ctr"], scifi["cvr"]) == (1.0, 1.0)

    by_genre = {row["genre"]: row for _, row in result["by_genre"].iterrows()}
    assert (by_genre["Drama"]["clicks"], by_genre["Drama"]["orders"]) == (1, 0)
    assert (by_genre["Action"]["clicks"], by_genre["Action"]["orders"]) == (1, 1)
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_relevance_publishes_clicks_orders_and_rates -v
```

Expected: FAIL — `KeyError: 'clicks'`.

- [ ] **Step 3: Add the columns**

Replace `compute_relevance` lines 89-98 with:

```python
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
```

`_rates` is defined below `compute_relevance` in the module; that is fine, it resolves at call time. Using it here rather than inlining the division keeps one definition of `cvr` in the file.

- [ ] **Step 4: Run the full Python test file**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -v
```

Expected: PASS — 23 tests, including the pre-existing `test_compute_relevance_funnel_and_means`.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat: publish clicks, orders, CTR and CVR per query and genre"
```

---

## Task 4: Publish keyword relevance and rates, and export enough rows for Top-K

`by_keyword` has no score column, so the sampled Top-K selector sorted on `undefined` and the heatmap coloured every chip identically. Ten exported rows also make a 10/20/30/50 selector inert.

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py:107-151` (`compute_keyword`)
- Modify: `frontend/export_dashboard_json.py:89-95`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `dash.compute_keyword(df)`
- Produces: `by_keyword` and `by_subkeyword` rows carry `movie_impressions, query_clicks, query_orders, mean_score, movie_share, query_share, divergence, ctr, cvr`; `tops[level]` rows additionally carry `ctr`; the exporter emits up to 50 keyword rows

- [ ] **Step 1: Write the failing test**

Append to `test_analysis_dashboard.py`:

```python
def test_compute_keyword_publishes_relevance_and_rates():
    """The heatmap colours by mean_score and the Top-K selector sorts by it."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    result = dash.compute_keyword(_df(pd))
    rows = {row["keyword"]: row for _, row in result["by_keyword"].iterrows()}

    drama = rows["Drama"]
    assert (drama["movie_impressions"], drama["query_clicks"], drama["query_orders"]) == (2, 1, 0)
    assert drama["mean_score"] == 0.5
    assert (drama["ctr"], drama["cvr"]) == (0.5, 0.0)

    for row in result["by_keyword"].to_dict(orient="records"):
        assert row["mean_score"] is not None

    for level in ("l1", "l2", "l3"):
        assert "ctr" in result["tops"][level].columns
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_keyword_publishes_relevance_and_rates -v
```

Expected: FAIL — `KeyError: 'query_orders'`.

- [ ] **Step 3: Rewrite `dist` and add `ctr` to the level tables**

Replace `dist` (lines 114-124) with a single grouped aggregation. The previous two-groupby-and-`fillna` shape existed only because clicks were counted on a filtered frame; aggregating a 0/1 column removes the need to fill anything.

```python
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
```

Add the rate to `top_keywords`, immediately before the `rank` assignment:

```python
        g["ctr"] = (g["query_clicks"] / g["movie_impressions"]).round(4)
```

`pd.concat` was the only use of pandas in this function, so remove the now-orphaned `import pandas as pd` at line 109. Leave the `import feature_derivations as mc` above it.

- [ ] **Step 4: Run the full Python test file**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -v
```

Expected: PASS — 24 tests.

- [ ] **Step 5: Raise the keyword export depth**

In `frontend/export_dashboard_json.py`, replace lines 89-95:

```python
    kw = dash.compute_keyword(df)
    # The Top-K keyword report selects from this pool, so it needs more than a
    # top-ten: 50 rows keeps the snapshot small while making the selector real.
    keyword = {
        "headline": kw["headline"],
        "by_keyword": _records(kw["by_keyword"].head(50)),
        "by_subkeyword": _records(kw["by_subkeyword"].head(50)),
        "tops": {lvl: _records(kw["tops"][lvl].head(10)) for lvl in ("l1", "l2", "l3")},
    }
```

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py \
        frontend/export_dashboard_json.py
git commit -m "feat: publish keyword relevance, rates and a Top-K sized pool"
```

---

## Task 5: Publish average query length and per-bucket query counts

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py:163-185` (`compute_query`)
- Modify: `frontend/export_dashboard_json.py:97-102`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `dash.compute_query(df)`
- Produces: the returned dict carries `average_query_length` (float, or `None` for an empty frame); `by_length` rows carry `queries`

- [ ] **Step 1: Write the failing test**

Append to `test_analysis_dashboard.py`:

```python
def test_compute_query_publishes_average_length_and_bucket_query_counts():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    result = dash.compute_query(_df(pd))

    # "Drama" (5 chars) twice, "Sci-Fi Action" (13 chars) once.
    assert result["average_query_length"] == round((5 + 5 + 13) / 3, 2)

    buckets = {row["bucket"]: row for _, row in result["by_length"].iterrows()}
    assert buckets["short (<=10)"]["queries"] == 1
    assert buckets["long (>10)"]["queries"] == 1
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_query_publishes_average_length_and_bucket_query_counts -v
```

Expected: FAIL — `KeyError: 'average_query_length'`.

- [ ] **Step 3: Add the count and the mean**

In `compute_query`, add `queries` to the bucket aggregation (lines 177-179):

```python
    bylen = (d.groupby("bucket")
               .agg(impressions=("label", "size"), clicks=("clk", "sum"),
                    orders=("ord", "sum"), queries=("query", "nunique"))
               .reset_index())
```

and extend the return (line 185):

```python
    return {"headline": headline, "top_queries": top, "by_length": bylen,
            "average_query_length": round(float(d["query_len"].mean()), 2) if len(d) else None}
```

- [ ] **Step 4: Run the full Python test file**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -v
```

Expected: PASS — 25 tests.

- [ ] **Step 5: Export the new key**

In `frontend/export_dashboard_json.py`, replace lines 97-102:

```python
    qy = dash.compute_query(df)
    query = {
        "headline": qy["headline"],
        "average_query_length": qy["average_query_length"],
        "top_queries": _records(qy["top_queries"].head(10)),
        "by_length": _records(qy["by_length"]),
    }
```

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py \
        frontend/export_dashboard_json.py
git commit -m "feat: publish average query length and per-bucket query counts"
```

---

## Task 6: Publish the ranking positive rate

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py:226-236` (`compute_ranking`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `dash.compute_ranking(df, host, port)`
- Produces: every row in `rows` carries `positive_rate` — a float, or `None` when `n` is 0

- [ ] **Step 1: Write the failing test**

Append to `test_analysis_dashboard.py`. This calls the pure row-shaping logic through `compute_ranking` with an unreachable Redis, which yields zero-coverage rows — exactly the case where a fabricated `0.0` rate would be wrong:

```python
def test_compute_ranking_reports_a_null_positive_rate_when_nothing_was_scored():
    """A signal with no scored rows has no positive rate — not a rate of zero."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    frame = _df(pd).assign(position=[0, 1, 0])
    # Port 1 refuses connections, so popularity and embeddings come back empty.
    result = dash.compute_ranking(frame, "localhost", 1)

    rows = {row["signal"]: row for row in result["rows"]}
    assert rows["popularity"]["n"] == 0
    assert rows["popularity"]["positive_rate"] is None

    # `position` is derived from the frame, so it is always scorable.
    assert rows["position"]["positive_rate"] == round(2 / 3, 4)
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_compute_ranking_reports_a_null_positive_rate_when_nothing_was_scored -v
```

Expected: FAIL — `KeyError: 'positive_rate'`.

- [ ] **Step 3: Derive the rate after the signal loop**

In `compute_ranking`, insert immediately after the `for name in SIGNALS:` loop closes and before the `best = max(...)` line (line 236):

```python
    for row in rows:
        row["positive_rate"] = round(row["positives"] / row["n"], 4) if row["n"] else None
```

- [ ] **Step 4: Run the full Python test file**

```bash
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -v
```

Expected: PASS — 26 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat: publish the ranking positive rate, null when nothing scored"
```

---

## Task 7: Gate the new diagnostic fields in the validator

`validate_measurements.mjs` covers the seven measurement sections only. The new fields need the same build-time gate, or a stale snapshot silently renders a page of N/A.

**Files:**
- Modify: `frontend/validate_measurements.mjs`

**Interfaces:**
- Consumes: `frontend/data/dashboard.json`
- Produces: a non-zero exit when a diagnostic section is present but missing an expected field

- [ ] **Step 1: Add the diagnostic contract**

Insert after the measurement loop closes (after line 41), before the `if (problems.length)` block:

```js
// Diagnostic sections serialize as null when their inputs are absent, which is a
// valid state. When one IS published, the fields its report reads must be present:
// a section that renders every column as N/A is a stale snapshot, not a measurement.
const DIAGNOSTIC_ROWS = {
  engagement: {
    by_query: ["query", "impressions", "clicks", "orders", "ctr", "cvr", "mean_score"],
    by_genre: ["genre", "impressions", "clicks", "orders", "ctr", "cvr", "mean_score"],
  },
  keyword: {
    by_keyword: ["keyword", "movie_impressions", "query_clicks", "query_orders",
                 "mean_score", "ctr", "cvr", "divergence"],
    by_subkeyword: ["subkeyword", "movie_impressions", "query_clicks", "query_orders",
                    "mean_score", "ctr", "cvr", "divergence"],
  },
  query: {
    top_queries: ["query", "impressions", "clicks", "orders", "query_len", "ctr", "cvr"],
    by_length: ["bucket", "queries", "impressions", "clicks", "orders", "ctr", "cvr"],
  },
  ranking: { rows: ["signal", "n", "positives", "positive_rate", "coverage", "auc", "logloss"] },
};

for (const [section, tables] of Object.entries(DIAGNOSTIC_ROWS)) {
  const published = data[section];
  if (published === null || published === undefined) continue;
  for (const [table, fields] of Object.entries(tables)) {
    const rows = published[table];
    if (!Array.isArray(rows)) {
      problems.push(`"${section}.${table}" must be an array`);
      continue;
    }
    if (rows.length === 0) continue;
    for (const field of fields) {
      if (!Object.prototype.hasOwnProperty.call(rows[0], field)) {
        problems.push(`"${section}.${table}" rows must carry "${field}"`);
      }
    }
  }
}

if (data.query && !Object.prototype.hasOwnProperty.call(data.query, "average_query_length")) {
  problems.push('"query" must carry average_query_length');
}
```

- [ ] **Step 2: Run the validator against the current, not-yet-regenerated snapshot**

```bash
cd frontend && npm run validate:data
```

Expected: FAIL, listing the missing fields — `"engagement.by_query" rows must carry "clicks"` and similar. This confirms the gate is live before Task 8 regenerates the data.

- [ ] **Step 3: Commit**

```bash
git add frontend/validate_measurements.mjs
git commit -m "test: gate the new diagnostic fields in the snapshot validator"
```

---

## Task 8: Regenerate the snapshot and verify no existing number moved

**Files:**
- Modify: `frontend/data/dashboard.json`

**Interfaces:**
- Consumes: all exporter changes from Tasks 2-6
- Produces: a snapshot that passes `npm run validate:data`

- [ ] **Step 1: Keep a copy of the current snapshot**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
cp frontend/data/dashboard.json /tmp/dashboard-before.json
```

- [ ] **Step 2: Confirm Redis is reachable and regenerate**

```bash
redis-cli -h localhost ping
REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input /tmp/spark-recsys/movie-category-sim/training-samples \
  --output frontend/data/dashboard.json \
  --experiences /tmp/spark-recsys/movie-category-sim/slates \
  --live-metrics /tmp/spark-recsys/movie-category-sim/live-metrics.json
```

Expected: `PONG`, then `wrote frontend/data/dashboard.json (N bytes)`.

- [ ] **Step 3: Diff every pre-existing value against the new snapshot**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline && python3 -c "
import json
before = json.load(open('/tmp/dashboard-before.json'))
after = json.load(open('frontend/data/dashboard.json'))

def walk(b, a, path=''):
    if isinstance(b, dict):
        out = []
        for key, value in b.items():
            if key not in a:
                out.append(f'{path}.{key}: REMOVED')
            else:
                out += walk(value, a[key], f'{path}.{key}')
        return out
    if isinstance(b, list):
        out = []
        if len(b) != len(a):
            out.append(f'{path}: length {len(b)} -> {len(a)}')
        return out + [d for i, (x, y) in enumerate(zip(b, a)) for d in walk(x, y, f'{path}[{i}]')]
    return [] if b == a else [f'{path}: {b!r} -> {a!r}']

drift = walk(before, after)
print('\n'.join(drift) if drift else 'no drift in pre-existing values')
"
```

Expected drift, and nothing else:
- `.keyword.by_keyword: length 10 -> N` and `.keyword.by_subkeyword: length 10 -> N` — the intentional Task 4 depth change.
- `query_clicks`, `query_share` and `divergence` movement in `.keyword.*` **only if** Task 2 Step 1 reported a non-zero `disagreeing` count, and only in proportion to it.

Any other changed value means an unintended behaviour change. Stop, find it, and fix it before committing. Do not commit a snapshot with unexplained drift.

- [ ] **Step 4: Validate and build**

```bash
cd frontend && npm run build 2>&1 | tail -8
```

Expected: `dashboard.json valid: 7 measurement sections, schema 2.0` and a successful compile.

- [ ] **Step 5: Commit**

```bash
git add frontend/data/dashboard.json
git commit -m "feat: regenerate the snapshot with the new diagnostic fields"
```

---

## Task 9: Merge the UI primitives into one set

**Files:**
- Modify: `frontend/components/ui.jsx` (full rewrite)
- Modify: `frontend/app/globals.css` (append)

**Interfaces:**
- Consumes: nothing
- Produces: `ui.jsx` exports `Section({title, headline, description, actions, id, children})`, `NaCard({title, reason, id})`, `MetricTile({title, value, label, sampleSize, status, reason, href})`, `MetricGrid({children})`, `MetricCard({label, value, detail})`, `ChartGrid({children})`, `BarChart({labels, values, title, horizontal, percentage, valueFormatter})`, `GroupedBarChart({labels, series, title, percentage, valueFormatter})`, `DataTable({rows, columns, formatters, compact})`

`Section` keeps the existing `.report-card` / `.section-heading` / `.section-body` class names rather than adopting the sample's `.report-section` / `.section-header` / `.section-content`, because the existing rules already style them and the sample's names would mean rewriting working CSS for no visual gain.

- [ ] **Step 1: Rewrite `frontend/components/ui.jsx`**

```jsx
const round4 = (v) => (typeof v === "number" ? Math.round(v * 1e4) / 1e4 : v);

// A value that is not a finite number is missing, not zero. Charts omit its bar
// and label it N/A rather than drawing a zero-height mark that reads as measured.
const finite = (v) => (Number.isFinite(Number(v)) ? Number(v) : null);

export function Section({ title, headline, description, actions, id, children }) {
  return (
    <section className="report-card" id={id}>
      {/* The flex row is its own element: `.section-heading` is shared with NaCard,
          whose heading is a plain h2 + paragraph that must keep stacking. */}
      <div className="section-heading">
        <div className="section-heading-row">
          <div className="section-heading-main">
            <h2>{title}</h2>
            {headline ? <p className="insight">{headline}</p> : null}
            {description ? <p className="section-description">{description}</p> : null}
          </div>
          {actions ? <div className="section-actions">{actions}</div> : null}
        </div>
      </div>
      <div className="section-body">{children}</div>
    </section>
  );
}

export function NaCard({ title, reason, id }) {
  return (
    <section className="report-card status-card" id={id}>
      <div className="section-heading">
        <h2>{title}</h2>
        <p className="na">N/A — {reason}</p>
      </div>
    </section>
  );
}

export function MetricGrid({ children }) {
  return <div className="metric-grid">{children}</div>;
}

export function MetricCard({ label, value, detail }) {
  return (
    <div className="metric-card">
      <span className="card-label">{label}</span>
      <strong className="card-value">{value ?? "N/A"}</strong>
      {detail ? <span className="card-detail">{detail}</span> : null}
    </div>
  );
}

// Two-up layout for charts that read together. Named for what it lays out, so it
// cannot be confused with `.report-grid`, which is the page's section stack.
export function ChartGrid({ children }) {
  return <div className="chart-grid">{children}</div>;
}

function formatter({ percentage, valueFormatter }) {
  return (v) => {
    if (v === null) return "N/A";
    if (valueFormatter) return valueFormatter(v);
    if (percentage) return `${(v * 100).toFixed(1)}%`;
    return round4(v).toLocaleString();
  };
}

export function BarChart({ labels, values, title, horizontal = false, percentage = false, valueFormatter }) {
  const numeric = labels.map((_, i) => finite(values[i]));
  const observed = numeric.filter((v) => v !== null).map(Math.abs);
  const scale = (observed.length ? Math.max(...observed) : 0) || 1;
  const format = formatter({ percentage, valueFormatter });
  return (
    <div className="chart-card">
      {title ? <h3>{title}</h3> : null}
      <div className={horizontal ? "bar-chart horizontal" : "bar-chart"}>
        {labels.map((label, i) => {
          const v = numeric[i];
          return (
            <div className="bar-row" key={`${label}-${i}`}>
              <span className="bar-label" title={String(label)}>{label}</span>
              <div className="bar-track">
                {v === null ? null : (
                  <div
                    className={v < 0 ? "bar-fill negative" : "bar-fill"}
                    style={{ width: `${Math.max(1, (Math.abs(v) / scale) * 100)}%` }}
                    title={`${label}: ${format(v)}`}
                  />
                )}
              </div>
              <span className="bar-value">{format(v)}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// Two or three series sharing one scale, for comparisons a single series hides
// (ndcg vs mrr, fresh vs established). Series colour is fixed by index, never
// cycled; a fourth series needs a different chart, not a fourth hue.
export function GroupedBarChart({ labels, series, title, percentage = false, valueFormatter }) {
  const observed = series.flatMap((s) => s.values).map(finite).filter((v) => v !== null).map(Math.abs);
  const scale = (observed.length ? Math.max(...observed) : 0) || 1;
  const format = formatter({ percentage, valueFormatter });
  return (
    <div className="chart-card">
      {title ? <h3>{title}</h3> : null}
      <div className="chart-legend">
        {series.map((s, si) => (
          <span className="legend-item" key={s.name}>
            <span className={`legend-swatch series-${si}`} />
            {s.name}
          </span>
        ))}
      </div>
      <div className="bar-chart grouped">
        {labels.map((label, li) => (
          <div className="bar-group" key={`${label}-${li}`}>
            <span className="bar-label" title={String(label)}>{label}</span>
            <div className="bar-group-bars">
              {series.map((s, si) => {
                const v = finite(s.values[li]);
                return (
                  <div className="bar-row" key={s.name}>
                    <div className="bar-track">
                      {v === null ? null : (
                        <div
                          className={`bar-fill series-${si}`}
                          style={{ width: `${Math.max(1, (Math.abs(v) / scale) * 100)}%` }}
                          title={`${s.name} ${label}: ${format(v)}`}
                        />
                      )}
                    </div>
                    <span className="bar-value">{format(v)}</span>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export function DataTable({ rows = [], columns, formatters = {}, compact = false }) {
  const cols = columns || (rows.length ? Object.keys(rows[0]) : []);
  if (!rows.length) return <p className="empty-state">No rows available.</p>;
  return (
    <div className="table-shell">
      <table className={compact ? "rpt compact" : "rpt"}>
        <thead>
          <tr>
            {cols.map((c) => (
              <th key={c}>{c.replaceAll("_", " ")}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => {
                const format = formatters[c];
                const value = format ? format(r[c], r) : formatCell(r[c]);
                return <td key={c}>{value === null || value === undefined || value === "" ? "N/A" : value}</td>;
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(v) {
  if (v === null || v === undefined) return "N/A";
  if (typeof v === "number") return String(round4(v));
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

// One scorecard tile. Status reflects DATA AVAILABILITY only — never whether the
// number is good, because no targets have been set for these measurements.
export function MetricTile({ title, value, label, sampleSize, status, reason, href }) {
  return (
    <a className={`metric-tile status-${status}`} href={href}>
      <span className="metric-title">{title}</span>
      <span className="metric-value">{value}</span>
      <span className="metric-label">{label}</span>
      <span className="metric-support">
        {status === "na" ? reason : `n=${(sampleSize ?? 0).toLocaleString()}`}
      </span>
      {/* Border color alone can't convey the low-coverage flag to screen readers or
          color-vision-deficient users, so state it as text too. */}
      {status === "low" ? <span className="sr-only">Low coverage — at or below 50%</span> : null}
    </a>
  );
}
```

- [ ] **Step 2: Append the primitive styles to `frontend/app/globals.css`**

```css
/* --- Expanded report primitives --------------------------------------- */

/* Series hues are a validated categorical set (all-pairs CVD ΔE 9.2, normal
   vision 27.6 on this surface). series-3 sits at 2.74:1 contrast, under the 3:1
   gate; the relief is that every bar carries a visible value label and every
   chart ships beside its data table. Do not add a fourth. */
:root {
  --series-0: #4f46e5;
  --series-1: #eb6834;
  --series-2: #1baf7a;
}

.section-heading-row {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 20px;
}

.section-heading-main {
  min-width: 0;
}

.section-description {
  margin: 10px 0 0;
  color: var(--muted);
  font-size: 0.86rem;
  max-width: 62ch;
}

.section-actions {
  flex: none;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px;
}

.metric-card {
  display: grid;
  gap: 3px;
  align-content: start;
  padding: 14px 16px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: #fbfcff;
}

.metric-card .card-label {
  font-size: 0.68rem;
  font-weight: 800;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--muted);
}

.metric-card .card-value {
  font-size: 1.35rem;
  font-weight: 800;
  letter-spacing: -0.02em;
  font-variant-numeric: tabular-nums;
}

.metric-card .card-detail {
  font-size: 0.76rem;
  color: var(--muted);
  overflow-wrap: anywhere;
}

.chart-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 20px;
}

.chart-card {
  min-width: 0;
  padding: 16px 18px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: var(--surface);
}

.chart-card h3 {
  margin: 0 0 12px;
  font-size: 0.9rem;
  letter-spacing: -0.01em;
}

.chart-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  margin-bottom: 10px;
  font-size: 0.75rem;
  color: var(--muted);
}

.legend-item {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.legend-swatch {
  width: 10px;
  height: 10px;
  border-radius: 3px;
  background: var(--series-0);
}

.legend-swatch.series-1 { background: var(--series-1); }
.legend-swatch.series-2 { background: var(--series-2); }

.bar-chart {
  display: grid;
  gap: 7px;
}

.bar-row {
  display: grid;
  grid-template-columns: minmax(70px, 150px) 1fr minmax(56px, auto);
  align-items: center;
  gap: 10px;
  font-size: 0.78rem;
}

.bar-label {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--muted);
}

.bar-track {
  height: 14px;
  border-radius: 4px;
  background: #f1f5f9;
}

.bar-fill {
  height: 100%;
  border-radius: 4px;
  background: var(--series-0);
}

.bar-fill.series-1 { background: var(--series-1); }
.bar-fill.series-2 { background: var(--series-2); }
.bar-fill.negative { background: var(--amber); }

.bar-value {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.bar-chart.grouped {
  gap: 14px;
}

.bar-group {
  display: grid;
  grid-template-columns: minmax(70px, 150px) 1fr;
  align-items: center;
  gap: 10px;
}

/* Inside a group the label is carried by the group, so each bar drops its own. */
.bar-group-bars {
  display: grid;
  gap: 3px;
}

.bar-group-bars .bar-row {
  grid-template-columns: 1fr minmax(56px, auto);
}

table.rpt.compact {
  font-size: 0.76rem;
}

table.rpt.compact td,
table.rpt.compact th {
  padding: 7px 9px;
}

.empty-state {
  margin: 0;
  color: var(--muted);
  font-size: 0.82rem;
}

@media (max-width: 700px) {
  .section-heading-row {
    flex-direction: column;
  }
  .bar-row,
  .bar-group {
    grid-template-columns: minmax(60px, 100px) 1fr minmax(48px, auto);
  }
  .bar-group {
    grid-template-columns: minmax(60px, 100px) 1fr;
  }
}
```

- [ ] **Step 3: Verify the build still passes**

The existing `sections.jsx` uses `Section`, `NaCard`, `BarChart`, `GroupedBarChart`, `DataTable` and `MetricTile` with call signatures this rewrite preserves, so the page must still compile and render unchanged apart from the new chart styling.

```bash
cd frontend && npm run build 2>&1 | tail -8
```

Expected: successful compile.

- [ ] **Step 4: Look at the page**

```bash
cd frontend && npm run dev
```

Open `http://localhost:3000`. Confirm: the scorecard tiles are unchanged, every section still renders, and the charts are now CSS bars with a value beside each. Stop the server.

- [ ] **Step 5: Commit**

```bash
git add frontend/components/ui.jsx frontend/app/globals.css
git commit -m "feat: one chart engine and one table for the whole dashboard

Replaces the SVG bars with CSS bars that carry percentage and custom value
formatting and reflow inside a two-up grid. A non-finite value now omits its
bar and reads N/A instead of drawing a zero-width mark."
```

---

## Task 10: Give the measurement sections cards and chart grids

**Files:**
- Modify: `frontend/components/sections.jsx:102-402`

**Interfaces:**
- Consumes: `MetricGrid`, `MetricCard`, `ChartGrid`, `BarChart`, `GroupedBarChart`, `DataTable` from `./ui`
- Produces: `Scorecard`, `RelevanceSection`, `SatisfactionSection`, `FreshnessSection`, `DiversitySection`, `FairnessSection`, `SafetySection`, `LatencySection` — unchanged export names and props

- [ ] **Step 1: Update the imports at line 1**

```jsx
import {
  Section, NaCard, BarChart, GroupedBarChart, DataTable, MetricTile,
  MetricGrid, MetricCard, ChartGrid,
} from "./ui";
```

- [ ] **Step 2: Give `MeasurementSection` a description and card grid**

Replace the body of `MeasurementSection` (lines 102-130). The N/A path, the fine-print support line, the warnings and the caveat `children` are unchanged:

```jsx
function MeasurementSection({ title, data, columns, kpis, chart, description, children }) {
  if (!data || data.status !== "available") {
    return <NaCard title={title} id={title.toLowerCase()} reason={data?.warnings?.[0] || "measurement unavailable"} />;
  }
  const rows = data.rows || [];
  const values = kpis ? kpis(rows) : [];
  return (
    <Section title={title} headline={data.headline} description={description} id={title.toLowerCase()}>
      <p className="fine-print">
        sample size {data.sampleSize?.toLocaleString() ?? "N/A"} · coverage {share(data.coverage)}
        {data.window ? ` · window ${data.window}` : ""}
      </p>
      {data.warnings?.length ? <p className="na">{data.warnings.join(" · ")}</p> : null}
      {values.length ? (
        <MetricGrid>
          {values.map((kpi) => (
            <MetricCard key={kpi.label} label={kpi.label} value={kpi.value} detail={kpi.detail} />
          ))}
        </MetricGrid>
      ) : null}
      {chart ? chart(rows) : null}
      <DataTable rows={rows} columns={columns} />
      {children}
    </Section>
  );
}
```

- [ ] **Step 3: Add a description and a second chart to each measurement section**

Keep every existing `columns` array and `kpis` function exactly as they are — only add the `description` prop and replace the `chart` prop. For each section below, add the `description` line and swap the `chart` body.

`RelevanceSection`:

```jsx
      description="Ranking quality of the served slates at each cutoff, over slates that carry at least one positive label."
      chart={(rows) => (
        <ChartGrid>
          <GroupedBarChart
            title="Relevance by cutoff"
            labels={rows.map((r) => `k=${r.k}`)}
            series={[
              { name: "ndcg", values: rows.map((r) => r.ndcg_at_k) },
              { name: "mrr", values: rows.map((r) => r.mrr_at_k) },
            ]}
          />
          <BarChart
            title="Recall by cutoff"
            labels={rows.map((r) => `k=${r.k}`)}
            values={rows.map((r) => r.recall_at_k)}
          />
        </ChartGrid>
      )}
```

Note the series values no longer pass `?? 0` — a missing cutoff now reads N/A instead of plotting as zero.

`SatisfactionSection`:

```jsx
      description="Observed engagement and the coverage of each optional feedback signal."
      chart={(rows) => {
        const row = rows[0] || {};
        // negative_feedback_coverage is the same expression as negative_feedback_rate (both count
        // the samples carrying a reason), so plotting it here would read as "not instrumented"
        // for a signal that is instrumented and simply did not fire. It stays in the table
        // beside its rate, where the two are legible together.
        const fields = ["rating_coverage", "dwell_coverage", "completion_coverage"];
        return (
          <ChartGrid>
            <BarChart title="Optional signal coverage" percentage
              labels={fields.map((f) => f.replace("_coverage", ""))}
              values={fields.map((f) => row[f])} />
            <BarChart title="Engagement rates" percentage
              labels={["ctr", "order rate", "negative feedback"]}
              values={[row.ctr, row.order_rate, row.negative_feedback_rate]} />
          </ChartGrid>
        );
      }}
```

`FreshnessSection`:

```jsx
      description="How much of what was shown is recent, and whether recency tracks engagement."
      chart={(rows) => {
        const row = rows[0] || {};
        return (
          <ChartGrid>
            <BarChart title="CTR by content age" percentage
              labels={["fresh", "established"]}
              values={[row.fresh_ctr, row.established_ctr]} />
            <BarChart title="Mean reward by content age"
              labels={["fresh", "established"]}
              values={[row.fresh_mean_reward, row.established_mean_reward]} />
          </ChartGrid>
        );
      }}
```

`DiversitySection` — one chart, full width. The three normalized signals already share a scale; there is no second view that is not a restatement:

```jsx
      description="Genre spread and long-tail exposure within a slate, on a 0–1 scale."
      chart={(rows) => {
        const row = rows.find((r) => r.scope === "aggregate") || rows[0] || {};
        return (
          <BarChart title="Diversity (0–1)"
            labels={["genre entropy", "intra-list distance", "long-tail share"]}
            values={[row.normalized_genre_entropy, row.intra_list_genre_distance,
                     row.long_tail_exposure_share]} />
        );
      }}
```

`FairnessSection`:

```jsx
      description="Engagement and ranking quality by demographic group, for the dimension with the widest CTR gap."
      chart={(rows) => {
        const row = maxByField(rows, "ctr_max_min_gap") || rows[0] || {};
        const groups = row.groups || [];
        return groups.length ? (
          <ChartGrid>
            <BarChart title={`CTR by ${row.dimension} (overall ${num(row.overall_ctr, 3)})`}
              labels={groups.map((g) => g.group)} values={groups.map((g) => g.ctr)} percentage />
            <BarChart title={`NDCG by ${row.dimension} (overall ${num(row.overall_ndcg, 3)})`}
              labels={groups.map((g) => g.group)} values={groups.map((g) => g.ndcg)} />
          </ChartGrid>
        ) : null;
      }}
```

`SafetySection` — one chart. Add only the description:

```jsx
      description="Policy filter decisions over evaluated candidates, and the share the policy could not classify."
```

`LatencySection`:

```jsx
      description="Live request and stage latency from the retrieval service."
      chart={(rows) => {
        const stages = rows.filter((r) => r.scope === "stage");
        const endpoints = rows.filter((r) => r.scope === "endpoint");
        return (
          <ChartGrid>
            {stages.length ? (
              <BarChart title="p95 by stage (ms)"
                labels={stages.map((r) => r.name)} values={stages.map((r) => r.p95)} />
            ) : null}
            {endpoints.length ? (
              <GroupedBarChart title="Percentiles by endpoint (ms)"
                labels={endpoints.map((r) => r.name)}
                series={[
                  { name: "p50", values: endpoints.map((r) => r.p50) },
                  { name: "p95", values: endpoints.map((r) => r.p95) },
                  { name: "p99", values: endpoints.map((r) => r.p99) },
                ]} />
            ) : null}
          </ChartGrid>
        );
      }}
```

- [ ] **Step 4: Verify the build and look at the page**

```bash
cd frontend && npm run build 2>&1 | tail -6 && npm run dev
```

Open `http://localhost:3000`. Confirm for each of the seven sections: the headline chip, the `sample size · coverage` line, any warnings, and the caveat paragraphs are all still present, and no card shows a number where the old `kpi-row` showed N/A. Stop the server.

- [ ] **Step 5: Commit**

```bash
git add frontend/components/sections.jsx
git commit -m "feat: measurement sections gain KPI cards and paired charts"
```

---

## Task 11: Rebuild the diagnostic sections

`EngagementSection` absorbs what the sample called `RelevanceSection` — the funnel report — under its correct name, so the real `RelevanceSection` keeps its NDCG and MRR.

**Files:**
- Modify: `frontend/components/sections.jsx:404-505`

**Interfaces:**
- Consumes: `MetricGrid`, `MetricCard`, `ChartGrid`, `BarChart`, `DataTable`, `NaCard`, `Section` from `./ui`
- Produces: `EngagementSection`, `QuerySection`, `RecallSection`, `RankingSection`, `OpeSection`, `MdpSection` — unchanged export names and props. `KeywordSection` is **removed** from this file; Task 12 creates it in `keyword-report.jsx`.

- [ ] **Step 1: Add shared helpers below the existing `ci` helper (after line 11)**

```jsx
const count = (v) => (v === null || v === undefined ? "N/A" : Number(v).toLocaleString());

// Rank by a field, dropping rows that have no value for it. Treating a missing
// value as zero would let "best AUC" name a signal that was never scored.
const rankBy = (rows, field, direction = "desc") =>
  (rows ?? [])
    .filter((r) => r?.[field] !== null && r?.[field] !== undefined)
    .sort((a, b) => (direction === "desc" ? b[field] - a[field] : a[field] - b[field]));

const COUNT_COLUMNS = {
  impressions: count, clicks: count, orders: count, queries: count,
  movie_impressions: count, query_clicks: count, query_orders: count,
  users_evaluated: count, instances: count, n: count, positives: count,
  episodes: count,
};
const RATE_COLUMNS = { ctr: share, cvr: share, coverage: share, positive_rate: share };
```

`share` and `num` already exist at the top of the file.

- [ ] **Step 2: Replace `EngagementSection` (lines 404-414)**

```jsx
export function EngagementSection({ data }) {
  if (!data) return <NaCard title="Engagement funnel" id="engagement" reason="no engagement data" />;
  const funnel = data.funnel || {};
  return (
    <Section title="Engagement funnel" headline={data.headline} id="engagement"
      description="Traffic from recommendation impression through click to order, split by query and by genre.">
      <MetricGrid>
        <MetricCard label="Impressions" value={count(funnel.impression)} />
        <MetricCard label="Clicks" value={count(funnel.click)} detail={`${share(data.ctr)} CTR`} />
        <MetricCard label="Orders" value={count(funnel.order)} detail={`${share(data.cvr)} CVR`} />
        <MetricCard label="Queries" value={count((data.by_query || []).length)} />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Funnel"
          labels={["impression", "click", "order"]}
          values={[funnel.impression, funnel.click, funnel.order]} />
        <BarChart title="CTR by genre" percentage
          labels={(data.by_genre || []).map((r) => r.genre)}
          values={(data.by_genre || []).map((r) => r.ctr)} />
      </ChartGrid>
      <h3 className="report-subtitle">By query</h3>
      <DataTable rows={data.by_query} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS, mean_score: (v) => num(v, 4) }}
        columns={["query", "impressions", "clicks", "orders", "ctr", "cvr", "mean_score"]} />
      <h3 className="report-subtitle">By genre</h3>
      <DataTable rows={data.by_genre} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS, mean_score: (v) => num(v, 4) }}
        columns={["genre", "impressions", "clicks", "orders", "ctr", "cvr", "mean_score"]} />
      <p className="fine-print">
        CVR is orders per impression, matching the rate the exporter publishes everywhere else
        on this page — not orders per click. <code>mean_score</code> is the mean label.
      </p>
    </Section>
  );
}
```

- [ ] **Step 3: Delete `KeywordSection` (lines 416-431)**

Remove it entirely. Task 12 recreates it in its own module.

- [ ] **Step 4: Replace `QuerySection` (lines 433-446)**

```jsx
export function QuerySection({ data }) {
  if (!data) return <NaCard title="Query intent" id="query" reason="no query data" />;
  const rows = data.top_queries || [];
  const byCtr = rankBy(rows, "ctr");
  const byCvr = rankBy(rows, "cvr");
  return (
    <Section title="Query intent" headline={data.headline} id="query"
      description="Query demand, engagement, conversion and intent depth.">
      <MetricGrid>
        <MetricCard label="Queries analyzed" value={count(rows.length)} />
        <MetricCard label="Highest CTR" value={share(byCtr[0]?.ctr)} detail={byCtr[0]?.query} />
        <MetricCard label="Highest CVR" value={share(byCvr[0]?.cvr)} detail={byCvr[0]?.query} />
        <MetricCard label="Mean query length" value={num(data.average_query_length, 2)} detail="characters" />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Top queries by impressions" horizontal
          labels={rows.map((r) => r.query)} values={rows.map((r) => r.impressions)} />
        <BarChart title="Top queries by CTR" horizontal percentage
          labels={byCtr.map((r) => r.query)} values={byCtr.map((r) => r.ctr)} />
      </ChartGrid>
      <DataTable rows={rows} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS }}
        columns={["query", "impressions", "clicks", "orders", "query_len", "ctr", "cvr"]} />
      <h3 className="report-subtitle">By query length</h3>
      <DataTable rows={data.by_length} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS }}
        columns={["bucket", "queries", "impressions", "clicks", "orders", "ctr", "cvr"]} />
    </Section>
  );
}
```

- [ ] **Step 5: Replace `RecallSection` (lines 448-455)**

```jsx
export function RecallSection({ data }) {
  if (!data) return <NaCard title="Candidate recall" id="recall" reason="no movie:*:features in Redis" />;
  const rows = data.rows || [];
  const byRecall = rankBy(rows, "recall_at_k");
  const byHitRate = rankBy(rows, "hitrate_at_k");
  return (
    <Section title="Candidate recall" headline={data.headline} id="recall"
      description="Candidate-generation quality across retrieval strategies and cutoffs.">
      <MetricGrid>
        <MetricCard label="Best recall@K" value={num(byRecall[0]?.recall_at_k, 3)}
          detail={byRecall[0] ? `${byRecall[0].method}, K=${byRecall[0].k}` : undefined} />
        <MetricCard label="Best hit rate@K" value={num(byHitRate[0]?.hitrate_at_k, 3)}
          detail={byHitRate[0] ? `${byHitRate[0].method}, K=${byHitRate[0].k}` : undefined} />
        <MetricCard label="Methods" value={count(new Set(rows.map((r) => r.method)).size)} />
        <MetricCard label="Users evaluated" value={count(rankBy(rows, "users_evaluated")[0]?.users_evaluated)} />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Recall@K" horizontal
          labels={rows.map((r) => `${r.method} @ ${r.k}`)} values={rows.map((r) => r.recall_at_k)} />
        <BarChart title="Hit rate@K" horizontal
          labels={rows.map((r) => `${r.method} @ ${r.k}`)} values={rows.map((r) => r.hitrate_at_k)} />
      </ChartGrid>
      <DataTable rows={rows} formatters={COUNT_COLUMNS}
        columns={["method", "k", "recall_at_k", "hitrate_at_k", "users_evaluated", "instances"]} />
    </Section>
  );
}
```

- [ ] **Step 6: Replace `RankingSection` (lines 457-468)**

```jsx
export function RankingSection({ data }) {
  if (!data) return <NaCard title="Ranking quality" id="ranking" reason="no popularity or i2vEmb:* signals in Redis" />;
  const rows = data.rows || [];
  const byAuc = rankBy(rows, "auc");
  const byLogloss = rankBy(rows, "logloss", "asc");
  const byCoverage = rankBy(rows, "coverage");
  return (
    <Section title="Ranking quality" headline={data.headline} id="ranking"
      description="Predictive quality and coverage of each available ranking signal.">
      <MetricGrid>
        <MetricCard label="Best AUC" value={num(byAuc[0]?.auc, 3)} detail={byAuc[0]?.signal} />
        <MetricCard label="Lowest log loss" value={num(byLogloss[0]?.logloss, 3)} detail={byLogloss[0]?.signal} />
        <MetricCard label="Best coverage" value={share(byCoverage[0]?.coverage)} detail={byCoverage[0]?.signal} />
        <MetricCard label="Signals evaluated" value={count(rows.length)} />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="AUC by signal" horizontal
          labels={rows.map((r) => r.signal)} values={rows.map((r) => r.auc)} />
        <BarChart title="Coverage by signal" horizontal percentage
          labels={rows.map((r) => r.signal)} values={rows.map((r) => r.coverage)} />
      </ChartGrid>
      <DataTable rows={rows} formatters={{ ...COUNT_COLUMNS, ...RATE_COLUMNS }}
        columns={["signal", "n", "positives", "positive_rate", "coverage", "auc", "logloss"]} />
      <p className="fine-print">
        A signal with no scored rows reports no AUC and no positive rate rather than zero, and
        is omitted from the charts above.
      </p>
    </Section>
  );
}
```

- [ ] **Step 7: Replace `OpeSection` (lines 470-491)**

```jsx
export function OpeSection({ data }) {
  if (!data) return <NaCard title="Off-policy evaluation" id="ope" reason="no replay-buffer events with reward in Redis" />;
  const rows = data.rows || [];
  const cal = data.calibration || {};
  const byValue = rankBy(rows, "value");
  const byLift = rankBy(rows, "lift_vs_logging");
  const disp = rows.map((x) => ({
    policy: x.policy,
    value: num(x.value),
    value_95ci: ci(x.value_ci_low, x.value_ci_high),
    lift_vs_logging: pct(x.lift_vs_logging),
    lift_95ci: ci(x.lift_ci_low, x.lift_ci_high, true),
    n: count(x.n_events),
  }));
  return (
    <Section title="Off-policy evaluation" headline={data.headline} id="ope"
      description="Estimated value and lift of each candidate policy on logged events.">
      <MetricGrid>
        <MetricCard label="Best policy" value={byValue[0]?.policy ?? "N/A"} detail={`value ${num(byValue[0]?.value, 3)}`} />
        <MetricCard label="Highest lift" value={pct(byLift[0]?.lift_vs_logging)} detail={byLift[0]?.policy} />
        <MetricCard label="Reward model AUC" value={num(cal.auc, 3)} />
        <MetricCard label="Reward model MSE" value={num(cal.mse, 4)} />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Estimated policy value" horizontal
          labels={rows.map((r) => r.policy)} values={rows.map((r) => r.value)} />
        <BarChart title="Lift vs logging policy" horizontal percentage
          labels={rows.map((r) => r.policy)} values={rows.map((r) => r.lift_vs_logging)} />
      </ChartGrid>
      <DataTable rows={disp} columns={["policy", "value", "value_95ci", "lift_vs_logging", "lift_95ci", "n"]} />
      <p className="fine-print">
        Direct Method · reward estimator AUC {num(cal.auc)} MSE {num(cal.mse)} (n_test {count(cal.n_test)}). 95%
        event-bootstrap CIs are conditional on the fixed reward model; model-fit uncertainty excluded.
      </p>
    </Section>
  );
}
```

- [ ] **Step 8: Replace `MdpSection` (lines 493-505)**

```jsx
export function MdpSection({ data }) {
  if (!data) return <NaCard title="MDP policy evaluation" id="mdp" reason="no mdp_eval.csv" />;
  const rows = (data.rows || []).map((r) => ({ ...r, ci95: ci(r.ci95_low, r.ci95_high) }));
  const byReturn = rankBy(rows, "mean_return");
  const bySteps = rankBy(rows, "mean_steps", "asc");
  return (
    <Section title="MDP policy evaluation" headline={data.headline} id="mdp"
      description="Finite-horizon discounted returns over seeded episodes.">
      <MetricGrid>
        <MetricCard label="Best mean return" value={num(byReturn[0]?.mean_return, 3)} detail={byReturn[0]?.policy} />
        <MetricCard label="Shortest trajectory" value={num(bySteps[0]?.mean_steps, 2)} detail={bySteps[0]?.policy} />
        <MetricCard label="Policies evaluated" value={count(rows.length)} />
        <MetricCard label="Total episodes"
          value={count(rows.reduce((sum, r) => sum + Number(r.episodes ?? 0), 0))} />
      </MetricGrid>
      <ChartGrid>
        <BarChart title="Mean return by policy" horizontal
          labels={rows.map((r) => r.policy)} values={rows.map((r) => r.mean_return)} />
        <BarChart title="Mean episode length" horizontal
          labels={rows.map((r) => r.policy)} values={rows.map((r) => r.mean_steps)} />
      </ChartGrid>
      <DataTable rows={rows} formatters={COUNT_COLUMNS}
        columns={["policy", "episodes", "mean_return", "mean_steps", "standard_error", "ci95"]} />
      <p className="fine-print">
        Finite-horizon discounted return over seeded episodes; 95% bootstrap CIs quantify episode-sampling
        uncertainty for this fixed dataset.
      </p>
    </Section>
  );
}
```

- [ ] **Step 9: Add the subtitle style to `frontend/app/globals.css`**

```css
.report-subtitle {
  margin: 4px 0 -8px;
  font-size: 0.82rem;
  font-weight: 800;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: var(--muted);
}
```

- [ ] **Step 10: Remove the keyword import so the build can run**

`page.jsx` still imports `KeywordSection` from `./sections`, which no longer exports it. **Delete** — do not comment out — the `KeywordSection,` entry from the `../components/sections` import list and the `<KeywordSection data={data.keyword} />` line in `frontend/app/page.jsx`. Task 12 adds both back, importing from the new module. The keyword section is absent from the page for exactly one task; leaving commented-out code behind instead would be dead code in the committed tree.

- [ ] **Step 11: Verify the build and look at the page**

```bash
cd frontend && npm run build 2>&1 | tail -6 && npm run dev
```

Open `http://localhost:3000`. Confirm the engagement, query, recall, ranking and OPE sections each show four KPI cards, two charts and their tables, that MDP renders its N/A card, and that no column shows a value where the data has none. Stop the server.

- [ ] **Step 12: Commit**

```bash
git add frontend/components/sections.jsx frontend/app/page.jsx frontend/app/globals.css
git commit -m "feat: rebuild the diagnostic sections as card-and-chart reports

The sample's RelevanceSection was the engagement funnel under another name; it
lands as EngagementSection so the real RelevanceSection keeps NDCG and MRR.
Best-of cards rank with rows that have no value excluded, not coerced to zero."
```

---

## Task 12: Build the Top-K keyword report

The only interactive section. It lives in its own client module so `page.jsx` stays a server component.

**Files:**
- Create: `frontend/components/keyword-report.jsx`
- Modify: `frontend/app/page.jsx`
- Modify: `frontend/app/globals.css` (append)

**Interfaces:**
- Consumes: `Section`, `NaCard`, `BarChart`, `DataTable`, `MetricGrid`, `MetricCard`, `ChartGrid` from `./ui`
- Produces: `KeywordSection({ data })` as a named export from `./keyword-report`

- [ ] **Step 1: Create `frontend/components/keyword-report.jsx`**

```jsx
"use client";

import { useMemo, useState } from "react";
import { Section, NaCard, BarChart, DataTable, MetricGrid, MetricCard, ChartGrid } from "./ui";

const num = (v, d = 4) => (v === null || v === undefined ? "N/A" : (Math.round(v * 10 ** d) / 10 ** d).toString());
const share = (v) => (v === null || v === undefined ? "N/A" : `${(v * 100).toFixed(1)}%`);
const count = (v) => (v === null || v === undefined ? "N/A" : Number(v).toLocaleString());

const TOP_K_CHOICES = [10, 20, 30, 50];

// Text must stay legible as the chip darkens, so it flips at the point the ramp
// crosses into its dark half rather than relying on one ink for the whole scale.
const INK_FLIP = 0.55;

function Select({ label, value, onChange, options }) {
  return (
    <label className="select-control">
      <span>{label}</span>
      <select value={value} onChange={(e) => onChange(Number(e.target.value))}>
        {options.map((o) => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
    </label>
  );
}

function TokenHeatmap({ items, labelKey, scoreKey, selectedKey, onSelect }) {
  const scores = items.map((i) => i[scoreKey]).filter((s) => s !== null && s !== undefined);
  const min = scores.length ? Math.min(...scores) : 0;
  const max = scores.length ? Math.max(...scores) : 0;
  const normalize = (s) => (s === null || s === undefined ? null : max === min ? 0.5 : (s - min) / (max - min));

  return (
    <div className="token-heatmap">
      {items.map((item, i) => {
        const label = item[labelKey];
        const score = item[scoreKey];
        const t = normalize(score);
        // A keyword with no score is rendered unfilled rather than at the cold end
        // of the ramp, which would read as "measured, and lowest".
        const style = t === null
          ? { "--token-score": 0, "--token-ink": "var(--muted)" }
          : { "--token-score": t, "--token-ink": t > INK_FLIP ? "#fff" : "var(--ink)" };
        const className = [
          "token-chip",
          t === null ? "unscored" : "",
          selectedKey === label ? "selected" : "",
        ].filter(Boolean).join(" ");

        return (
          <button key={`${label}-${i}`} type="button" className={className} style={style}
            title={`${label}: ${num(score, 4)}`} aria-pressed={selectedKey === label}
            onClick={() => onSelect(item)}>
            <span>{label}</span>
            <small>{num(score, 3)}</small>
          </button>
        );
      })}
    </div>
  );
}

function KeywordDetail({ keyword }) {
  if (!keyword) {
    return <aside className="keyword-detail-panel"><p className="empty-state">Select a keyword to inspect it.</p></aside>;
  }
  return (
    <aside className="keyword-detail-panel">
      <span className="eyebrow">Keyword detail</span>
      <h3 className="keyword-detail-title">{keyword.keyword}</h3>
      <div className="keyword-detail-metrics">
        <MetricCard label="Rank" value={`#${keyword.rank}`} />
        <MetricCard label="Relevance" value={num(keyword.mean_score, 3)} detail="mean label" />
        <MetricCard label="Impressions" value={count(keyword.movie_impressions)} />
        <MetricCard label="Clicks" value={count(keyword.query_clicks)} />
        <MetricCard label="CTR" value={share(keyword.ctr)} />
        <MetricCard label="CVR" value={share(keyword.cvr)} />
      </div>
      <h4 className="keyword-detail-subtitle">Shown versus clicked</h4>
      <BarChart title={null} percentage
        labels={["share of impressions", "share of clicks"]}
        values={[keyword.movie_share, keyword.query_share]} />
      <p className="fine-print">
        Divergence {num(keyword.divergence, 4)} is the second share minus the first: positive means
        the keyword earns a larger share of clicks than of exposure.
      </p>
    </aside>
  );
}

export function KeywordSection({ data }) {
  const available = data?.by_keyword ?? [];
  const [topK, setTopK] = useState(20);
  const [selectedKeyword, setSelectedKeyword] = useState(null);

  // Never offer a cutoff larger than the pool: "Top 50" over twelve keywords is a
  // promise the data cannot keep.
  const options = useMemo(() => {
    const fits = TOP_K_CHOICES.filter((k) => k < available.length).map((k) => ({ value: k, label: `Top ${k}` }));
    return [...fits, { value: available.length, label: `All (${available.length})` }];
  }, [available.length]);

  // The default cutoff may exceed a small pool, which would leave the select with
  // a value matching no option.
  const activeK = options.some((o) => o.value === topK) ? topK : options[options.length - 1].value;

  // Ranked by relevance, with unscored keywords last rather than sorted as zero.
  const keywords = useMemo(() => {
    const scored = available.filter((r) => r.mean_score !== null && r.mean_score !== undefined);
    const unscored = available.filter((r) => r.mean_score === null || r.mean_score === undefined);
    return [...scored.sort((a, b) => b.mean_score - a.mean_score), ...unscored]
      .slice(0, activeK)
      .map((row, i) => ({ ...row, rank: i + 1 }));
  }, [available, activeK]);

  // After the hooks, never before: hook order must not depend on the data.
  if (!available.length) {
    return <NaCard title="Keyword relevance" id="keyword" reason="no keyword rows in the snapshot" />;
  }

  const selected = keywords.find((r) => r.keyword === selectedKeyword) ?? keywords[0] ?? null;
  const best = keywords[0];
  const impressions = keywords.reduce((sum, r) => sum + Number(r.movie_impressions ?? 0), 0);

  return (
    <Section title="Keyword relevance" headline={data.headline} id="keyword"
      description="Catalog keywords ranked by relevance and colored by it. Select one to inspect how its exposure compares with its clicks."
      actions={<Select label="Show" value={activeK} onChange={setTopK} options={options} />}>
      <MetricGrid>
        <MetricCard label="Keywords shown" value={count(keywords.length)} detail={`of ${available.length} exported`} />
        <MetricCard label="Highest relevance" value={num(best?.mean_score, 3)} detail={best?.keyword} />
        <MetricCard label="Impressions covered" value={count(impressions)} />
        <MetricCard label="Widest divergence"
          value={num([...keywords].sort((a, b) => Math.abs(b.divergence ?? 0) - Math.abs(a.divergence ?? 0))[0]?.divergence, 3)}
          detail={[...keywords].sort((a, b) => Math.abs(b.divergence ?? 0) - Math.abs(a.divergence ?? 0))[0]?.keyword} />
      </MetricGrid>

      <div className="keyword-report-layout">
        <div className="keyword-main-panel">
          <TokenHeatmap items={keywords} labelKey="keyword" scoreKey="mean_score"
            selectedKey={selected?.keyword} onSelect={(item) => setSelectedKeyword(item.keyword)} />
          <div className="token-legend">
            <span>lower relevance</span>
            <div className="token-legend-gradient" />
            <span>higher relevance</span>
          </div>
        </div>
        <KeywordDetail keyword={selected} />
      </div>

      <ChartGrid>
        <BarChart title="Impressions by keyword" horizontal
          labels={keywords.map((r) => r.keyword)} values={keywords.map((r) => r.movie_impressions)} />
        <BarChart title="Click-to-exposure divergence" horizontal
          labels={keywords.map((r) => r.keyword)} values={keywords.map((r) => r.divergence)} />
      </ChartGrid>

      <DataTable rows={keywords}
        columns={["rank", "keyword", "mean_score", "movie_impressions", "query_clicks",
                  "query_orders", "ctr", "cvr", "divergence"]}
        formatters={{
          mean_score: (v) => num(v, 4),
          movie_impressions: count, query_clicks: count, query_orders: count,
          ctr: share, cvr: share, divergence: (v) => num(v, 4),
        }} />

      {["l1", "l2", "l3"].map((level) => {
        const rows = data.tops?.[level] ?? [];
        if (!rows.length) return null;
        return (
          <div key={level}>
            <h3 className="report-subtitle">Taxonomy level {level.toUpperCase()}</h3>
            <DataTable rows={rows} compact
              columns={[level, "keyword", "movie_impressions", "query_clicks", "ctr"]}
              formatters={{ movie_impressions: count, query_clicks: count, ctr: share }} />
          </div>
        );
      })}

      <p className="fine-print">
        Relevance is the mean label over a keyword&apos;s impressions. The pool is the {available.length}{" "}
        most-shown keywords, so this ranks relevance within them rather than across the whole catalog.
      </p>
    </Section>
  );
}
```

- [ ] **Step 2: Restore the import in `frontend/app/page.jsx`**

Uncomment the `<KeywordSection data={data.keyword} />` line and replace the commented import with a separate one, leaving the section order unchanged:

```jsx
import { KeywordSection } from "../components/keyword-report";
```

`KeywordSection` must not appear in the `../components/sections` import list.

- [ ] **Step 3: Append the keyword styles to `frontend/app/globals.css`**

```css
/* --- Keyword report ---------------------------------------------------- */

.keyword-report-layout {
  display: grid;
  grid-template-columns: minmax(0, 1.6fr) minmax(260px, 1fr);
  gap: 20px;
  align-items: start;
}

.keyword-main-panel {
  display: grid;
  gap: 12px;
  min-width: 0;
}

.token-heatmap {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

/* Sequential ramp: one hue, light to dark. Relevance is also printed on each
   chip and available in the table, so the encoding is never colour alone. */
.token-chip {
  display: inline-flex;
  align-items: baseline;
  gap: 7px;
  padding: 7px 11px;
  border: 1px solid var(--line);
  border-radius: 999px;
  background: color-mix(in oklab, var(--indigo) calc(var(--token-score) * 100%), var(--indigo-soft));
  color: var(--token-ink);
  font-size: 0.82rem;
  font-weight: 600;
  cursor: pointer;
}

.token-chip.unscored {
  background: repeating-linear-gradient(135deg, #f8fafc, #f8fafc 4px, #eef2f7 4px, #eef2f7 8px);
}

.token-chip.selected {
  outline: 2px solid var(--ink);
  outline-offset: 2px;
}

.token-chip small {
  font-variant-numeric: tabular-nums;
  font-weight: 500;
  opacity: 0.85;
}

.token-legend {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 0.72rem;
  color: var(--muted);
}

.token-legend-gradient {
  flex: 1;
  max-width: 190px;
  height: 9px;
  border-radius: 999px;
  background: linear-gradient(90deg, var(--indigo-soft), var(--indigo));
}

.keyword-detail-panel {
  display: grid;
  gap: 12px;
  align-content: start;
  padding: 18px;
  border: 1px solid var(--line);
  border-radius: 16px;
  background: #fbfcff;
}

.keyword-detail-title {
  margin: 0;
  font-size: 1.1rem;
  letter-spacing: -0.02em;
}

.keyword-detail-metrics {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(96px, 1fr));
  gap: 8px;
}

.keyword-detail-metrics .metric-card {
  padding: 10px 12px;
}

.keyword-detail-metrics .card-value {
  font-size: 1.05rem;
}

.keyword-detail-subtitle {
  margin: 4px 0 0;
  font-size: 0.78rem;
  font-weight: 800;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: var(--muted);
}

.select-control {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-size: 0.76rem;
  font-weight: 700;
  color: var(--muted);
}

.select-control select {
  padding: 7px 10px;
  border: 1px solid var(--line);
  border-radius: 9px;
  background: var(--surface);
  color: var(--ink);
  font: inherit;
  font-weight: 600;
}

@media (max-width: 900px) {
  .keyword-report-layout {
    grid-template-columns: 1fr;
  }
}
```

- [ ] **Step 4: Verify the build**

```bash
cd frontend && npm run build 2>&1 | tail -8
```

Expected: successful compile. The route listing should still show `/` as static — only `keyword-report.jsx` is a client component, and it takes its data as a prop from the server component.

- [ ] **Step 5: Look at the page and exercise the control**

```bash
cd frontend && npm run dev
```

At `http://localhost:3000#keyword`, confirm: the chips run light to dark by relevance rather than all one colour; changing the selector changes how many chips appear and the "Keywords shown" card agrees; clicking a chip updates the detail panel; and the selector offers no option larger than the number of exported keywords. Stop the server.

- [ ] **Step 6: Commit**

```bash
git add frontend/components/keyword-report.jsx frontend/app/page.jsx frontend/app/globals.css
git commit -m "feat: Top-K keyword report with a real relevance heatmap

Chips are coloured by the mean label the exporter now publishes, so the ramp
encodes a measurement instead of a constant. The sampled detail panel's
synthesized per-token scores are gone; the panel compares exposure share with
click share, which is what divergence measures."
```

---

## Task 13: Document, verify end to end, and open the pull request

**Files:**
- Modify: `frontend/README.md`

**Interfaces:**
- Consumes: everything above
- Produces: a pull request against `master`

- [ ] **Step 1: Update the layout block in `frontend/README.md`**

Replace the `components/` lines in the Layout section (lines 75-77):

```
├── components/
│   ├── ui.jsx            # Section, NaCard, MetricTile, MetricGrid/MetricCard,
│   │                     #   ChartGrid, BarChart, GroupedBarChart, DataTable
│   ├── sections.jsx      # Scorecard + the measurement and diagnostic sections
│   └── keyword-report.jsx # "use client": the Top-K keyword report
```

- [ ] **Step 2: Add a note under "Refresh the data"**

Append after the existing configuration-flags paragraph:

```markdown
The keyword report selects from the 50 most-shown keywords, so `by_keyword` and
`by_subkeyword` are exported 50 rows deep while the other diagnostic tables stay at 10.
```

- [ ] **Step 3: Run the full verification**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
cd recsys-pipeline && python -m pytest integration-tests/python_modeling/ -q 2>&1 | tail -5
cd ../frontend && npm run build 2>&1 | tail -8
```

Expected: all Python tests pass, and the frontend builds with `dashboard.json valid: 7 measurement sections, schema 2.0`.

- [ ] **Step 4: Confirm no section regressed to N/A**

```bash
cd frontend && npm run dev
```

Walk the whole page against the section list: Scorecard, Relevance, Satisfaction, Freshness, Diversity, Fairness, Safety, Latency, Engagement, Keyword, Query, Recall, Ranking, OPE, MDP. Every section that showed a value before this branch must still show one; only MDP is expected to be N/A, because `mdp` is `null` in this snapshot. Stop the server.

- [ ] **Step 5: Commit and open the pull request**

```bash
git add frontend/README.md
git commit -m "docs: describe the expanded report components and export depth"
git push -u origin feature/expanded-dashboard-reports
gh pr create --base master --title "Expanded dashboard reports" --body "$(cat <<'EOF'
Merges the sampled expanded dashboard UI into the existing report structure.

The sample was appended to `sections.jsx` and `ui.jsx` rather than merged, so both
files declared eleven components twice and the app did not build. It also read
about fifteen fields the snapshot does not publish, and synthesized per-token
relevance scores for display.

**Data.** `compute_relevance`, `compute_keyword`, `compute_query` and
`compute_ranking` now publish the clicks, orders, CTR, CVR, keyword relevance,
average query length and positive rate the reports read — all derived from the
`label` column already present. `coverage_at_k` and per-token scores are not
derivable and were dropped along with the visuals that wanted them.
`schemaVersion` stays `2.0`: the seven measurement sections are unchanged.

**UI.** One chart engine and one table replace the duplicated pairs. Every
section gained a KPI card grid and, where an honest second view exists, a paired
chart. The Top-K keyword report moved to its own client component so the page
still prerenders. Rankings exclude rows with no value instead of coercing them to
zero, so a "best AUC" card cannot name an unscored signal.

Design: `.superpowers/docs/specs/2026-07-30-expanded-dashboard-reports-design.md`
Plan: `.superpowers/docs/plans/2026-07-30-expanded-dashboard-reports.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Notes for the implementer

**Why `EngagementSection` and not `RelevanceSection`.** The sampled file defined a `RelevanceSection` that renders `data.funnel`, `by_query` and `by_genre` — the engagement funnel. The Python function behind that data is also called `compute_relevance`, which is where the confusion started, but the exporter maps it to the `engagement` key and the dashboard's `RelevanceSection` is the NDCG/MRR measurement. Merging the sample under its own name would have deleted a measurement section.

**Why rankings drop nulls.** `ranking.rows` legitimately contains rows with `auc: null` — a signal Redis had no data for. `Number(null ?? 0)` is `0`, so a descending sort puts them last, but an ascending sort by `logloss` puts them *first*, and the "lowest log loss" card would name a signal that was never scored. `rankBy` filters before sorting.

**What was deliberately not built.** `MetricCard` has no `trend` prop: nothing in the snapshot carries a prior period to compare against, so a trend arrow would have no input. `coverage_at_k` is absent from the recall table for the same reason.
