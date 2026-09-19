# A category × keyword relevance heatmap design

## Problem and decision

The Keyword gap section answers "where does catalog supply diverge from query demand" with two
ranked tables and three top-ten lists. It cannot answer "which genres perform well inside which
category", because nothing in the section crosses the two axes.

The cross-tab exists in the computation and is discarded before it reaches the browser.
`compute_keyword`'s `top_keywords` groups by `(l1, genres)` — category family by keyword — with
impressions and clicks per cell. Two truncations then remove it: `top_keywords` keeps only
`rank <= 10` within each family, and `export_dashboard_json.py` calls `.head(10)` on the result, so
the snapshot carries ten cells from a single family.

Emit the grid and render it as a heatmap: **6 categories × 18 keywords**, CTR as the heat value,
impressions as the support.

| Axis | Values |
|---|---|
| Category (`l1`) | Action&Adventure, Comedy, Crime&Thriller, Drama&Romance, SciFi&Fantasy, Other |
| Keyword (genre) | the eighteen MovieLens genres, Action through Western |

## Why a new key rather than undoing the truncations

The obvious reading of "stop truncating" is to raise the rank cap and drop `.head(10)` for all three
levels. That would be wrong for two of them. `l2` is the primary genre, so its grid is 18 × 18; `l3`
is genre·decade, roughly 180 values × 18 keywords. Shipping those in a committed JSON snapshot means
thousands of rows for tables that display ten.

So `compute_keyword` gains a dedicated `grid`: the full `(l1, keyword)` cross-tab, at most 108 rows,
with no rank cap. `tops` keeps its `rank <= 10` and the exporter keeps its `.head(10)` for the three
top-ten tables, which are unchanged and still display ten rows.

## What this does and does not claim

It claims the grid is complete for its axes. No rank cap and no head, so every
(category, keyword) pair the run observed appears. Pairs that never co-occur are absent from the
rows and render as empty cells rather than zeros — a genre that was never served under a category is
not the same as one served and never clicked.

It claims the snapshot is regenerated for this change. The committed one predates the grid, and the
simulation's Parquet is gone from `/tmp`, so the snapshot comes from a fresh
`run-movie-category-sim.sh`. That is a departure from every other dashboard change this session,
each of which left the snapshot alone; here the point of the change is data the snapshot does not
contain.

It does not claim the regenerated snapshot is comparable to the old one line by line. It is not
byte-reproducible — freshness ages shift with wall-clock, and a new simulation run produces different
sampled traffic — so the diff is large and the numbers move. Every figure on every page will change,
which is expected and is the cost of showing this at all.

It does not change the three top-ten tables, the two ranked tables, the divergence bars, or the
existing token heatmap. The section gains one visual.

It does not claim CTR is the only sensible heat value. Divergence would answer a different and also
useful question — over- versus under-supply per cell — but divergence is computed per keyword against
the global distribution, not per (category, keyword) pair, and inventing a per-cell divergence is a
measurement decision rather than a rendering one.

## Global constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Four commits: the computation, the export, the component, the regenerated snapshot. The snapshot is
  last and alone, so the diff that matters can be reviewed without it.
- `tops`, `by_keyword` and `by_subkeyword` keep their current shapes and truncations.
- `validate_measurements.mjs` is untouched: `keyword` appears in `DIAGNOSTIC_ROWS` with required
  fields on `by_keyword` and `by_subkeyword`, and the validator does not reject unknown keys.
- `MEASUREMENT_SCHEMA_VERSION` stays `"2.0"`. The grid is a diagnostic addition, not a measurement.
- Every section keeps its component, route, tile and id.
- The heatmap reuses the existing `.token-heatmap` / `--token-score` idiom rather than introducing a
  second heat treatment.
- `keyword-report.jsx` stays a client component; it already is, for its Top-K selector.

## Implementation

### Commit 1 -- the computation

`compute_keyword` gains `grid`, built from the same `lv` frame `top_keywords` uses: explode `genres`,
group by `(l1, genres)`, count impressions and clicks, derive `ctr`. Columns are named `category`,
`keyword`, `movie_impressions`, `query_clicks`, `ctr` — `category` rather than `l1`, because the
section's axis label is what a reader sees and `l1` means nothing to them.

No rank cap. The result is bounded by the genre vocabulary at 6 × 18.

### Commit 2 -- the export

`export_dashboard_json.py` adds `"grid": _records(kw["grid"])` to the keyword block, without
`.head()`. The three `tops` entries keep theirs.

### Commit 3 -- the component

`KeywordSection` gains a heatmap above the existing top-ten tables: categories down the left, keywords
across the top, each cell shaded by CTR relative to the grid's maximum and labelled with its value.
A pair with no observations renders as an empty cell, visibly distinct from a zero-CTR cell.

The cell shading reuses `--token-score` and the `.token-chip` background rule, so the section has one
heat scale rather than two.

When `data.grid` is absent — which is every snapshot exported before this change — the heatmap renders
a short note saying the grid needs a fresh export, rather than an empty table.

### Commit 4 -- the snapshot

`run-movie-category-sim.sh` regenerates `frontend/data/dashboard.json`. Committed on its own, so the
code review is not buried under a few thousand changed numbers.

## Validation and acceptance

1. `python3 -m pytest -q` from `recsys-pipeline`. The measured baseline is recorded in the plan; the
   change adds tests for the grid and for the absent-grid fallback.
2. `compute_keyword(df)["grid"]` over a frame with known genres produces one row per observed
   (category, keyword) pair, with `ctr = query_clicks / movie_impressions`.
3. The regenerated snapshot's `keyword.grid` has more than one distinct `category`.
4. `cd frontend && npm run validate:data` passes against the regenerated snapshot.
5. Against a running dev server, `/demand/keyword` renders a heatmap whose row count equals the
   snapshot's distinct categories.
6. The three top-ten tables still show ten rows each.
7. `cd frontend && npm run build` succeeds.
8. `git diff --check` clean.

## Limits

A regenerated snapshot invalidates every number anyone has looked at today. The dashboard's figures
are a property of the run that produced them, and this change replaces that run. Nothing carries over,
including the OPE section's fifteen policies, which will be whatever the new run's replay buffer holds
— probably nothing, since no backend will be running.

CTR per cell is a ratio over whatever impressions that cell received, and the cells are wildly
unequal: a category-keyword pair with nine impressions and three clicks reads as 33% next to a pair
with nine thousand. The support number is shown per cell for that reason, but a heatmap invites
comparison between cells that the underlying counts may not support.

The grid counts a row once per genre it carries, because `genres` is exploded. A film tagged Action,
Comedy and Romance contributes to three keyword columns under its own category. That is the same
convention `top_keywords` already uses, so the heatmap and the top-ten tables agree — but it means
column totals exceed the impression count, and the heatmap does not say so.
