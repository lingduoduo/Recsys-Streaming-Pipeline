# Spec: Analysis Dashboard (consolidated HTML report)

## Objective

A single self-contained HTML dashboard that presents the five existing analysis reports
(keyword / query / relevance / recall / ranking) for one simulation run as one narrative page,
so a reviewer can read the whole engagement-to-ranking story without opening five CSV folders.

Delivered as one standalone Python script (`analysis_dashboard_report.py`) that recomputes the
metrics from source and writes `index.html`. It is a **read-only presentation layer** — it does not
change the five existing report scripts, and it accepts that its metric logic duplicates theirs
(explicitly chosen tradeoff: full decoupling over DRY).

## Scope decisions (locked during brainstorming)

- **Format:** self-contained HTML with script-generated inline SVG charts (like the repo's existing
  `recsys-streaming-pipeline.html`). No CDN, no JS charting lib, opens offline. Linked from
  specs/plans/PRs rather than rendered inline in a PR diff.
- **Data layer:** plain `python` — pandas + pyarrow read the Parquet, `redis-py` reads Redis. **Not**
  PySpark: sim `training_samples` is small, and requiring `SPARK_HOME` to view a report would kill
  its use. Matches the existing `recall_eval_report.py` / `ranking_eval_report.py` idiom.
- **Standalone recompute:** the script re-reads Parquet + Redis and recomputes all five reports'
  metrics itself. No import of the five scripts; duplication is accepted.
- **Manual invocation:** run by hand, same as the five reports. Not auto-wired into the sim
  harnesses.

## Inputs / sources

- `--input <dir>` — a run's `training_samples` Parquet directory (same flag/semantics as the five
  reports), e.g. `/tmp/spark-recsys/movie-category-sim/training-samples`.
- Parquet columns used: `user_id`, `session_id`, `item_id`, `clicked`, engagement label
  (`0.0/1.0/2.0` = impression / click / order), `genres` when present.
- Redis (`REDIS_HOST`, default `localhost`): `movie:{id}:features`, `i2vEmb:*`, `uEmb:*` — needed
  only by the recall and ranking sections.

## Output

- `<input>/../report-dashboard/index.html` — one file, matching the existing `report-*` convention.

## Shared definitions (mirror the five reports)

- **query** = a recommended impression's genre-combo intent (`concat_ws(" ", genres)`); the
  clicked-genre framing follows the keyword/query report definitions.
- engagement label `0.0/1.0/2.0` = impression-only / clicked / ordered.
- **CTR** = clicks / impressions; **CVR** = orders / impressions.

These appear in a header note on the page, matching the sub-README's "Definitions shared across
reports" block.

## Internal structure (isolated, testable units)

Three layers so compute is testable without HTML and rendering is testable without Redis:

```
load_samples(input)          -> DataFrame     # parquet part-*.parquet glob → pandas
compute_relevance(df)        -> dict          # funnel counts, CTR/CVR, mean score by query & genre
compute_keyword(df)          -> dict          # movies-vs-queries distribution, top keywords l1/l2/l3
compute_query(df)            -> dict          # top queries, short (<=10) vs long (>10) engagement
compute_recall(df, redis)    -> dict | None   # BM25 / embedding / hybrid (RRF) recall@k, hitrate@k
compute_ranking(df, redis)   -> dict | None   # logloss + ROC-AUC per signal (popularity/position/emb)

svg_bar(series) / svg_line(series) / html_table(df)       # pure renderers, inline SVG + HTML
section(title, headline, body)                             # one card
render_html(sections)        -> str
main()                                                     # load → compute → render → write
```

Each `compute_*` returns plain data; each renderer is pure. `compute_recall` / `compute_ranking`
return `None` when their Redis prerequisites are absent.

## Report layout — funnel narrative (demand → expression → retrieval → ranking)

Five sections, each led by a headline number:

| # | Section | Headline metric | Lead chart | Table |
|---|---------|-----------------|-----------|-------|
| 1 | Engagement funnel | `impressions N · CTR x% · CVR y%` | SVG bar: impression → click → order | mean score by query & by genre |
| 2 | Keyword gap | biggest shown-vs-clicked divergence | grouped SVG bar: movies vs queries per keyword | top keywords l1/l2/l3 |
| 3 | Query intent | top query + short-vs-long CTR | SVG bar of top queries | short (≤10) vs long (>10) engagement |
| 4 | Recall | `hybrid recall@10 vs BM25 recall@10` | SVG line: recall@k, 3 methods | recall@k / hitrate@k |
| 5 | Ranking | best signal by AUC | SVG bar of AUC per signal | logloss + AUC per signal |

Charts are script-generated inline SVG with `<title>` hover tooltips (no JS dependency).

## Error handling

- **Missing / empty Parquet** → exit non-zero with a clear message (mirrors `recall_eval`'s guard).
- **No Redis corpus** (`movie:*:features` absent) → sections 4 & 5 render an explicit
  `N/A — no movie:*:features in Redis` card, never an empty table or a misleading 0.
- **Empty group** (e.g. no long queries) → skip the row, note "none," never divide-by-zero on
  CTR/CVR.

## Testing

- **Unit:** each `compute_*` against a tiny hand-built DataFrame (pure, no Spark/Redis); renderers
  against a fixed dict → assert on SVG/table substrings.
- **Integration:** `integration-tests/python_modeling/test_analysis_dashboard.py` (matching existing
  style) — write a small Parquet, seed fake Redis keys, run the script, assert `index.html` exists,
  contains each section's headline, and shows the `N/A` card when Redis is unseeded.
- Add to the `pytest -q` line in the sub-README Tests table.

## Non-goals

- No changes to the five existing report scripts.
- No auto-wiring into `run-*-sim.sh`.
- No interactive JS charts / CDN dependencies.
- No new metrics beyond what the five reports already compute.
