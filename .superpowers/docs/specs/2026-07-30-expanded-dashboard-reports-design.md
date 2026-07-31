# Expanded dashboard reports — design

Date: 2026-07-30
Status: approved

## Problem

A sample implementation of an expanded dashboard UI was appended to the end of
`frontend/components/sections.jsx` and `frontend/components/ui.jsx` rather than merged into
them. Both files now declare `Section`, `NaCard`, `BarChart`, `DataTable`, `RelevanceSection`,
`KeywordSection`, `QuerySection`, `RecallSection`, `RankingSection`, `OpeSection` and
`MdpSection` twice, with a `"use client"` directive in the middle of each file. The app does
not build.

The sample carries features worth keeping: KPI summary cards, a color-token keyword
visualization, a Top-K selector, a keyword detail panel, query and genre comparison reports,
CTR/CVR charts, ranking-quality summaries, recall comparison charts, OPE lift visualization,
and reusable report layouts.

Two conflicts must be resolved before it can be merged.

**Naming.** The sample's `RelevanceSection` renders the engagement funnel — it is the existing
`EngagementSection` under a different name. The existing `RelevanceSection` is the NDCG/MRR
measurement. Adopting the sample verbatim would delete NDCG/MRR and render two funnels. The
sample also omits the Scorecard and the Satisfaction, Freshness, Diversity, Fairness, Safety
and Latency sections entirely.

**Missing data.** The sample reads roughly fifteen fields that `data/dashboard.json` does not
publish. The consequences are not cosmetic:

- `by_keyword` has no `mean_score`, so the Top-K panel's sort key is `undefined` for every row.
  "Top 20 by relevance" is really "the first 20 in file order", and the heatmap colors every
  chip identically because `score ?? 0` is constant.
- `engagement.by_query` and `by_genre` have no `clicks` or `orders`, so every CTR and CVR
  column in those tables would read as zero.
- `KeywordDetailPanel` synthesizes per-token scores as
  `keyword.score * (0.75 + 0.25 * ((index + 1) / arr.length))` and labels the result "Token
  relevance". These numbers do not come from the pipeline.

The last of these contradicts a standing property of this dashboard: the README states
"Nothing is zero-filled", `MeasurementSection` documents that it "Never invents a value for
N/A", and the three most recent commits on `master` are fixes for fabricated values.

## Approach

Merge the sample into the existing structure. Every section survives, every displayed number
is measured, and the visual treatment is applied uniformly across the whole page.

Three decisions frame the work:

1. **Fields the snapshot lacks are computed for real** in `export_dashboard_json.py`, from
   columns the dataframe already carries. Fields that are not derivable are dropped along with
   the visuals that depended on them.
2. **One chart engine.** The sample's CSS bar charts replace the SVG ones, and
   `GroupedBarChart` is ported to match. The dashboard loses its visual parity with
   `analysis_dashboard_report.py`'s `svg_bar`, in exchange for fluid widths that work inside a
   two-up report grid.
3. **One visual language.** The seven measurement sections adopt the KPI card grid and report
   layouts alongside the diagnostics. Their measurement semantics are unchanged.

## Data contract

`schemaVersion` stays `2.0`. It is emitted from `MEASUREMENT_SCHEMA_VERSION` in
`analysis_dashboard_report.py`, which versions the seven **measurement** sections; those are
not changed here. Bumping it would signal a measurement-contract change that did not happen
and would falsify the assertions in `test_dashboard_measurement_contract.py`. The new
diagnostic fields are asserted directly by `validate_measurements.mjs` instead.

### Added

| Section | Fields | Derivation |
| --- | --- | --- |
| `engagement.by_query`, `engagement.by_genre` | `clicks`, `orders`, `ctr`, `cvr` | counts over `label` thresholds, matching `compute_query` |
| `keyword.by_keyword`, `keyword.by_subkeyword` | `mean_score`, `query_orders`, `ctr`, `cvr` | `mean_score` is the mean model score over impressions carrying the keyword |
| `query` | `average_query_length` | impression-weighted mean of `query_len` |
| `query.by_length` | `queries` | distinct query count per bucket |
| `ranking.rows` | `positive_rate` | `positives / n`, null when `n` is 0 |

`keyword.mean_score` is the quantity the color-token heatmap encodes and the Top-K selector
sorts on. Without it neither feature conveys anything. It is the mean `label` over the
keyword's impressions — the same definition `compute_relevance` already publishes as
`mean_score` for queries and genres.

### Rate definitions the UI must not redefine

`_rates` in `analysis_dashboard_report.py` computes `cvr` as `orders / impressions`, not
`orders / clicks`. The sampled UI recomputes CVR as `orders / clicks`, which would put two
different quantities under one column name on the same page. The UI renders the `ctr` and
`cvr` the exporter publishes and never derives them client-side.

### Export depth

`by_keyword` and `by_subkeyword` are currently exported with `.head(10)`. A Top-K selector
offering 10/20/30/50 over ten rows is inert, so both go to `.head(50)`. Because `dist()`
sorts by `movie_impressions`, the pool is the fifty most-shown keywords and the Top-K sort by
`mean_score` happens within it; the section states this in fine print rather than implying a
catalog-wide ranking. The selector offers only options that do not exceed the rows available,
plus an "All" option.

### Not added

- **`recall.coverage_at_k`.** This would be a new catalog-coverage metric, not a re-slice of
  data already computed. The recall table keeps `instances`; the recall comparison charts use
  `recall_at_k` and `hitrate_at_k`, which exist.
- **`keyword.tokens`, `keyword.token_scores`, `keyword.related_movies`.** Nothing in the
  pipeline produces these. The detail panel's synthesized "Token relevance" heatmap and its
  "Top matched movies" table are removed rather than fed invented values.

### Click definition to verify

`compute_keyword` counts clicks as `clicked == 1`; `compute_query` uses `label >= 1`. If those
predicates select different rows — that is, if an order does not also set `clicked` — then a
keyword CTR derived from `query_clicks` would undercount relative to the query CTR shown
elsewhere on the same page.

The first implementation step checks both predicates against the real Parquet. If they agree,
nothing changes. If they diverge, the new keyword CTR uses `label >= 1`, and the resulting
change to the already-published `query_clicks` values is called out explicitly in the commit
rather than left silent.

## Component architecture

```
frontend/components/
├── ui.jsx              # server-safe primitives; one chart engine, one table
├── sections.jsx        # Scorecard + thirteen sections (server components)
└── keyword-report.jsx  # "use client": Select, TokenHeatmap, KeywordSection
```

Only the keyword report needs `useState`, `useId` and `onClick`. Isolating it keeps `page.jsx`
a server component, so the rest of the page still prerenders to static HTML from the
build-time JSON import.

`ui.jsx` after the merge, each name defined once:

- `Section({ title, headline, description, actions, id, children })` — merged from both
  variants. Retaining `id` is required: the Scorecard tiles are anchors to `#relevance`,
  `#safety` and so on, and the sample's `Section` dropped it.
- `NaCard({ title, reason, id })` and `MetricTile` — unchanged. `MetricTile`'s `ok`/`low`/`na`
  status contract, including the screen-reader string that carries the low-coverage flag
  non-visually, stays exactly as it is.
- `MetricGrid`, `MetricCard`, `ReportGrid` — from the sample.
- `BarChart({ labels, values, title, horizontal, percentage, valueFormatter })` — the sample's
  CSS bars.
- `GroupedBarChart` — ported from SVG to the same CSS grammar so Relevance keeps its
  ndcg-versus-mrr comparison.
- `DataTable({ rows, columns, formatters, compact })` — the sample's signature, retaining the
  existing `formatCell` fallback. The sample's `String(value)` renders Safety's
  `reason_counts` object as `[object Object]`; the current code JSON-stringifies it.

### Helper correction

The sample's `sortDesc` coerces missing values with `Number(b[field] ?? 0)`, so a row whose
field is `null` sorts as zero rather than being excluded. Ranking rows legitimately carry a
null `auc`, so a "Best AUC" card could name a signal that has no AUC. `sortDesc` drops
null and undefined rows instead.

## Sections

`page.jsx` renders the same fifteen entries in the same order. Nothing is renamed away.

### Measurement sections

Relevance, Satisfaction, Freshness, Diversity, Fairness, Safety and Latency keep every
semantic they have today: the headline, the `sample size · coverage · window` fine print, the
warnings list, the N/A path, and every caveat paragraph verbatim. `HEADLINES`, `maxByField`,
`headlineFieldPublished` and `LOW_COVERAGE` are untouched.

What changes: `kpi-row` becomes `MetricGrid` of `MetricCard`, each section gains a
`description`, and a second chart is added where an honest one exists.

| Section | Charts |
| --- | --- |
| Relevance | grouped ndcg-vs-mrr by cutoff; recall@k by cutoff |
| Satisfaction | optional-signal coverage; CTR and order rate |
| Freshness | CTR by content age; mean reward by content age |
| Diversity | single chart, full width — no honest second view |
| Fairness | CTR by group and NDCG by group, both for the widest-gap dimension |
| Safety | single chart: filter decisions by reason |
| Latency | p95 by stage; p50/p95/p99 by endpoint |

### Diagnostic sections

- **Engagement** absorbs the sample's misnamed `RelevanceSection`. KPI cards read the
  `engagement.ctr` and `engagement.cvr` already published in the snapshot rather than
  recomputing them from the funnel. Funnel and top-queries charts; the by_query and by_genre
  tables carry real clicks, orders, CTR and CVR.
- **Keyword** (client component) — Top-K selector over 10/20/30/50, color-token heatmap keyed
  on `mean_score`, and a detail panel showing rank, score, impressions, clicks, CTR and
  divergence, plus a `movie_share` versus `query_share` comparison, which is what `divergence`
  measures. Impressions and divergence charts, the full keyword table, and the l1/l2/l3
  taxonomy tables.
- **Query** — CTR and CVR cards and charts, top-queries and by-length tables.
- **Recall** — best recall@k and hitrate@k cards, two comparison charts, table keeping
  `instances`.
- **Ranking** — best AUC, lowest log loss, best coverage and signal-count cards; AUC and
  coverage charts; table with `positive_rate`.
- **OPE** — best policy, highest lift, and reward-model AUC and MSE cards; value and lift
  charts; the confidence-interval table and its caveat.
- **MDP** — written to the same shape as the others. It renders `NaCard` against the current
  snapshot, where `mdp` is `null`.

## Styling

Roughly thirty new class names are added to `app/globals.css`, reusing the existing custom
properties: `report-section`, `section-header`, `section-content`, `section-description`,
`section-actions`, `headline-chip`, `metric-grid`, `metric-card`, `metric-detail`,
`metric-trend`, `chart-card`, `bar-chart`, `bar-row`, `bar-label`, `bar-track`, `bar-fill`,
`bar-value`, `table-wrapper`, `data-table`, `token-heatmap`, `token-chip`, `token-legend`,
`token-legend-gradient`, `keyword-report-layout`, `keyword-main-panel`,
`keyword-detail-panel`, `keyword-detail-title`, `keyword-detail-metrics`,
`keyword-detail-subtitle`, `report-subtitle`, `na-card`, `empty-state`, `select-control`,
`muted`.

Two collisions with existing rules must be avoided:

- `.metric-value` and `.metric-label` already style `MetricTile` on the Scorecard. The card
  styles are scoped under `.metric-card` so the tiles keep their own type scale.
- `.report-grid` is already the page-level section wrapper in `page.jsx`. The new two-up chart
  layout emits `.chart-grid` instead, so `ReportGrid` does not restyle the whole page.

The heatmap's `--token-score` needs a sequential color ramp that stays legible in light and
dark and does not rely on hue alone.

## Verification

1. `npm run build` compiles. It currently fails on the duplicate exports, so this is a real
   gate rather than a formality.
2. Contract tests for each added field, against a synthetic frame, written failing first and
   added to the existing `test_dashboard_measurement_contract.py` and
   `test_analysis_dashboard.py`.
3. `validate_measurements.mjs` extended to assert the presence of the new diagnostic fields;
   `npm run validate:data` passes. `schemaVersion` remains `2.0`.
4. `data/dashboard.json` regenerated from the run data on disk at
   `/tmp/spark-recsys/movie-category-sim` with Redis running, then diffed against the
   committed snapshot. Only the new keys may appear. Any movement in an existing number is
   investigated before the snapshot is committed.
5. The page loads with no section that previously rendered a value now rendering N/A.

Work proceeds on a branch and lands through a pull request.
