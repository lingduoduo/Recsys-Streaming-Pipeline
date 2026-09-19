# Recsys Dashboard Frontend (Next.js)

A Next.js (app-router) rendering of the recsys **analysis dashboard** — the seven
recommendation measurement sections (relevance, satisfaction, freshness, diversity,
fairness, safety, latency) followed by the engagement / keyword / query / recall /
ranking / off-policy diagnostics, computed by the Python
`analysis_dashboard_report.py` and rendered here as React components.

The app reads a static JSON snapshot at [`data/dashboard.json`](data/dashboard.json), so
`npm run dev` works out of the box without Redis or Spark. Regenerate that snapshot from a
real run with the export script.

## Routes

A sidebar lists every destination, grouped; each page shows exactly one section under an eyebrow,
a title and a description.

| Route | Sidebar group | Shows |
|---|---|---|
| `/` | — | The scorecard: one tile per section, grouped, each linking to that section's page |
| `/demand/<section>` | Demand & content | What users asked for and what the catalog offers: intents, keyword gap |
| `/serving/<section>` | Serving & outcomes | What was served and what followed: engagement funnel, satisfaction, freshness, diversity, fairness, safety, latency |
| `/models/<section>` | Model evaluation | How the models score when re-run: candidate recall, ranking quality, relevance, off-policy evaluation |

All thirteen come from one file, `app/[group]/[section]/page.jsx`, with `generateStaticParams`
enumerating the catalogue in `components/groups.js`. That catalogue is pure data with no imports,
because the sidebar is a client component; `components/section-registry.jsx` holds the
key-to-component map and is imported only by the server page.

The groups say what question a section answers, not where its data came from. On that second
question: only `latency` is purely live telemetry, and satisfaction, freshness and safety are offline
rows with live ones merged in when a backend was running.

## Run

```bash
# from the repository root
cd recsys-pipeline/frontend
npm install
npm run dev            # http://localhost:3000
```

`npm run build && npm run start` serves the production build (the page prerenders to static
HTML since the data is read at build time).

## Refresh the data

`export_dashboard_json.py` reuses the pure `compute_*` functions from
`recsys-pipeline/services/python-modeling/analysis_dashboard_report.py`, which computes the
metrics and renders nothing. This app is the only surface that renders them:

```bash
# from recsys-pipeline/ (Redis up; a run's training_samples Parquet available)

# optional inputs: live latency/freshness/safety/feedback coverage, and the ranked slates that
# relevance and diversity need. ExperienceCollectorStreamingJob writes slates to Parquet when
# EXPERIENCE_COLLECTOR_OUTPUT_PATH is set — run-movie-category-sim.sh sets it and captures both
# inputs automatically as part of its one-command run (see recsys-pipeline/README.md).
# Paths below are what run-movie-category-sim.sh writes; it also captures /metrics itself.
curl -s http://localhost:8080/api/v1/retrieval/metrics > /tmp/spark-recsys/movie-category-sim/live-metrics.json

REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input /tmp/spark-recsys/movie-category-sim/training-samples \
  --output frontend/data/dashboard.json \
  --experiences /tmp/spark-recsys/movie-category-sim/slates \
  --live-metrics /tmp/spark-recsys/movie-category-sim/live-metrics.json
```

The off-policy section reads one of two sources. Without `--ope-parquet` it reads the
`replay:recommendations` Redis list, which the serving path writes and this repository does not, so
it is N/A unless a backend has been running. With `--ope-parquet` it reads the scored replay that
`post_train_dpo.py` or `post_train_q.py --output-parquet` writes, which is the only source carrying
the post-training arms (`dpoScore`, `tabQ`, `fqiQ`, `grpoScore`). The section names whichever source
it used.

On a default run it is N/A: nothing in this repository writes `replay:recommendations`, and slate and
replay `requestId`s only join when the backend runs with `RECSYS_GRPO_EMIT_EVENTS=true`. See
[Populating the off-policy section](../docs/recommendation_architecture/Analysis_Report.md#populating-the-off-policy-section)
for the three steps that fill it.

The metrics URL above uses the service's versioned retrieval prefix; see the
[repository boundary](../../README.md#repository-boundary) for how `SERVICE_URL` and
`RETRIEVAL_BASE` compose it.

Measurement configuration flags (defaults shown): `--fairness-min-support 100`,
`--freshness-window-days 30`, `--long-tail-percentile 0.80`,
`--safety-policy-version catalog-filter-v1`.

The keyword report selects from the 50 most-shown keywords, so `by_keyword` and
`by_subkeyword` are exported 50 rows deep while the other diagnostic tables stay at 10.

Sections whose inputs are unavailable serialize with `"status": "unavailable"` and an explicit
warning, and render an **N/A** card; the diagnostic sections (recall, ranking, OPE)
serialize as `null` and do the same. Nothing is zero-filled.

## Validate the snapshot

```bash
npm run validate:data     # also runs automatically as part of `npm run build`
```

The validator asserts `schemaVersion` is `2.0`, that all seven measurement sections are
present with a valid `status`, that available sections report `sampleSize` and `coverage`,
and that unavailable sections explain why.

## Layout

```
frontend/
├── app/
│   ├── layout.jsx        # root layout + metadata
│   ├── page.jsx          # server component: imports data/dashboard.json, renders sections
│   └── globals.css       # design tokens (shared look with the Python dashboard)
├── components/
│   ├── ui.jsx            # Section, NaCard, MetricTile, MetricGrid/MetricCard,
│   │                     #   ChartGrid, BarChart, GroupedBarChart, DataTable
│   ├── groups.js         # SECTION_ROUTE: which route renders each section
│   ├── format.js         # num / pct / share / ci / count / rankBy
│   ├── scorecard.jsx     # the overview tiles, linked through SECTION_ROUTE
│   ├── measurements.jsx  # the seven measurement sections
│   ├── diagnostics.jsx   # engagement / query / recall / ranking / off-policy
│   ├── nav.jsx           # route nav (the only client component)
│   └── keyword-report.jsx # "use client": the Top-K keyword report
├── data/dashboard.json   # committed snapshot (regenerate with export_dashboard_json.py)
├── validate_measurements.mjs  # data-contract gate for `npm run build`
└── export_dashboard_json.py
```
