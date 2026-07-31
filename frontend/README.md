# Recsys Dashboard Frontend (Next.js)

A Next.js (app-router) rendering of the recsys **analysis dashboard** — the seven
recommendation measurement sections (relevance, satisfaction, freshness, diversity,
fairness, safety, latency) followed by the engagement / keyword / query / recall /
ranking / off-policy / MDP diagnostics from the Python `analysis_dashboard_report.py`,
as React components.

The app reads a static JSON snapshot at [`data/dashboard.json`](data/dashboard.json), so
`npm run dev` works out of the box without Redis or Spark. Regenerate that snapshot from a
real run with the export script.

## Run

```bash
cd frontend
npm install
npm run dev            # http://localhost:3000
```

`npm run build && npm run start` serves the production build (the page prerenders to static
HTML since the data is read at build time).

## Refresh the data

`export_dashboard_json.py` reuses the pure `compute_*` functions from
`recsys-pipeline/services/python-modeling/analysis_dashboard_report.py`. The diagnostic sections
match the Python HTML dashboard; the seven measurement sections are exported for this app only
(`analysis_dashboard_report.py`'s HTML report does not render them):

```bash
# from the repo root (Redis up; a run's training_samples Parquet available)

# optional inputs: live latency/freshness/safety/feedback coverage, and the ranked slates that
# relevance and diversity need. ExperienceCollectorStreamingJob writes slates to Parquet when
# EXPERIENCE_COLLECTOR_OUTPUT_PATH is set — run-movie-category-sim.sh sets it and captures both
# inputs automatically as part of its one-command run (see recsys-pipeline/README.md).
# Paths below are what run-movie-category-sim.sh writes; it also captures /metrics itself.
curl -s http://localhost:8080/metrics > /tmp/spark-recsys/movie-category-sim/live-metrics.json

REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input /tmp/spark-recsys/movie-category-sim/training-samples \
  --output frontend/data/dashboard.json \
  --experiences /tmp/spark-recsys/movie-category-sim/slates \
  --live-metrics /tmp/spark-recsys/movie-category-sim/live-metrics.json \
  --mdp-csv /tmp/spark-recsys/mdp_eval.csv   # optional; MovieLensPolicyEvaluation writes it
```

Measurement configuration flags (defaults shown): `--fairness-min-support 100`,
`--freshness-window-days 30`, `--long-tail-percentile 0.80`,
`--safety-policy-version catalog-filter-v1`.

Sections whose inputs are unavailable serialize with `"status": "unavailable"` and an explicit
warning, and render an **N/A** card; the diagnostic sections (recall, ranking, OPE, MDP)
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
│   ├── ui.jsx            # Section, NaCard, BarChart (SVG), DataTable
│   └── sections.jsx      # MeasurementSection + the measurement and diagnostic sections
├── data/dashboard.json   # committed snapshot (regenerate with export_dashboard_json.py)
├── validate_measurements.mjs  # data-contract gate for `npm run build`
└── export_dashboard_json.py
```
