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
`recsys-pipeline/services/python-modeling/analysis_dashboard_report.py`, so the React
dashboard shows exactly what the Python HTML dashboard would:

```bash
# from the repo root (Redis up; a run's training_samples Parquet available)
curl -s http://localhost:8080/metrics > /tmp/spark-recsys/live-metrics.json   # optional (latency)

REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input /tmp/spark-recsys/training-samples \
  --output frontend/data/dashboard.json \
  --experiences /tmp/spark-recsys/training-experiences \  # optional (relevance, diversity)
  --live-metrics /tmp/spark-recsys/live-metrics.json \    # optional (latency, live rows)
  --mdp-csv /tmp/spark-recsys/mdp_eval.csv                # optional (MDP card)
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
