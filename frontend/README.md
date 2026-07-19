# Recsys Dashboard Frontend (Next.js)

A Next.js (app-router) rendering of the recsys **analysis dashboard** — the same
engagement / keyword / query / recall / ranking / off-policy / MDP sections as the
Python `analysis_dashboard_report.py`, as React components.

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
REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input /tmp/spark-recsys/training-samples \
  --output frontend/data/dashboard.json \
  --mdp-csv /tmp/spark-recsys/mdp_eval.csv      # optional (MDP card)
```

Sections whose inputs are unavailable (e.g. no `movie:*:features` corpus in Redis for recall,
or no `mdp_eval.csv`) serialize as `null` and render an explicit **N/A** card — matching the
Python dashboard's behavior.

## Layout

```
frontend/
├── app/
│   ├── layout.jsx        # root layout + metadata
│   ├── page.jsx          # server component: imports data/dashboard.json, renders sections
│   └── globals.css       # design tokens (shared look with the Python dashboard)
├── components/
│   ├── ui.jsx            # Section, NaCard, BarChart (SVG), DataTable
│   └── sections.jsx      # the 7 dashboard sections
├── data/dashboard.json   # committed snapshot (regenerate with export_dashboard_json.py)
└── export_dashboard_json.py
```
