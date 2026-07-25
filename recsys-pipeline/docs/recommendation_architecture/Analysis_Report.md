# Analysis Reports

This is a focused reference for choosing, generating, and troubleshooting analysis
artifacts. For the canonical end-to-end setup, simulation commands, and service startup
instructions, see the root [README](../../../README.md#1a-movie-category-simulation--react-dashboard).
Do not duplicate that workflow here.

## Choose the artifact

| Desired artifact | Generator | Output |
|---|---|---|
| Spark analysis CSVs | `KeywordAnalysisReportJob`, `QueryAnalysisReportJob`, `RelevanceAnalysisReportJob` | sibling report directories |
| Standalone HTML | `analysis_dashboard_report.py` | `<input>/../report-dashboard/index.html` |
| React dashboard snapshot | `frontend/export_dashboard_json.py` | `frontend/data/dashboard.json` |

The Spark outputs are CSV directories beside the input: `report-keywords`,
`report-queries`, and `report-relevance`. The standalone HTML is self-contained and can
be opened directly in a browser. The React dashboard instead consumes a committed
static JSON snapshot; start the React app using the root README after refreshing that
file.

## Prerequisites and inputs

Run these report commands from the repository's `recsys-pipeline/` directory:

```bash
cd recsys-pipeline
IN=/tmp/spark-recsys/movie-category-sim/training-samples
```

The input must be a populated `training_samples` Parquet directory, not merely an
existing directory. Run the movie-category simulation to completion first and wait for
its literal `==> done` line. Keep Redis running: populated genre/category breakdowns
require `movie:*:features` to still be available in Redis.

Training samples can contain an empty `genres` array. The Python loader enriches those
rows from Redis key `movie:{item_id}:features`. If that metadata is absent, keyword and
query values become `unknown`, and exploded category tables are empty. An empty input
directory is rejected with `No training samples found`.

## Generate Spark CSV reports

Build the Spark assembly first (`cd services/spark-streaming-job && sbt assembly`),
then return to `recsys-pipeline/` and run:

```bash
cd recsys-pipeline
IN=/tmp/spark-recsys/movie-category-sim/training-samples

SPARK_MAIN_CLASS=com.demo.report.KeywordAnalysisReportJob \
  KEYWORD_ANALYSIS_INPUT_PATH="$IN" ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.report.QueryAnalysisReportJob \
  QUERY_ANALYSIS_INPUT_PATH="$IN" ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.report.RelevanceAnalysisReportJob \
  RELEVANCE_ANALYSIS_INPUT_PATH="$IN" ./run-streaming-job.sh
```

These jobs produce the keyword/subkeyword, query, and relevance CSVs in their sibling
report directories. They use the Parquet `genres` column; the Redis enrichment described
above applies to the Python dashboard and React snapshot loaders.

## Generate the standalone HTML dashboard

From `recsys-pipeline/`, with the same completed simulation data and Redis available:

```bash
cd recsys-pipeline
IN=/tmp/spark-recsys/movie-category-sim/training-samples
REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py \
  --input "$IN"
```

The report is written to `<input>/../report-dashboard/index.html`. Its recall/ranking
sections additionally require Redis movie metadata and embeddings; unavailable inputs
render as explicit N/A cards.

## Refresh and validate the React snapshot

Run the exporter from the repository root (one directory above `recsys-pipeline/`):

```bash
REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input /tmp/spark-recsys/movie-category-sim/training-samples \
  --output frontend/data/dashboard.json

python - <<'PY'
import json
data = json.load(open("frontend/data/dashboard.json"))
print(data["input"], data["rows"], len(data["keyword"]["tops"]["l1"]))
PY
```

Validation succeeds when `input` is the movie-category path, `rows` is positive, and
the L1 count is positive. If the snapshot reports `unknown` values or zero L1 rows,
confirm the simulation reached `==> done`, the Parquet input contains files, and Redis
still holds `movie:*:features`.

For detailed definitions and the full operational workflow, use the root README rather
than treating this page as a second setup guide.
