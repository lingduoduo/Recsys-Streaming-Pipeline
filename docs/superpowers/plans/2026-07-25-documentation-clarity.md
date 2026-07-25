# Documentation Clarity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the root README and all recommendation documentation runnable and unambiguous about working directories, prerequisites, completion signals, outputs, and failure recovery.

**Architecture:** Treat the root `README.md` as the canonical end-to-end local workflow. Keep architecture documents focused on component contracts and focused commands, and keep recommendation-flow documents focused on read/write dependencies while linking back to the canonical setup instead of duplicating it.

**Tech Stack:** GitHub-flavored Markdown, Bash command examples, Docker Compose, Kafka, Redis, Spark, Python/pandas, Next.js.

## Global Constraints

- Modify the repository-root `README.md` and Markdown files under `recsys-pipeline/docs/`.
- Do not broadly rewrite `frontend/README.md` or `recsys-pipeline/README.md`.
- Do not change runtime behavior or application code.
- Every runnable sequence must identify its working directory, prerequisites, process lifetime, success signal, and output.
- Do not instruct users to stop or delete an unrelated container without first identifying it.
- Distinguish the standalone HTML dashboard from the Next.js dashboard and its static JSON snapshot.
- Preserve advanced reference material that is unrelated to the operational clarification.
- Preserve the existing unrelated working-tree files `frontend/data/dashboard.json` and `recsys-pipeline/sampledata/item_embedding.txt`.

---

### Task 1: Establish the canonical root quick start

**Files:**
- Modify: `README.md:166-237`
- Modify: `README.md:333-368`

**Interfaces:**
- Consumes: Existing scripts `recsys-pipeline/run-movie-category-sim.sh`, `recsys-pipeline/run-streaming-job.sh`, and `frontend/export_dashboard_json.py`.
- Produces: The canonical local workflow and troubleshooting anchors referenced by architecture and flow documents.

- [ ] **Step 1: Replace the infrastructure and data-pipeline introduction with a numbered workflow**

In `README.md`, make each command block begin from the repository root and explicitly use:

```bash
cd recsys-pipeline
docker compose up -d zookeeper kafka redis
docker compose ps
```

State that Kafka and Redis must report healthy, that port `6379` must be free, and that `docker compose up` failure is not a Spark failure.

- [ ] **Step 2: Separate finite setup from long-running processes**

Document these behaviors immediately beside their commands:

- `producer.py` runs until `Ctrl-C`, defaults to one event per second, and logs every 100 events after the first.
- Spark streaming jobs wait for new Kafka records and therefore appear idle on an empty topic.
- `run-movie-category-sim.sh` is finite but can take several minutes; users must wait for its literal `==> done` line before exporting.

Include the bounded producer example:

```bash
EVENTS_PER_SECOND=20 LOG_EVERY=10 MAX_EVENTS=100 \
  python services/python-modeling/producer.py
```

- [ ] **Step 3: Add one complete movie-category-to-React-dashboard workflow**

Use one copy-pasteable sequence:

```bash
cd recsys-pipeline
./run-movie-category-sim.sh
# Wait for: ==> done

cd ..
REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input /tmp/spark-recsys/movie-category-sim/training-samples \
  --output frontend/data/dashboard.json

cd frontend
npm install
npm run dev
```

State that `http://localhost:3000` is the React dashboard, while `/tmp/spark-recsys/movie-category-sim/report-dashboard/index.html` is a separate standalone artifact.

- [ ] **Step 4: Add an evidence-oriented troubleshooting table**

Include exact rows for:

| Symptom | Diagnostic | Interpretation | Remedy |
|---|---|---|---|
| `Bind for 0.0.0.0:6379 failed` | `lsof -nP -iTCP:6379 -sTCP:LISTEN` and `docker ps --filter publish=6379` | another process/container owns the port | identify the owner; stop it only if safe, then rerun |
| `UnknownTopicOrPartitionException` | `docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic recsys_events` | missing/wrong topic | use the launcher bootstrap or create/produce to the configured topic |
| Producer appears stuck | inspect `rate=...` and `LOG_EVERY` banner | normal long-running producer | use `LOG_EVERY=1` or `MAX_EVENTS` |
| Dashboard exporter says no samples | `find /tmp/spark-recsys/movie-category-sim/training-samples -name '*.parquet'` | simulation not finished | wait for `==> done` |
| Keyword Gap is `unknown`; L1/L2/L3 empty | `docker compose exec -T redis redis-cli --scan --pattern 'movie:*:features' \| wc -l` | snapshot exported without movie metadata | keep Redis running and export from the movie-category path |
| Dashboard still shows old row count | inspect `input` and `rows` in `frontend/data/dashboard.json` | stale static snapshot/browser | rerun exporter, hard-refresh, or restart `npm run dev` |
| ONNX Gather index error | `GET /predict/metadata` | raw numeric ID exceeds lookup size | use string IDs or indices within metadata bounds |

- [ ] **Step 5: Verify the root README commands against the repository**

Run:

```bash
test -x recsys-pipeline/run-movie-category-sim.sh
test -f frontend/export_dashboard_json.py
test -f recsys-pipeline/services/python-modeling/producer.py
rg -n "movie-category-sim/training-samples|==> done|localhost:3000|report-dashboard/index.html" README.md
```

Expected: all `test` commands exit zero and each critical path appears in the README.

- [ ] **Step 6: Commit the canonical workflow**

```bash
git add README.md
git commit -m "docs: clarify local pipeline and dashboard workflow"
```

---

### Task 2: Make analysis-report documentation operationally complete

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md`

**Interfaces:**
- Consumes: The canonical root README workflow and the report generators.
- Produces: A focused reference for choosing, generating, and troubleshooting Spark CSV, standalone HTML, and React snapshot reports.

- [ ] **Step 1: Add a report-artifact decision table**

Document these distinct outputs:

| Desired artifact | Generator | Output |
|---|---|---|
| Spark analysis CSVs | `KeywordAnalysisReportJob`, `QueryAnalysisReportJob`, `RelevanceAnalysisReportJob` | sibling report directories |
| Standalone HTML | `analysis_dashboard_report.py` | `<input>/../report-dashboard/index.html` |
| React dashboard snapshot | `frontend/export_dashboard_json.py` | `frontend/data/dashboard.json` |

- [ ] **Step 2: Rewrite command blocks with explicit working directories and prerequisites**

Use:

```bash
cd recsys-pipeline
IN=/tmp/spark-recsys/movie-category-sim/training-samples
```

State that the Parquet path must contain files, Redis must still contain `movie:*:features` for populated genres/categories, and the simulation must have printed `==> done`.

- [ ] **Step 3: Document metadata enrichment and `unknown` semantics**

Explain:

- training samples may contain empty `genres`;
- the Python loader enriches empty genres from Redis `movie:{item_id}:features`;
- absent Redis metadata yields `unknown` keyword/query and empty exploded category tables;
- an empty input directory is rejected with `No training samples found`.

- [ ] **Step 4: Add React snapshot refresh and validation commands**

From the repository root:

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

Expected: input is the movie-category path, rows are positive, and L1 count is positive.

- [ ] **Step 5: Verify and commit**

Run:

```bash
rg -n "standalone|static JSON|movie:\\*:\\features|No training samples found|==> done" \
  recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md
git diff --check
```

Then commit:

```bash
git add recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md
git commit -m "docs: clarify analysis report inputs and outputs"
```

---

### Task 3: Clarify architecture API and data-pipeline operating contracts

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_architecture/API.md`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`

**Interfaces:**
- Consumes: Actual controller routes, environment variables, streaming job defaults, and Redis/Kafka key contracts.
- Produces: Accurate focused references linked by the recommendation-flow documents.

- [ ] **Step 1: Add API startup prerequisites and base URL**

At the top of `API.md`, state:

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn spring-boot:run
```

Required state: Java 17, Redis reachable at the configured host/port, server success line `Started RetrievalServiceApplication`, and base URL `http://localhost:8080`.

- [ ] **Step 2: Clarify string IDs versus raw ONNX indices**

For `/predict/{user}/{item}`, state that string IDs use the lookup tables and unknown values return `unknown_user_or_item`.

For `/predict/id`, state that IDs are zero-based internal lookup indices, not external movie IDs. Add:

```bash
curl -s http://localhost:8080/predict/metadata
```

Explain that `userId` must be `0..users-1` and `itemId` must be `0..items-1`; out-of-range values return HTTP 400.

- [ ] **Step 3: Add data-pipeline execution context and lifecycle notes**

At the beginning of the real-time path in `Data_Pipeline.md`, require:

```bash
cd recsys-pipeline
docker compose up -d zookeeper kafka redis
(cd services/spark-streaming-job && sbt assembly)
```

State that producer/streaming commands are long-running unless bounded, Kafka topics and environment-variable topic names must match, and the local launcher bootstraps the default UserEvent input topic.

- [ ] **Step 4: Add observable state checks to relevant job sections**

Add focused diagnostics:

```bash
docker compose exec -T kafka kafka-get-offsets \
  --bootstrap-server localhost:9092 --topic recsys_events

docker compose exec -T redis redis-cli ZCARD global:item_popularity
docker compose exec -T redis redis-cli --scan --pattern 'movie:*:features'
find /tmp/spark-recsys/training-samples -name '*.parquet'
```

Explain zero Kafka offsets as “no messages produced,” not a consumer crash.

- [ ] **Step 5: Audit documented environment variables against source**

Run:

```bash
rg -n 'sys\\.env|getOrElse\\("' recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo
rg -n 'KAFKA_|REDIS_|SPARK_|ONLINE_JOINER_' \
  recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
```

Compare spelling and defaults for every variable touched by this edit. Correct documentation rather than changing source.

- [ ] **Step 6: Verify and commit**

Run:

```bash
rg -n "zero-based|HTTP 400|kafka-get-offsets|long-running|working directory" \
  recsys-pipeline/docs/recommendation_architecture/API.md \
  recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
git diff --check
```

Then commit:

```bash
git add recsys-pipeline/docs/recommendation_architecture/API.md \
  recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
git commit -m "docs: clarify API and data pipeline contracts"
```

---

### Task 4: Standardize recommendation-flow dependencies and navigation

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_flows/1_Query_Hydration.md`
- Modify: `recsys-pipeline/docs/recommendation_flows/2_Fetch_Popular_Stuff.md`
- Modify: `recsys-pipeline/docs/recommendation_flows/3_Cold_Start.md`
- Modify: `recsys-pipeline/docs/recommendation_flows/4_Filtering.md`
- Modify: `recsys-pipeline/docs/recommendation_flows/5_Candidate_Hydration.md`
- Modify: `recsys-pipeline/docs/recommendation_flows/6_Predicting_Scoring.md`
- Modify: `recsys-pipeline/docs/recommendation_flows/7_Shuffling.md`
- Modify: `recsys-pipeline/docs/recommendation_flows/8_Store_Context.md`
- Modify: `recsys-pipeline/docs/recommendation_flows/9_Track_Metrics.md`

**Interfaces:**
- Consumes: `Data_Pipeline.md` for state creation and `API.md` for request contracts.
- Produces: A navigable nine-stage serving-flow guide with explicit state dependencies.

- [ ] **Step 1: Add consistent previous/next and reference navigation**

Add a compact navigation line to every flow document:

```markdown
**Flow:** [Previous](...) · **Current: ...** · [Next](...)
**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)
```

For stages 1 and 9, omit the nonexistent previous or next link.

- [ ] **Step 2: Add a “Required state” section to stages 1–5**

Document the concrete Redis/config dependencies:

- Stage 1: user/movie context hashes and sequence keys; identify the context collector as writer.
- Stage 2: `global:item_popularity`; identify `UserEventStreamingJob` as writer and note an empty sorted set yields no popularity candidates.
- Stage 3: catalog/config fallback state and when cold-start applies.
- Stage 4: filter configuration plus hydrated user relationships/preferences.
- Stage 5: item embeddings and feature hashes required for candidate enrichment.

Each section must state what happens when the state is absent.

- [ ] **Step 3: Add a “Required state” section to stages 6–9**

Document:

- Stage 6: ONNX artifacts, lookup tables, Redis embedding/reward signals, and numeric-index bounds.
- Stage 7: shuffle configuration and deterministic/non-deterministic implications.
- Stage 8: request/impression context writes and the downstream feedback dependency.
- Stage 9: metrics/replay keys, the feedback calls that populate them, and why a fresh Redis returns zeros/empty aggregates.

- [ ] **Step 4: Remove competing setup instructions**

Where a flow document implies that reading the page alone is enough to run the system, replace duplicated setup with:

```markdown
For the complete local startup sequence, follow the [root quick start](../../../README.md#recsys-pipeline).
```

Keep focused diagnostic commands only when they inspect that stage's state.

- [ ] **Step 5: Verify all flow documents contain dependencies and valid navigation**

Run:

```bash
for f in recsys-pipeline/docs/recommendation_flows/*.md; do
  rg -q "Required state" "$f" || { echo "missing Required state: $f"; exit 1; }
  rg -q "References:" "$f" || { echo "missing References: $f"; exit 1; }
done
```

Expected: no output and exit zero.

- [ ] **Step 6: Commit the flow documentation**

```bash
git add recsys-pipeline/docs/recommendation_flows
git commit -m "docs: clarify recommendation flow dependencies"
```

---

### Task 5: Validate the full documentation set

**Files:**
- Verify: `README.md`
- Verify: `recsys-pipeline/docs/recommendation_architecture/*.md`
- Verify: `recsys-pipeline/docs/recommendation_flows/*.md`

**Interfaces:**
- Consumes: Documentation produced by Tasks 1–4.
- Produces: Evidence that paths, scripts, links, terms, and commands are internally consistent.

- [ ] **Step 1: Check all referenced repository paths introduced by the edits**

Run:

```bash
test -f README.md
test -f frontend/export_dashboard_json.py
test -x recsys-pipeline/run-movie-category-sim.sh
test -x recsys-pipeline/run-streaming-job.sh
test -f recsys-pipeline/services/python-modeling/analysis_dashboard_report.py
test -d recsys-pipeline/docs/recommendation_architecture
test -d recsys-pipeline/docs/recommendation_flows
```

Expected: exit zero.

- [ ] **Step 2: Check relative Markdown links**

Run this read-only checker:

```bash
python - <<'PY'
from pathlib import Path
import re

files = [Path("README.md")]
files += sorted(Path("recsys-pipeline/docs/recommendation_architecture").glob("*.md"))
files += sorted(Path("recsys-pipeline/docs/recommendation_flows").glob("*.md"))
missing = []
for source in files:
    text = source.read_text()
    for raw in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
        target = raw.split("#", 1)[0].strip()
        if not target or "://" in target or target.startswith("mailto:"):
            continue
        resolved = (source.parent / target).resolve()
        if not resolved.exists():
            missing.append(f"{source} -> {target}")
if missing:
    raise SystemExit("\n".join(missing))
print("all local Markdown links resolve")
PY
```

Expected: `all local Markdown links resolve`.

- [ ] **Step 3: Search for conflicting dashboard paths and ambiguous commands**

Run:

```bash
rg -n "/tmp/spark-recsys/training-samples|export_dashboard_json|run-movie-category-sim|report-dashboard" \
  README.md recsys-pipeline/docs
rg -n "^\\s*(docker compose|python |mvn |sbt |\\./run-)" README.md recsys-pipeline/docs
```

Review every match. Dashboard examples intended to populate Keyword Gap must use the movie-category simulation path; focused generic references may retain `/tmp/spark-recsys/training-samples` when explicitly labeled generic.

- [ ] **Step 4: Check formatting and working-tree isolation**

Run:

```bash
git diff --check
git status --short
git diff --stat
```

Expected: no whitespace errors. Confirm the documentation commits do not include `frontend/data/dashboard.json` or `recsys-pipeline/sampledata/item_embedding.txt`.

- [ ] **Step 5: Review the rendered Markdown diff**

Read the complete diff and verify:

- the root quick start is the only complete end-to-end workflow;
- commands identify their starting directory;
- long-running processes and `==> done` are explicit;
- standalone HTML and React snapshot outputs cannot be confused;
- troubleshooting never destroys unidentified state;
- flow pages link to accurate state writers.

- [ ] **Step 6: Commit any validation-only documentation corrections**

If validation required corrections:

```bash
git add README.md recsys-pipeline/docs
git commit -m "docs: fix documentation validation findings"
```

If no corrections were required, do not create an empty commit.
