# Movie-Category Embedding Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Train Item2Vec and user embeddings with the same `movie_*` and `user_*` identifiers as the movie-category simulation, while rendering unavailable Recall embedding metrics honestly.

**Architecture:** The producer exports ratings from simulated feedback. The simulation passes those ratings through the existing Spark Item2Vec and user-embedding runners, publishes matching Redis vectors, and regenerates the dashboard; Recall carries explicit availability metadata for truthful fallback rendering.

**Tech Stack:** Python 3, pytest, Bash, Spark 3.5/Scala 2.12, Redis

## Global Constraints

- Reuse existing Spark embedding jobs; do not create or remap vectors.
- Keep event schemas, category effects, metric formulas, and Redis prefixes unchanged.
- Keep generated artifacts under `SIM_ROOT`.
- Support `GENERATE_EMBEDDINGS=false`.

---

### Task 1: Export simulation ratings

**Files:**
- Modify: `services/python-modeling/movie_segment_producer.py`
- Test: `integration-tests/python_modeling/test_movie_category_sim.py`

**Interfaces:**
- Produces: `ratings_from_events(events: list[dict]) -> list[dict]`.
- Produces: optional `RATINGS_OUTPUT_PATH` CSV with `userId,movieId,rating,timestamp`.

- [ ] **Step 1: Write failing tests**

Test that impressions are excluded, clicks map to `4.0`, orders map to `5.0`, an order supersedes its click for the same pair, timestamps convert from milliseconds to seconds, and IDs remain `user_*/movie_*`. Patch the producer in a one-slate `main` test and assert the optional CSV header and row.

- [ ] **Step 2: Verify RED**

Run: `python -m pytest integration-tests/python_modeling/test_movie_category_sim.py -k ratings -v`

Expected: FAIL because ratings extraction/export does not exist.

- [ ] **Step 3: Implement extraction**

```python
RATINGS_OUTPUT_PATH = os.getenv("RATINGS_OUTPUT_PATH")

def ratings_from_events(events: list[dict]) -> list[dict]:
    by_pair = {}
    for event in events:
        if event["event_type"] not in ("click", "order"):
            continue
        pair = (event["user_id"], event["item_id"])
        row = {
            "userId": event["user_id"],
            "movieId": event["item_id"],
            "rating": 5.0 if event["event_type"] == "order" else 4.0,
            "timestamp": int(event["timestamp_ms"] // 1000),
        }
        if pair not in by_pair or row["rating"] > by_pair[pair]["rating"]:
            by_pair[pair] = row
    return list(by_pair.values())
```

When the environment path is set, create its parent, use `csv.DictWriter` with the exact header, and write extracted rows for each slate before sending events.

- [ ] **Step 4: Verify GREEN**

Run: `python -m pytest integration-tests/python_modeling/test_movie_category_sim.py -q`

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/python-modeling/movie_segment_producer.py integration-tests/python_modeling/test_movie_category_sim.py
git commit -m "feat(sim): export matching movie ratings"
```

---

### Task 2: Orchestrate existing embedding jobs

**Files:**
- Modify: `run-movie-category-sim.sh`
- Test: `integration-tests/test_service_scripts.py`

**Interfaces:**
- Consumes: `$SIM_ROOT/ratings.csv`.
- Produces: item/user embedding artifacts and Redis `i2vEmb:movie_*`, `uEmb:user_*`.

- [ ] **Step 1: Write a failing contract test**

Read the script and assert it contains `RATINGS_OUTPUT_PATH`, `GENERATE_EMBEDDINGS`, `ITEM2VEC_SAVE_TO_REDIS=true`, `ITEM2VEC_QUERY_ITEM=movie_1`, both existing embedding runner names, `USER_EMBEDDING_SAVE_TO_REDIS=true`, and `analysis_dashboard_report.py`.

- [ ] **Step 2: Verify RED**

Run: `python -m pytest integration-tests/test_service_scripts.py::test_movie_category_sim_trains_matching_embeddings -v`

Expected: FAIL on the first absent contract.

- [ ] **Step 3: Implement orchestration**

Define simulation-local ratings, item-vector, user-vector, and dashboard paths plus:

```bash
GENERATE_EMBEDDINGS="${GENERATE_EMBEDDINGS:-true}"
```

Pass the ratings path to the producer. If enabled, require at least one CSV data row and run:

```bash
RATINGS_INPUT_PATH="$RATINGS_PATH" \
ITEM2VEC_EMBEDDING_PATH="$ITEM_EMBEDDING_PATH" \
ITEM2VEC_QUERY_ITEM=movie_1 ITEM2VEC_SAVE_TO_REDIS=true \
REDIS_HOST=localhost ./run-offline-pipeline.sh

RATINGS_INPUT_PATH="$RATINGS_PATH" \
ITEM2VEC_EMBEDDING_PATH="$ITEM_EMBEDDING_PATH" \
USER_EMBEDDING_OUTPUT_PATH="$USER_EMBEDDING_PATH" \
USER_EMBEDDING_SAVE_TO_REDIS=true \
REDIS_HOST=localhost ./run-user-embedding-pipeline.sh
```

Fail clearly unless Redis contains nonzero matching item and user keys. Generate `$SIM_ROOT/report-dashboard/index.html` afterward and print its path plus `http://localhost:8000`.

- [ ] **Step 4: Verify GREEN**

```bash
python -m pytest integration-tests/test_service_scripts.py::test_movie_category_sim_trains_matching_embeddings -v
bash -n run-movie-category-sim.sh
```

Expected: test PASS and syntax exit 0.

- [ ] **Step 5: Commit**

```bash
git add run-movie-category-sim.sh integration-tests/test_service_scripts.py
git commit -m "feat(sim): train matching Redis embeddings"
```

---

### Task 3: Render unavailable Recall vectors honestly

**Files:**
- Modify: `services/python-modeling/analysis_dashboard_report.py`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Extends `compute_recall` result with `embedding_available: bool` and `embedding_coverage: float`.
- Updates `_recall_section(r) -> str`.

- [ ] **Step 1: Write failing fallback tests**

With a corpus and no vectors, assert availability false and coverage zero. Assert rendered HTML says `Embedding unavailable`, displays embedding cells as `N/A`, labels Hybrid as `Hybrid (BM25 only)`, and omits the embedding chart series. With matching vectors, assert availability true and numeric embedding metrics remain.

- [ ] **Step 2: Verify RED**

Run: `python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -k "recall and embedding" -v`

Expected: FAIL because availability metadata and labels are absent.

- [ ] **Step 3: Implement availability rendering**

Coverage is the fraction of corpus IDs found in `vecs`. If zero, keep raw evaluator formulas unchanged but create display rows with embedding recall/hitrate `N/A`, rename Hybrid to `Hybrid (BM25 only)`, omit embedding from the chart, and add an unavailable insight. Otherwise render all methods normally.

- [ ] **Step 4: Verify GREEN**

Run: `python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q`

Expected: all dashboard tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "fix(dashboard): distinguish unavailable recall embeddings"
```

---

### Task 4: Document and verify end to end

**Files:**
- Modify: `README.md`
- Generated only: `/tmp/spark-recsys/movie-category-sim/**`

- [ ] **Step 1: Document the default embedding-enabled run**

Document:

```bash
NUM_ITEMS=100 NUM_SLATES=1000 ./run-movie-category-sim.sh
python -m http.server 8000 --directory /tmp/spark-recsys/movie-category-sim/report-dashboard
```

Also document `GENERATE_EMBEDDINGS=false`.

- [ ] **Step 2: Run focused verification**

```bash
python -m pytest integration-tests/python_modeling/test_movie_category_sim.py \
  integration-tests/python_modeling/test_analysis_dashboard.py \
  integration-tests/test_service_scripts.py -q
bash -n run-movie-category-sim.sh
git diff --check
```

Expected: all selected tests PASS; syntax and diff checks are clean.

- [ ] **Step 3: Run reduced end-to-end simulation**

Run: `NUM_ITEMS=40 NUM_USERS=30 NUM_SLATES=600 ./run-movie-category-sim.sh`

Expected: exit 0 with Parquet, category CSVs, ratings, item/user vectors, Redis keys, and dashboard HTML.

- [ ] **Step 4: Verify matching keys and dashboard**

```bash
docker compose exec -T redis redis-cli --scan --pattern 'i2vEmb:movie_*' | wc -l
docker compose exec -T redis redis-cli --scan --pattern 'uEmb:user_*' | wc -l
rg -n "embedding|hybrid|Keyword gap|Ranking" /tmp/spark-recsys/movie-category-sim/report-dashboard/index.html
```

Expected: both counts exceed zero and the dashboard contains numeric embedding/hybrid results.

- [ ] **Step 5: Commit and push**

```bash
git add README.md
git commit -m "docs: document simulation embedding dashboard"
git push
```

