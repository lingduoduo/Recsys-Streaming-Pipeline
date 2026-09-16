# Excise Retrieval Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish removing the Java retrieval service from this repository, leaving no build file, workflow, script or live document that treats it as a component of this checkout.

**Architecture:** Delete the service directory and the Maven aggregator that existed only to hold it. Move contract verification to the repository that now owns the snapshots. Convert the one local harness that booted the service from source into one that probes a service over HTTP. Replace the retrieval-service workflow with one that covers the Python and Scala suites this repository actually still contains.

**Tech Stack:** Python 3.12 with pytest, Bash, GitHub Actions, sbt 1.12.9 under JDK 17.

**Spec:** [Excise the retrieval service design](../specs/2026-09-16-excise-retrieval-service-design.md)

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly.
- Producer-side canonical artifacts are untouched: `recsys-pipeline/schemas/recsys-event-v3.avsc`, `recsys-pipeline/schemas/fixtures/serving-impression-v3.avro`, `recsys-pipeline/integration-tests/fixtures/user_profile_v1.json`.
- No source change to `spark-streaming-job` or `python-modeling`. Their behavior must be identical before and after.
- Historical records are not rewritten: `.superpowers/docs/**`, `docs/superpowers/**` and `.planning/**` are dated design records, and the five Scala comment references to `java-retrieval-service` are provenance statements that remain true.
- `run-movie-category-sim.sh` must still complete when no service is reachable, degrading to a "Not measured" card exactly as it does today when `mvn` is absent.
- The replacement workflow must be hermetic: no Kafka, Redis, Spark cluster or Docker daemon required for it to pass.
- `spark-streaming-job` builds under JDK 17. JDK 25 aborts every Spark-session test with a misleading `getSubject` error.
- Remove only variables and imports that *these* changes orphan. Pre-existing dead code stays.

## Environment

All `pytest` commands run from `recsys-pipeline`, which holds the `pytest.ini` whose `testpaths = integration-tests` makes a bare `pytest` collect the right tree:

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q
```

sbt must run under JDK 17:

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/services/spark-streaming-job
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch test
```

## Test Count Ledger

Every task asserts an exact pytest total, so a stray deletion is caught immediately. The measured pre-change baseline is **1 failed, 558 passed, 1 skipped**.

| After task | failed | passed | skipped |
|---|---|---|---|
| baseline | 1 | 558 | 1 |
| 1 — contract seam retired | 0 | 556 | 1 |
| 2 — config test dropped | 0 | 555 | 1 |
| 3 — sim harness repointed | 0 | 556 | 1 |
| 4 — service tree deleted | 0 | 557 | 1 |
| 5 — workflow replaced | 0 | 557 | 1 |
| 6 — documentation updated | 0 | 558 | 1 |

The single baseline failure is `test_retrieval_contracts.py::RetrievalContractTest::test_event_schemas_match_the_canonical_producer_schema`, which reads the already-deleted `src/main/resources/schemas/`. The other two tests in that file still pass because the `contracts/` fixtures survive in the working tree, so retiring the file removes one failure and two passes.

## File Structure

- Delete: `recsys-pipeline/services/java-retrieval-service/` — the whole tree, all 187 tracked files.
- Delete: `recsys-pipeline/pom.xml` — an aggregator whose only module is that directory.
- Delete: `recsys-pipeline/integration-tests/test_retrieval_contracts.py` — the comparison moves to the consumer.
- Delete: `.github/workflows/retrieval-service.yml` — its only job builds the deleted directory.
- Create: `.github/workflows/pipeline.yml` — Python and Scala jobs for the suites that remain.
- Create: `recsys-pipeline/integration-tests/test_retrieval_service_extracted.py` — the two hygiene tests that keep references from creeping back.
- Modify: `recsys-pipeline/integration-tests/test_application_config.py` — drop the one test that read the service's `application.yml`.
- Modify: `recsys-pipeline/integration-tests/test_service_scripts.py` — add the sim-probes-a-URL test.
- Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh` — probe `SERVICE_URL` instead of booting from source; drop the MDP evaluator invocation.
- Modify: six live documents listed in Task 6. `recsys-pipeline/README.md` carries 13 of the references on its own; the other five have one to three each.

---

### Task 0: Open the draft pull request

**Files:**
- Already committed: `.superpowers/docs/specs/2026-09-16-excise-retrieval-service-design.md` (`eb23b81`)
- Create: `.superpowers/docs/plans/2026-09-16-excise-retrieval-service.md` (this file)

**Interfaces:**
- Produces: the branch `refactor/excise-retrieval-service` and a draft PR against `master` that later tasks push into.

- [ ] **Step 1: Commit this plan**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git add .superpowers/docs/plans/2026-09-16-excise-retrieval-service.md
git commit -m "docs: plan excising the retrieval service"
```

- [ ] **Step 2: Push and open the draft PR**

```bash
git push -u origin refactor/excise-retrieval-service
gh pr create --draft --base master \
  --title "Excise the Java retrieval service" \
  --body "Finishes the removal of the retrieval service, which now lives in lingduoduo/Recsys-Backend-Service.

Design: [spec](.superpowers/docs/specs/2026-09-16-excise-retrieval-service-design.md) · [plan](.superpowers/docs/plans/2026-09-16-excise-retrieval-service.md)

Implementation in progress."
```

Note that the 177 worktree deletions are deliberately still unstaged at this point; Task 4 stages them with `git rm`.

---

### Task 1: Retire the contract seam

**Files:**
- Delete: `recsys-pipeline/integration-tests/test_retrieval_contracts.py`
- Delete: `.github/workflows/retrieval-service.yml`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: a repository with no contract comparison and no retrieval-service workflow. Task 5 creates the replacement workflow; between these two tasks the repository has no CI at all, which is expected.

- [ ] **Step 1: Record the pre-change baseline**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -3
```

Expected: `1 failed, 558 passed, 1 skipped`. If the numbers differ, stop and reconcile before changing anything — every later assertion is relative to this.

- [ ] **Step 2: Delete both files**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git rm recsys-pipeline/integration-tests/test_retrieval_contracts.py
git rm .github/workflows/retrieval-service.yml
```

- [ ] **Step 3: Verify the count dropped by exactly one failure and two passes**

```bash
cd recsys-pipeline && python3 -m pytest -q 2>&1 | tail -3
```

Expected: `556 passed, 1 skipped` with no failures.

- [ ] **Step 4: Confirm nothing else referenced the deleted test**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
grep -rn 'test_retrieval_contracts\|RETRIEVAL_SERVICE_DIR' --include='*.py' --include='*.yml' --include='*.sh' --include='*.md' . | grep -v '.superpowers/docs' | grep -v 'docs/superpowers'
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git commit -m "chore: retire the retrieval contract seam and its workflow

The comparison belongs in the repository that owns the snapshots. The
canonical producer artifacts stay here; two of the three service-side
sources the test read no longer exist in this checkout."
```

---

### Task 2: Drop the configuration test that read the service

**Files:**
- Modify: `recsys-pipeline/integration-tests/test_application_config.py`

**Interfaces:**
- Consumes: nothing.
- Produces: a `test_application_config.py` holding six tests, all README-only.

- [ ] **Step 1: Remove the test, its path constant and the imports it orphaned**

Delete the `CONFIG_PATH` assignment near the top:

```python
CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "services",
    "java-retrieval-service",
    "src",
    "main",
    "resources",
    "application.yml",
)
```

Delete the whole test:

```python
def test_readme_documents_every_measurement_environment_variable():
    # Operators can only tune what is written down; derive the list from the config
    # itself so a new measurement knob cannot ship undocumented.
    with open(CONFIG_PATH) as f:
        measurements = yaml.safe_load(f)["recsys"]["measurements"]
    variables = [re.match(r"\$\{([A-Z0-9_]+):", str(value)).group(1) for value in measurements.values()]
    with open(README_PATH) as f:
        content = f.read()
    missing = [variable for variable in variables if variable not in content]
    assert not missing, f"README must document the measurement variables {missing}"
```

Delete these two imports, which that test was the only user of:

```python
import re
import yaml
```

Leave `import pytest` alone. It is already unused in this file and predates this change.

- [ ] **Step 2: Verify no leftover references**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
grep -n 'CONFIG_PATH\|yaml\.\|re\.' integration-tests/test_application_config.py
```

Expected: no output. `json`, `os` and `datetime` are still used by the surviving tests and must remain.

- [ ] **Step 3: Verify the count**

```bash
python3 -m pytest -q 2>&1 | tail -3
python3 -m pytest integration-tests/test_application_config.py -q 2>&1 | tail -3
```

Expected: `555 passed, 1 skipped` overall, and `6 passed` for the file alone.

- [ ] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git add recsys-pipeline/integration-tests/test_application_config.py
git commit -m "test: drop the measurement-variable cross-check

It derived the expected variable list from the retrieval service's
application.yml, which has left this repository. The six README
assertions in this file are unaffected."
```

---

### Task 3: Repoint the simulation harness at a running service

**Files:**
- Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh:13-14` (add `SERVICE_URL`), `:202-260` (the SERVICE BURST block), `:261-279` (the MDP block)
- Test: `recsys-pipeline/integration-tests/test_service_scripts.py`

**Interfaces:**
- Consumes: nothing.
- Produces: the sim reads `SERVICE_URL`, defaulting to `http://localhost:$SERVICE_PORT`, where `SERVICE_PORT` keeps honouring `RETRIEVAL_SERVICE_PORT`. Task 6 documents both.

Two existing tests already constrain this file and must keep passing: `test_movie_category_sim_wires_every_measurement_input` requires `/metrics` and `live-metrics.json` to remain in the script, and `test_movie_category_sim_never_fails_on_a_missing_live_service` splits the text on `SERVICE BURST` and requires that block to contain `|| true` or `continue` and no `set -e`.

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/test_service_scripts.py`, after `test_movie_category_sim_never_fails_on_a_missing_live_service`:

```python
def test_movie_category_sim_probes_a_running_service_instead_of_building_one() -> None:
    """The service lives in Recsys-Backend-Service; the sim measures whatever is listening."""
    script = SIM_SCRIPT.read_text(encoding="utf-8")

    # it must reach the service over a configurable URL
    assert 'SERVICE_URL="${SERVICE_URL:-http://localhost:$SERVICE_PORT}"' in script
    # and must not build, boot or shut down a service from this checkout
    assert "spring-boot:run" not in script
    assert "java-retrieval-service" not in script
    assert "MovieLensPolicyEvaluation" not in script
    assert "kill_service" not in script
    # the MDP card still has a path to render "Not measured"
    assert 'MDP_CSV="$SIM_ROOT/mdp_eval.csv"' in script
```

- [ ] **Step 2: Run it to make sure it fails**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest integration-tests/test_service_scripts.py::test_movie_category_sim_probes_a_running_service_instead_of_building_one -q
```

Expected: FAIL on the first assertion, because `SERVICE_URL` does not exist yet.

- [ ] **Step 3: Add the `SERVICE_URL` default**

After line 14's `SERVICE_PORT="${RETRIEVAL_SERVICE_PORT:-8080}"`, add:

```bash
SERVICE_URL="${SERVICE_URL:-http://localhost:$SERVICE_PORT}"
```

- [ ] **Step 4: Replace the boot with a probe**

Replace everything from `service_pid=""` through the `for _ in $(seq 1 40); do ... done` wait loop — that is the `kill_service` function, the `(cd services/java-retrieval-service && ... spring-boot:run ...) &` subshell, `service_pid=$!`, `trap kill_service EXIT`, and the retry loop — with this comment alone:

```bash
# The retrieval service lives in lingduoduo/Recsys-Backend-Service. The sim measures whatever
# is already listening on $SERVICE_URL rather than building one from this checkout, and a
# missing service only costs the latency card -- it must never fail the run.
```

Then rewrite the three request sites and the capture to use `$SERVICE_URL`:

- `if curl -sf "http://localhost:$SERVICE_PORT/metrics" >/dev/null 2>&1; then` becomes `if curl -sf "$SERVICE_URL/metrics" >/dev/null 2>&1; then`
- `item="$(curl -sf "http://localhost:$SERVICE_PORT/recommend/$user?limit=6" \` becomes `item="$(curl -sf "$SERVICE_URL/recommend/$user?limit=6" \`
- `curl -sf -X POST "http://localhost:$SERVICE_PORT/feedback" \` becomes `curl -sf -X POST "$SERVICE_URL/feedback" \`
- `curl -sf "http://localhost:$SERVICE_PORT/metrics" > "$LIVE_METRICS" 2>/dev/null || true` becomes `curl -sf "$SERVICE_URL/metrics" > "$LIVE_METRICS" 2>/dev/null || true`

Change the else branch, which currently blames a failed start:

```bash
else
  echo "   no service answering at $SERVICE_URL — latency stays N/A"
fi
```

Delete the three teardown lines that followed the `fi`:

```bash
kill_service
wait "$service_pid" 2>/dev/null || true
trap - EXIT
```

The `MEASUREMENT_JAVA_HOME` reference disappears with the boot subshell. It appeared nowhere else in the repository, so nothing else needs updating.

- [ ] **Step 5: Replace the MDP evaluator with its "Not measured" path**

Keep the `MDP_CSV` assignment, because line 285 passes `--mdp-csv "$MDP_CSV"` to `analysis_dashboard_report.py` unconditionally and line 291 guards on `[[ -s "$MDP_CSV" ]]`. Only the generation block goes. Replace the whole `if command -v mvn >/dev/null 2>&1; then ... fi` block with:

```bash
echo "   evaluator now lives in Recsys-Backend-Service — MDP card stays Not measured"
```

and update the comment above `MDP_CSV` so it no longer promises Maven:

```bash
# The evaluator (MovieLensPolicyEvaluation) moved to Recsys-Backend-Service, so this card is
# always "Not measured" from a pipeline-only checkout. MDP_CSV stays defined because the
# analysis report takes --mdp-csv unconditionally and the exporter guards on the file existing.
```

- [ ] **Step 6: Syntax-check the script**

```bash
bash -n recsys-pipeline/scripts/run-movie-category-sim.sh && echo "syntax ok"
```

Expected: `syntax ok`.

- [ ] **Step 7: Run the new test and the two existing guards**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest integration-tests/test_service_scripts.py -q -k "movie_category_sim or sim_starts_jobs" 2>&1 | tail -3
```

Expected: all pass, including `test_movie_category_sim_wires_every_measurement_input` and `test_movie_category_sim_never_fails_on_a_missing_live_service`.

- [ ] **Step 8: Verify the full count**

```bash
python3 -m pytest -q 2>&1 | tail -3
```

Expected: `556 passed, 1 skipped`.

- [ ] **Step 9: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git add recsys-pipeline/scripts/run-movie-category-sim.sh recsys-pipeline/integration-tests/test_service_scripts.py
git commit -m "refactor: measure a running service instead of building one

The sim booted the retrieval service with mvn spring-boot:run from a
directory this repository no longer contains. It now probes SERVICE_URL,
defaulting to the existing RETRIEVAL_SERVICE_PORT, and leaves the latency
card N/A when nothing answers -- the same degradation it already had for a
missing mvn. The MDP evaluator moved with the service, so that card is now
always Not measured."
```

---

### Task 4: Delete the service tree and the Maven aggregator

**Files:**
- Delete: `recsys-pipeline/services/java-retrieval-service/` (187 tracked files)
- Delete: `recsys-pipeline/pom.xml`
- Create: `recsys-pipeline/integration-tests/test_retrieval_service_extracted.py`

**Interfaces:**
- Consumes: Task 1 removed the workflow reference and Task 3 removed the script reference, so `recsys-pipeline/pom.xml` is the only non-document reference left when this task starts.
- Produces: `test_retrieval_service_extracted.py` with the module constants `GIT_ROOT`, `HISTORICAL_DIRS`, `SKIP_DIRS`, `LIVE_SUFFIXES` and the helper `references()`, which Task 6 reuses for its documentation test.

- [ ] **Step 1: Write the failing test**

Create `recsys-pipeline/integration-tests/test_retrieval_service_extracted.py`:

```python
"""The retrieval service moved to lingduoduo/Recsys-Backend-Service.

These guard against a reference creeping back into a file meant to describe this
repository as it is now.

Two categories are deliberately not scanned. Dated design records under
.superpowers/docs, docs/superpowers and .planning are history: they describe the
repository as it was and must not be rewritten. And .py and .scala files carry
provenance comments that name the Java classes which produced a fixture or a feature
layout -- GrpoFeatures in grpo_offline_eval.py, RecsysEventAvroCodec in
ServingImpressionFixtureSpec.scala -- which remain true statements about where the
data came from. Scanning .py would also flag this file, which contains the search
string itself, and the `not in` assertion in test_service_scripts.py.
"""

from pathlib import Path

GIT_ROOT = Path(__file__).resolve().parents[2]
NEEDLE = "java-retrieval-service"

HISTORICAL_PREFIXES = (".superpowers/", "docs/superpowers/", ".planning/")
SKIP_DIRS = {
    ".git", "target", "node_modules", ".next", "__pycache__",
    ".worktrees", ".pytest_cache", ".venv", "venv",
}
BUILD_SUFFIXES = {".xml", ".yml", ".yaml", ".sh"}
DOC_SUFFIXES = {".md", ".html"}


def references(suffixes):
    """Every live file with one of `suffixes` that still names the service."""
    hits = []
    for path in GIT_ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in suffixes:
            continue
        relative = path.relative_to(GIT_ROOT)
        if relative.as_posix().startswith(HISTORICAL_PREFIXES):
            continue
        if set(relative.parts) & SKIP_DIRS:
            continue
        if NEEDLE in path.read_text(encoding="utf-8", errors="ignore"):
            hits.append(relative.as_posix())
    return sorted(hits)


def test_no_build_file_workflow_or_script_references_the_extracted_service():
    hits = references(BUILD_SUFFIXES)
    assert not hits, f"build/CI/script files still reference the service: {hits}"
```

- [ ] **Step 2: Run it to make sure it fails**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest integration-tests/test_retrieval_service_extracted.py -q
```

Expected: FAIL naming `recsys-pipeline/pom.xml`. If it names anything else, Task 1 or Task 3 left something behind — fix that before continuing.

- [ ] **Step 3: Delete the tree and the aggregator**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git rm -r --quiet recsys-pipeline/services/java-retrieval-service
git rm --quiet recsys-pipeline/pom.xml
```

`git rm -r` stages the 177 files already deleted in the worktree and removes the 10 that survived.

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd recsys-pipeline && python3 -m pytest integration-tests/test_retrieval_service_extracted.py -q
```

Expected: `1 passed`.

- [ ] **Step 5: Verify the tree is gone and nothing unique went with it**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git ls-files recsys-pipeline/services/java-retrieval-service | wc -l   # expect 0
find recsys-pipeline -name pom.xml | wc -l                              # expect 0
git show HEAD:recsys-pipeline/services/java-retrieval-service/src/main/resources/schemas/recsys-event-v3.avsc | shasum
shasum recsys-pipeline/schemas/recsys-event-v3.avsc
```

The last two commands must print the same hash, `eb636a771c19cd736592e37e0a135ffef55f3f8e`, confirming the deleted production schema was a byte-identical copy of the canonical one that stays.

- [ ] **Step 6: Verify the full count**

```bash
cd recsys-pipeline && python3 -m pytest -q 2>&1 | tail -3
```

Expected: `557 passed, 1 skipped`.

- [ ] **Step 7: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git add recsys-pipeline/integration-tests/test_retrieval_service_extracted.py
git commit -m "chore: delete the retrieval service and its Maven aggregator

All 187 tracked files go, including the pom.xml, Dockerfile and
application.yml that survived the initial move and would otherwise suggest
the service still builds here. recsys-pipeline/pom.xml declared that
directory as its only module, so it goes too; no Maven build remains in
this repository.

Nothing unique is lost: the deleted production schema is byte-identical to
the canonical recsys-pipeline/schemas/recsys-event-v3.avsc, and the
contracts/ fixtures were frozen copies of canonical artifacts that stay."
```

---

### Task 5: Replace the retired workflow with pipeline CI

**Files:**
- Create: `.github/workflows/pipeline.yml`

**Interfaces:**
- Consumes: Task 1 deleted the only previous workflow, so this is the repository's sole CI definition.
- Produces: jobs named `python` and `scala`, which Task 7 checks are green.

- [ ] **Step 1: Confirm the Python job's command works locally**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -3
```

Expected: `557 passed, 1 skipped`. The one skip is `test_avro_kafka_round_trip.py`, which self-skips with no broker — that is what makes this job hermetic.

- [ ] **Step 2: Confirm the Scala job's command works locally**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/services/spark-streaming-job
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch test 2>&1 | tail -5
```

Expected, measured before any file was touched: `Total number of tests run: 444`, `Suites: completed 66, aborted 0`, `Tests: succeeded 444, failed 0`, in 10 minutes 29 seconds. These must be identical after the change, because it touches no Scala file. Budget for the duration — it dominates the workflow's wall-clock time.

- [ ] **Step 3: Create the workflow**

```yaml
name: Pipeline

on:
  push:
    paths:
      - recsys-pipeline/integration-tests/**
      - recsys-pipeline/services/python-modeling/**
      - recsys-pipeline/services/spark-streaming-job/**
      - recsys-pipeline/scripts/**
      - recsys-pipeline/schemas/**
      - recsys-pipeline/sampledata/**
      - recsys-pipeline/pytest.ini
      - recsys-pipeline/README.md
      - .github/workflows/pipeline.yml
  pull_request:
    paths:
      - recsys-pipeline/integration-tests/**
      - recsys-pipeline/services/python-modeling/**
      - recsys-pipeline/services/spark-streaming-job/**
      - recsys-pipeline/scripts/**
      - recsys-pipeline/schemas/**
      - recsys-pipeline/sampledata/**
      - recsys-pipeline/pytest.ini
      - recsys-pipeline/README.md
      - .github/workflows/pipeline.yml

permissions:
  contents: read

jobs:
  python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
          cache: pip
      - name: Install test dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pytest pyyaml numpy pandas 'pyarrow>=14,<24' kafka-python lz4 'fastavro>=1.9,<2'
          pip install torch --index-url https://download.pytorch.org/whl/cpu
      - name: Run the pipeline integration tests
        working-directory: recsys-pipeline
        run: python -m pytest -q

  scala:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '17'
          cache: sbt
      - uses: sbt/setup-sbt@v1
      - name: Run the spark-streaming-job tests
        working-directory: recsys-pipeline/services/spark-streaming-job
        run: sbt -batch test
```

`torch` comes from the CPU wheel index because the default index pulls CUDA payloads this suite never uses; `python_modeling/test_post_training_dpo.py` imports it unguarded, so it cannot be omitted. `sbt/setup-sbt@v1` is required because recent Ubuntu runner images no longer ship sbt.

- [ ] **Step 4: Lint the workflow**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
actionlint .github/workflows/pipeline.yml && echo "actionlint clean"
```

Expected: `actionlint clean`. If `actionlint` is not installed, `brew install actionlint`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/pipeline.yml
git commit -m "ci: cover the Python and Scala suites this repository still has

Deleting retrieval-service.yml left the repository with no CI at all,
while 557 Python tests and the spark-streaming-job suite passed locally and
ran nowhere. Both jobs are hermetic: the one Kafka-dependent test self-skips
without a broker, and sbt is pinned to JDK 17 because JDK 25 aborts every
Spark-session test with a misleading getSubject error."
```

---

### Task 6: Update the live documentation

**Files:**
- Modify: `recsys-pipeline/README.md` (13 references), `README.md`, `recsys-pipeline/docs/recommendation_architecture/API.md`, `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`, `recsys-pipeline/frontend/README.md`, `recsys-pipeline/recsys-streaming-pipeline-architecture.html`
- Modify: `recsys-pipeline/integration-tests/test_retrieval_service_extracted.py`
- Do **not** modify: `recsys-pipeline/docs/recommendation_flows/3_Cold_Start.md:45` and `6_Predicting_Scoring.md:75`. An earlier draft of this plan listed them, wrongly. They do not reference the service's location at all — each links to `../../README.md#retrieval-service-configuration`, a section anchor. See Step 4's anchor constraint.

**Interfaces:**
- Consumes: `references()` and `DOC_SUFFIXES` from the module Task 4 created.
- Produces: nothing later tasks depend on.

This repository has many tests that assert README content — `test_application_config.py` alone has six, and `python_modeling/test_dashboard_measurement_contract.py` asserts specific paths in `frontend/README.md`. Run the full suite after editing, not just the new test.

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/test_retrieval_service_extracted.py`:

```python
def test_no_live_document_references_the_extracted_service():
    hits = references(DOC_SUFFIXES)
    assert not hits, f"live documents still reference the service: {hits}"
```

- [ ] **Step 2: Run it to make sure it fails**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest integration-tests/test_retrieval_service_extracted.py -q
```

Expected: FAIL listing exactly these six, verified by running the same scan before writing this plan:

```
README.md
recsys-pipeline/README.md
recsys-pipeline/docs/recommendation_architecture/API.md
recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
recsys-pipeline/frontend/README.md
recsys-pipeline/recsys-streaming-pipeline-architecture.html
```

A seventh, `recsys-pipeline/services/java-retrieval-service/README.md`, appears in that scan only if Task 4 has not run yet; it is deleted with the tree rather than edited.

- [ ] **Step 3: Enumerate every line to change**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
grep -rn 'java-retrieval-service' README.md recsys-pipeline/README.md \
  recsys-pipeline/docs recsys-pipeline/frontend/README.md \
  recsys-pipeline/recsys-streaming-pipeline-architecture.html
```

- [ ] **Step 4: Rewrite each reference**

Apply these rules rather than deleting text wholesale, so the documents keep describing the system and only stop claiming the service is built here:

- A path like `recsys-pipeline/services/java-retrieval-service` that names *where the code is* becomes a pointer to the external repository: `the retrieval service ([lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service))`.
- An instruction to build or run it from this checkout — `cd services/java-retrieval-service && mvn ...`, `mvn spring-boot:run` — is replaced by a sentence saying the service is a separate deployable and that the sim measures whatever answers on `SERVICE_URL`.
- A description of behavior that is still true (endpoints, ranking, Redis keys, Kafka topics) keeps its content; only the location claim changes.

**Anchor constraint.** `recsys-pipeline/README.md:326` is the heading `## Retrieval Service Configuration`, and two documents this task does not otherwise touch link to its anchor — `docs/recommendation_flows/3_Cold_Start.md:45` and `docs/recommendation_flows/6_Predicting_Scoring.md:75` both point at `../../README.md#retrieval-service-configuration`. Keep that heading's text exactly as it is so the anchor stays valid; rewrite only its body, which at line 328 points at the deleted `services/java-retrieval-service/src/main/resources/application.yml` and should instead say the configuration now lives in the backend repository. If you do rename the heading, you must update both links in the same commit.

One row needs specific wording. `recsys-pipeline/README.md:581` currently reads:

```
| `RETRIEVAL_SERVICE_PORT` | `8080` | `run-movie-category-sim.sh` | Port the sim starts the retrieval service on for its traffic burst |
```

The sim no longer starts anything, so replace it with two rows:

```
| `RETRIEVAL_SERVICE_PORT` | `8080` | `run-movie-category-sim.sh` | Port used to build the default `SERVICE_URL` |
| `SERVICE_URL` | `http://localhost:$RETRIEVAL_SERVICE_PORT` | `run-movie-category-sim.sh` | Retrieval service the sim measures; the latency card stays N/A when nothing answers |
```

- [ ] **Step 5: Run the new test to verify it passes**

```bash
cd recsys-pipeline && python3 -m pytest integration-tests/test_retrieval_service_extracted.py -q
```

Expected: `2 passed`.

- [ ] **Step 6: Run the full suite, because documentation assertions are load-bearing here**

```bash
python3 -m pytest -q 2>&1 | tail -3
```

Expected: `558 passed, 1 skipped`. A failure here is most likely a README assertion in `test_application_config.py` or `python_modeling/test_dashboard_measurement_contract.py` that your rewording broke — fix the wording, not the test, unless the test asserts something the move genuinely made false.

- [ ] **Step 7: Confirm the only survivors are history and provenance**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
grep -rln 'java-retrieval-service' --exclude-dir=.git --exclude-dir=.worktrees \
  --exclude-dir=target --exclude-dir=node_modules . | sort
```

Expected: only

- dated records under `.superpowers/docs/`, `docs/superpowers/` and `.planning/`;
- the three `spark-streaming-job` `.scala` test files, whose comments record which Java class encoded a fixture;
- `recsys-pipeline/services/python-modeling/post-training/grpo_offline_eval.py` and `recsys-pipeline/integration-tests/python_modeling/test_grpo_offline_eval.py`, whose comments pin the `GrpoFeatures` feature layout to the Java class that writes it;
- `recsys-pipeline/integration-tests/test_service_scripts.py` and `test_retrieval_service_extracted.py`, which contain the string as an assertion.

Anything else is a miss. These are all provenance or assertions rather than location claims, which is why `.py` and `.scala` are outside the scanned suffixes.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "docs: point the live documentation at Recsys-Backend-Service

Describes the retrieval service as a separate deployable rather than a
module of this checkout, and replaces instructions to build it from here.
Behavioral descriptions -- endpoints, ranking, Redis keys, Kafka topics --
are unchanged; only location claims and build instructions move. The
RETRIEVAL_SERVICE_PORT row gains a SERVICE_URL companion.

Dated design records under .superpowers/docs, docs/superpowers and .planning
are left as written, and so are the Scala comments that name the service
only to record where a fixture came from."
```

---

### Task 7: Mark the pull request ready

**Files:** none.

**Interfaces:**
- Consumes: green `python` and `scala` jobs from Task 5's workflow.

- [ ] **Step 1: Confirm a clean tree and push**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git status --short          # expect no output
git diff --check            # expect no output
git push
```

- [ ] **Step 2: Wait for both jobs and confirm they pass**

```bash
gh pr checks --watch
```

Expected: `python` and `scala` both pass. The `scala` job takes several minutes.

- [ ] **Step 3: Record the evidence in the PR body**

```bash
gh pr edit --body "$(cat <<'BODY'
Finishes removing the Java retrieval service, which now lives in
[lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service).

Design: [spec](.superpowers/docs/specs/2026-09-16-excise-retrieval-service-design.md) · [plan](.superpowers/docs/plans/2026-09-16-excise-retrieval-service.md)

What changed:
- Deleted all 187 tracked files under `recsys-pipeline/services/java-retrieval-service/`, including the `pom.xml`, `Dockerfile` and `application.yml` that survived the initial move.
- Deleted `recsys-pipeline/pom.xml`, an aggregator whose only module was that directory. No Maven build remains here.
- Moved contract verification to the repository that owns the snapshots; deleted `test_retrieval_contracts.py`.
- Replaced `retrieval-service.yml` with `pipeline.yml`, giving the Python and Scala suites their first CI.
- `run-movie-category-sim.sh` measures a service on `SERVICE_URL` instead of booting one with `mvn spring-boot:run`.

Validation:
- pytest: baseline 1 failed / 558 passed / 1 skipped → now 0 failed / 558 passed / 1 skipped.
- `spark-streaming-job` sbt suite unchanged under JDK 17: 444 tests across 66 suites, 0 failures, both before and after. No Scala file was touched.
- The deleted production schema was byte-identical to the canonical one that stays (`eb636a77`).
- `run-movie-category-sim.sh` completes with no service running, leaving the latency and MDP cards unmeasured.

Follow-up, tracked separately: `Recsys-Backend-Service` still has the copy untracked with all 174 files declaring `package com.demo.retrieval`, is missing the `avro` and `spring-boot-starter-data-redis` dependencies, and needs the contract comparison this change moves to it. Until that lands, schema drift between the two repositories is detected nowhere.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
BODY
)"
gh pr ready
```

---

## Self-Review

**Spec coverage.** Delete the service tree → Task 4. Drop the Maven aggregator → Task 4. Retire the contract seam → Task 1. Fix the one broken Python test → Task 2. Repoint the simulation harness → Task 3. Replace the workflow → Task 5. Update live documentation → Task 6. Acceptance criteria 1 and 4 → Task 4 Step 5 and Task 6 Step 7; criterion 2 → the Test Count Ledger, asserted in every task; criterion 3 → Task 5 Step 2; criterion 5 → Task 3 Step 7 via the two existing sim guards, plus the manual no-service run recorded in Task 7's PR body; criterion 6 → Task 5 Step 4 and Task 7 Step 2; criterion 7 → Task 7 Step 1. Delivery → Task 0 and Task 7.

**Placeholder scan.** No "TBD", "TODO" or "similar to Task N". Task 6 is the one task that cannot enumerate every edit as literal text, because the eight documents' wording varies; it compensates with a `grep` that produces the authoritative work queue, three explicit rewrite rules, literal before-and-after text for the one row whose wording matters, and a full-suite gate that catches a rewording which breaks a documentation assertion.

**Type consistency.** `references(suffixes)`, `GIT_ROOT`, `NEEDLE`, `HISTORICAL_PREFIXES`, `SKIP_DIRS`, `BUILD_SUFFIXES` and `DOC_SUFFIXES` are defined once in Task 4 Step 1; Task 6 Step 1 calls `references(DOC_SUFFIXES)` using those exact names. An earlier draft of this plan had Task 6 call `references(LIVE_SUFFIXES - {".scala"})` over a suffix set that included `.py`, which would have made the test fail permanently by matching the search string inside its own source and inside `test_service_scripts.py`'s `not in` assertion. `.py` and `.scala` are excluded outright instead. `SERVICE_URL`, `SERVICE_PORT`, `MDP_CSV` and `LIVE_METRICS` are spelled identically in Task 3's test, script edits and Task 6's README row. The test names asserted in Task 3 Step 7 match the names in the existing file.

**The new test code was executed, not just written.** The `references()` helper was run against the current tree before this plan was finalised, which corrected two errors. It found three build/CI/script hits — the workflow, `recsys-pipeline/pom.xml` and the sim script, exactly the three that Tasks 1, 3 and 4 remove — confirming Task 4's test fails on `pom.xml` alone once its predecessors have run. And it found six live documents rather than the eight an earlier draft listed: `3_Cold_Start.md` and `6_Predicting_Scoring.md` match a broader `retrieval-service` grep but contain no path into the service, only an anchor link, which is now handled as a constraint in Task 6 Step 4 instead of an edit.

**One ordering constraint worth restating.** Task 4's hygiene test only passes because Tasks 1 and 3 already removed the workflow and script references. Running Task 4 before them leaves it failing on files those tasks own. Tasks 1, 2 and 3 are independent of each other; 4 must follow 1 and 3; 6 must follow 4.
