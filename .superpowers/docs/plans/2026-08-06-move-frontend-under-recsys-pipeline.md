# Move Frontend Under `recsys-pipeline` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `recsys-pipeline/frontend/` the only active location used by dashboard code, scripts, tests, ignore rules, and user-facing documentation.

**Architecture:** Preserve the user's directory move and repair consumers at their existing boundaries. Repository-root commands use `recsys-pipeline/frontend`; pipeline-local scripts and documentation use `frontend`; the exporter derives the pipeline root from its own file location and imports Python modeling from `services/python-modeling`.

**Tech Stack:** Git, Bash, Python/pytest, Next.js/npm, Markdown

## Global Constraints

- Do not modify historical `.superpowers` specifications or implementation plans other than this new design and plan.
- Do not create a compatibility symlink, duplicate frontend directory, or dual-path discovery logic.
- Preserve frontend runtime behavior and the dashboard JSON schema.
- Preserve all unrelated user changes in the working tree.

---

### Task 1: Repair Executable Path Contracts

**Files:**
- Modify: `.gitignore`
- Modify: `recsys-pipeline/frontend/export_dashboard_json.py`
- Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh`
- Modify: `recsys-pipeline/integration-tests/test_service_scripts.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py`
- Move: `frontend/**` to `recsys-pipeline/frontend/**` (preserve the user's existing move)

**Interfaces:**
- Consumes: script execution from `recsys-pipeline/` and exporter execution from either the repository root or frontend directory.
- Produces: canonical dashboard path `recsys-pipeline/frontend/data/dashboard.json`; exporter import root `recsys-pipeline/services/python-modeling`.

- [ ] **Step 1: Add failing relocation assertions**

Extend `test_movie_category_sim_wires_every_measurement_input` with exact path checks:

```python
assert '--output "frontend/data/dashboard.json"' in script
assert 'python3 frontend/export_dashboard_json.py' in script
assert '(cd frontend && npm run validate:data)' in script
assert "../frontend/" not in script
```

Update the dashboard contract test's frontend fixture root from
`_REPO / "frontend"` to `_REPO / "recsys-pipeline" / "frontend"`; `_REPO`
already denotes the repository root. In `test_analysis_dashboard.py`, replace
`Path(__file__).parents[3] / "frontend"` with
`Path(__file__).parents[2] / "frontend"`, because `parents[2]` denotes
`recsys-pipeline/`.

- [ ] **Step 2: Run the focused tests to verify the stale paths fail**

Run:

```bash
/Users/linghuang/miniconda3/bin/pytest \
  recsys-pipeline/integration-tests/test_service_scripts.py::test_movie_category_sim_wires_every_measurement_input \
  recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py::test_frontend_readme_points_at_the_paths_a_run_actually_writes -q
```

Expected: FAIL because the simulation script still uses `../frontend` and/or
the test fixture still resolves the old root-level directory.

- [ ] **Step 3: Repair executable and test-relative paths**

In `export_dashboard_json.py`, make `_REPO` represent `recsys-pipeline/`:

```python
_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "services" / "python-modeling"))
```

Set its default output to:

```python
ap.add_argument("--output", default=str(_REPO / "frontend" / "data" / "dashboard.json"))
```

In `run-movie-category-sim.sh`, replace each `../frontend` reference with
`frontend`, because the script already runs with `recsys-pipeline/` as its
working-directory contract.

Update `.gitignore` from `frontend/__pycache__/` to
`recsys-pipeline/frontend/__pycache__/`. Update test fixture paths so they open
the moved frontend files.

- [ ] **Step 4: Run focused tests and frontend validation**

Run:

```bash
/Users/linghuang/miniconda3/bin/pytest \
  recsys-pipeline/integration-tests/test_service_scripts.py::test_movie_category_sim_wires_every_measurement_input \
  recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py::test_frontend_readme_points_at_the_paths_a_run_actually_writes -q
cd recsys-pipeline/frontend && npm run validate:data
```

Expected: both pytest cases pass and the validator reports seven valid
measurement sections.

- [ ] **Step 5: Commit the executable relocation**

Stage only the moved frontend, ignore rule, script, and directly affected tests:

```bash
git add -- .gitignore frontend recsys-pipeline/frontend \
  recsys-pipeline/scripts/run-movie-category-sim.sh \
  recsys-pipeline/integration-tests/test_service_scripts.py \
  recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py \
  recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py
git commit -m "refactor: move frontend into recsys pipeline"
```

### Task 2: Update Active Documentation and Verify the Move

**Files:**
- Modify: `README.md`
- Modify: `recsys-pipeline/README.md`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md`
- Modify: `recsys-pipeline/frontend/README.md`
- Modify: `recsys-pipeline/frontend/export_dashboard_json.py` (module example only)

**Interfaces:**
- Consumes: canonical paths established in Task 1.
- Produces: commands that resolve correctly from their documented working directory.

- [ ] **Step 1: Add a stale-active-reference regression check**

Run this search and save the unexpected active matches before editing:

```bash
rg -n --hidden --glob '!**/.git/**' --glob '!**/node_modules/**' \
  --glob '!.superpowers/**' '(^|[ (])frontend/' README.md .gitignore recsys-pipeline
```

Expected: root README links, exporter examples, and script references still
contain paths that assume a root-level frontend.

- [ ] **Step 2: Update commands and links by working-directory context**

Use `recsys-pipeline/frontend/...` in root `README.md`. Use `frontend/...` in
`recsys-pipeline/README.md`, `Analysis_Report.md`, the frontend README commands,
and exporter module examples. Keep the frontend README layout tree headed by
`frontend/`, since it describes the directory itself from the pipeline root.

- [ ] **Step 3: Verify active path references**

Run:

```bash
test ! -e frontend
test -f recsys-pipeline/frontend/package.json
rg -n --hidden --glob '!**/.git/**' --glob '!**/node_modules/**' \
  --glob '!.superpowers/**' 'frontend/' README.md .gitignore recsys-pipeline
```

Expected: every result is valid relative to its documented or executable
context; no active reference requires a root-level `frontend/` directory.

- [ ] **Step 4: Run complete affected verification**

Run:

```bash
/Users/linghuang/miniconda3/bin/pytest \
  recsys-pipeline/integration-tests/test_service_scripts.py \
  recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py \
  recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py -q
cd recsys-pipeline/frontend && npm run validate:data && npm run build
git diff --check
git status --short
```

Expected: all pytest suites pass, the dashboard validates and builds, the diff
has no whitespace errors, and status contains only the intended relocation and
documentation changes.

- [ ] **Step 5: Commit documentation updates**

```bash
git add -- README.md recsys-pipeline/README.md \
  recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md \
  recsys-pipeline/frontend/README.md \
  recsys-pipeline/frontend/export_dashboard_json.py
git commit -m "docs: update frontend paths after relocation"
```
