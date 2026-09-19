# Drop the HTML dashboard renderer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the second dashboard renderer — the 249 lines of `analysis_dashboard_report.py` that build a self-contained `index.html` for six of the dashboard's thirteen sections — leaving the React app reading `frontend/data/dashboard.json` as the single surface.

**Architecture:** `analysis_dashboard_report.py` splits cleanly at line 494: everything above computes and is imported by `frontend/export_dashboard_json.py`; everything below renders HTML and is used by nothing else. Each task writes a guard assertion, watches it fail against the duplication, then removes the duplication until it passes.

**Tech Stack:** Python 3 / pytest (`pytest.ini` sets `testpaths = integration-tests`). Bash. Markdown. No JavaScript changes — `frontend/` is touched only in its README.

**Spec:** `.superpowers/docs/specs/2026-09-19-drop-html-dashboard-renderer-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- `frontend/` is not touched apart from `frontend/README.md`. No `.jsx`, no `.mjs`, no `package.json`, no `next.config.js`, and `frontend/data/dashboard.json` is **not** regenerated.
- `build_measurement_dashboard`, `measurement_config`, `MEASUREMENT_SCHEMA_VERSION` and every `compute_*` function keep their exact names and signatures. `frontend/export_dashboard_json.py` imports them and is **not edited**.
- The seven measurement sections and `frontend/validate_measurements.mjs` are untouched. The schema stays `"2.0"` on both sides.
- `import os` stays (used at line 57). `import argparse` goes. `pandas`, `numpy` and `redis` are lazy imports inside functions.
- Historical records under `.superpowers/docs/**` and `.planning/**` are not rewritten.
- Measured baseline at this branch point: **565 passed, 2 skipped**. This plan deletes five tests and adds three, so the final expected result is **563 passed, 2 skipped**.

## Deviation from the spec

The spec ordered four commits with the guard last. This plan folds the guard's two assertions into the tasks they guard — assertion one with the renderer deletion, assertion two with the simulation change — so each removal is proved by a test that failed first. A guard committed after the thing it guards is already gone can only ever pass, which is not a test. Commit count drops from four to three.

Note the arithmetic: the spec says "deletes five tests and adds two" for 562. This plan's guard file carries **three** tests, not two — the sim assertion is split into "still exports the snapshot" and "no longer calls the report", because a single test that fails could mean either, and the failure message should say which. Hence 563, not 562.

## File Structure

| File | Responsibility |
|---|---|
| Create: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_single_renderer.py` | The guard: the compute module exposes no rendering, and the sim produces one artifact. |
| Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py` | Delete lines 494-742, `import argparse`, rewrite the module docstring. Becomes a pure compute library. |
| Modify: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py` | Delete the five renderer tests; the other 22 stay. |
| Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh` | Remove the `ANALYSIS DASHBOARD` step; rewrite the closing banner. |
| Modify: `recsys-pipeline/frontend/README.md` · `recsys-pipeline/README.md` · `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md` · `README.md` | Remove two whole sections and six inline references; correct the Node-optional claim. |

---

### Task 1: Delete the renderer

**Files:**
- Create: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_single_renderer.py`
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py` (delete 494-742, delete `import argparse` at line 12, rewrite docstring lines 2-8)
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py` (delete five tests)

**Interfaces:**
- Consumes: nothing.
- Produces: `RENDERING_SYMBOLS`, `REPO`, `MODELING`, `SIM` module constants in the new test file. Task 2 appends to the same file and reuses `SIM`.

- [ ] **Step 1: Write the failing test**

Create `recsys-pipeline/integration-tests/python_modeling/test_dashboard_single_renderer.py`. Note the module-level `sys.path.insert` — that is how every test in this directory reaches the modeling code; there is no `conftest.py`.

```python
"""The dashboard has one renderer: the React app reading frontend/data/dashboard.json.

analysis_dashboard_report.py used to carry a second one -- 249 lines building a
self-contained index.html for six of the dashboard's thirteen sections -- and
run-movie-category-sim.sh ran both on every run, then named the smaller artifact
in its closing banner. The snapshot publishes every field that renderer read, at
equal or greater row counts, so the HTML was a strict subset of the React app.

_esc and _ci are deliberately not guarded below. Both are generic enough that a
future compute function could legitimately want them, and a guard that forbids a
two-line string escaper is one that gets deleted rather than obeyed.
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
MODELING = REPO / "services" / "python-modeling"
SIM = REPO / "scripts" / "run-movie-category-sim.sh"

sys.path.insert(0, str(MODELING))

RENDERING_SYMBOLS = (
    "render_html", "svg_bar", "svg_line", "html_table", "na_card", "section", "main",
)


def test_compute_module_exposes_no_html_rendering():
    import analysis_dashboard_report as dash

    present = [name for name in RENDERING_SYMBOLS if hasattr(dash, name)]
    assert not present, (
        "analysis_dashboard_report is the dashboard's compute layer; these rendering "
        f"symbols are back: {', '.join(present)}"
    )
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_single_renderer.py -q`
Expected: FAIL listing all seven symbols — `render_html, svg_bar, svg_line, html_table, na_card, section, main`.

- [ ] **Step 3: Delete the renderer block**

Everything from `def _esc` to end of file is rendering plus `main`. Verified symbol by symbol: no reference to any of it exists above line 494.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("services/python-modeling/analysis_dashboard_report.py")
lines = p.read_text().splitlines(keepends=True)
assert lines[493].startswith("def _esc("), f"line 494 is not def _esc: {lines[493]!r}"
assert len(lines) == 742, f"expected 742 lines, found {len(lines)}"
p.write_text("".join(lines[:493]).rstrip("\n") + "\n")
PY
wc -l services/python-modeling/analysis_dashboard_report.py
```

Expected: `493` or fewer (trailing blank lines are trimmed).

- [ ] **Step 4: Drop the orphaned import and rewrite the docstring**

`argparse` was used only by the deleted `main`. The docstring describes the deleted half.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("services/python-modeling/analysis_dashboard_report.py")
t = p.read_text()

old_doc = '''"""Consolidated analysis dashboard — keyword / query / relevance / recall / ranking as one HTML page.

Standalone pandas/Python (no Spark). Recomputes the metrics from a run's training_samples Parquet
+ Redis and writes a single self-contained index.html. Recall/ranking reuse the pure functions in
recall_eval_report.py / ranking_eval_report.py; genres/categories via feature_derivations.

    REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py --input <parquet>
"""'''
new_doc = '''"""Compute layer behind the analysis dashboard — the metrics, not their presentation.

Standalone pandas/Python (no Spark). Recomputes the metrics from a run's training_samples Parquet
+ Redis and returns them as plain dicts and DataFrames. Recall/ranking reuse the pure functions in
recall_eval_report.py / ranking_eval_report.py; off-policy evaluation reuses ope_eval_report.py;
genres/categories via feature_derivations.

The only consumer is frontend/export_dashboard_json.py, which writes frontend/data/dashboard.json
for the Next.js dashboard. This module renders nothing and has no command line.
"""'''
assert t.count(old_doc) == 1, "module docstring does not match verbatim"
t = t.replace(old_doc, new_doc)

assert t.count("\nimport argparse\n") == 1
t = t.replace("\nimport argparse\n", "\n")
p.write_text(t)
PY
head -14 services/python-modeling/analysis_dashboard_report.py
python3 -c "import sys; sys.path.insert(0, 'services/python-modeling'); import analysis_dashboard_report; print('imports clean')"
```

Expected: the new docstring, no `import argparse`, and `imports clean`.

- [ ] **Step 5: Delete the five renderer tests**

```bash
cd recsys-pipeline
python3 - <<'PY'
import re
from pathlib import Path
p = Path("integration-tests/python_modeling/test_analysis_dashboard.py")
text = p.read_text()
doomed = [
    "test_renderers_emit_svg_and_tables",
    "test_render_html_uses_modern_product_analytics_structure",
    "test_render_html_embeds_responsive_visual_system",
    "test_ope_section_renderer",
    "test_main_writes_recall_na_and_position_ranking_without_redis",
]
# Each test runs from its "def <name>" to the blank line before the next top-level def.
for name in doomed:
    pattern = re.compile(rf"\ndef {name}\(.*?(?=\ndef |\Z)", re.DOTALL)
    text, n = pattern.subn("\n", text)
    assert n == 1, f"{name}: matched {n} times"
p.write_text(text)
PY
grep -c '^def test_' integration-tests/python_modeling/test_analysis_dashboard.py
```

Expected: `22` (was 27).

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_single_renderer.py integration-tests/python_modeling/test_analysis_dashboard.py integration-tests/python_modeling/test_dashboard_measurement_contract.py -q`
Expected: PASS — 1 + 22 + 15 = 38 passed, 0 failed.

Then prove the exporter's import surface survived:
Run: `cd recsys-pipeline/frontend && python3 export_dashboard_json.py --help`
Expected: usage text, exit 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_single_renderer.py \
        recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "$(cat <<'MSG'
refactor: delete the second dashboard renderer

249 of the 742 lines in analysis_dashboard_report.py built a self-contained
index.html for six sections. The React dashboard renders thirteen from the same
compute functions, and the snapshot publishes every field the HTML read -- at
equal or greater row counts, since keyword carried fifty rows there against ten
in the HTML. The file is now what its only consumer already treated it as: a
compute library with no command line.

Five renderer tests go with it; the twenty-two compute tests and the fifteen
measurement-contract tests pass unchanged.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: Stop the simulation running two renderers

**Files:**
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_single_renderer.py` (append)
- Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh` (remove lines 254-258, rewrite line 271)

**Interfaces:**
- Consumes: `SIM` from Task 1.
- Produces: nothing.

- [ ] **Step 1: Write the failing tests**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_single_renderer.py`:

```python
def test_simulation_still_writes_the_react_snapshot():
    script = SIM.read_text()
    assert "export_dashboard_json.py" in script, (
        "run-movie-category-sim.sh no longer exports frontend/data/dashboard.json -- "
        "that snapshot is the dashboard's only data source"
    )


def test_simulation_does_not_run_a_second_renderer():
    script = SIM.read_text()
    offenders = [
        needle for needle in ("analysis_dashboard_report.py", "report-dashboard")
        if needle in script
    ]
    assert not offenders, (
        "run-movie-category-sim.sh names the deleted HTML renderer or its output "
        f"directory: {', '.join(offenders)}"
    )
```

- [ ] **Step 2: Run the tests to verify one fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_single_renderer.py -q`
Expected: 2 passed, 1 failed — `test_simulation_does_not_run_a_second_renderer` naming both `analysis_dashboard_report.py` (the invocation at line 257) and `report-dashboard` (the closing banner at line 271).

- [ ] **Step 3: Remove the ANALYSIS DASHBOARD step**

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("scripts/run-movie-category-sim.sh")
t = p.read_text()
old = '''echo
echo "==> ANALYSIS DASHBOARD (recall + ranking use Redis embeddings/popularity)"
REDIS_HOST=localhost REDIS_PORT=6379 \\
  python services/python-modeling/analysis_dashboard_report.py --input "$OUT_DIR" 2>&1 \\
  | grep -vE "INFO|WARN|^[0-9]{2}/"

'''
assert t.count(old) == 1, "the ANALYSIS DASHBOARD block does not match verbatim"
p.write_text(t.replace(old, ""))
PY
grep -n 'DASHBOARD' scripts/run-movie-category-sim.sh
```

Expected: only the `REACT DASHBOARD SNAPSHOT` line remains.

- [ ] **Step 4: Rewrite the closing banner**

The banner named the deleted artifact. It now names the one that exists and says what renders it.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("scripts/run-movie-category-sim.sh")
t = p.read_text()
old = 'echo "==> done. CSVs under $SIM_ROOT/report-categories ; dashboard at $SIM_ROOT/report-dashboard/index.html"'
new = 'echo "==> done. CSVs under $SIM_ROOT/report-categories ; dashboard snapshot at frontend/data/dashboard.json (render it with: cd frontend && npm run dev)"'
assert t.count(old) == 1
p.write_text(t.replace(old, new))
PY
bash -n scripts/run-movie-category-sim.sh && echo "syntax clean"
tail -3 scripts/run-movie-category-sim.sh
```

Expected: `syntax clean` and the new banner.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_single_renderer.py integration-tests/test_service_scripts.py integration-tests/python_modeling/test_movie_category_sim.py -q`
Expected: PASS, 0 failed. `test_service_scripts.py` asserts the `SERVICE BURST` properties and the `--experiences` / `--live-metrics` flags, all of which are in untouched parts of the script.

- [ ] **Step 6: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_single_renderer.py \
        recsys-pipeline/scripts/run-movie-category-sim.sh
git commit -m "$(cat <<'MSG'
refactor: run one dashboard step in the simulation, not two

The sim built both dashboards on every run and then pointed the reader at the
smaller one: "dashboard at $SIM_ROOT/report-dashboard/index.html" named the
six-section HTML file, not the thirteen-section snapshot the next step wrote.
The HTML step is gone and the banner names what exists, with the command that
renders it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: Documentation

**Files:**
- Modify: `recsys-pipeline/frontend/README.md` (lines 6 and 26-30)
- Modify: `recsys-pipeline/README.md` (the quoted banner at 828; the whole section at 934-953)
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md` (the table row at 18 and the trailing sentence at 21; the whole section at 189-206)
- Modify: `README.md` (line 85)

**Interfaces:**
- Consumes: the banner text written in Task 2, Step 4 — the quoted sample output must match it verbatim.
- Produces: nothing.

- [ ] **Step 1: Delete the two standalone-HTML sections**

Neither heading has an inbound link — verified by grepping for `optional-reference-standalone-html-dashboard` and `consolidated-standalone-html` across live markdown. `test_doc_links.py` (added in #247) will confirm on the next run.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path

def drop_section(path, heading, next_heading):
    p = Path(path)
    lines = p.read_text().splitlines(keepends=True)
    start = next(i for i, l in enumerate(lines) if l.startswith(heading))
    end = next(i for i in range(start + 1, len(lines)) if lines[i].startswith(next_heading))
    del lines[start:end]
    p.write_text("".join(lines))
    print(f"{path}: dropped {end - start} lines")

drop_section("README.md",
             "## Optional reference: standalone HTML dashboard",
             "## Troubleshooting the local workflow")
drop_section("docs/recommendation_architecture/Analysis_Report.md",
             "## Consolidated standalone HTML",
             "## React snapshot")
PY
```

Expected: two `dropped N lines` lines.

- [ ] **Step 2: Update the remaining inline references**

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path

edits = {
 "frontend/README.md": [
  ("""ranking / off-policy diagnostics from the Python `analysis_dashboard_report.py`,
as React components.""",
   """ranking / off-policy diagnostics, computed by the Python
`analysis_dashboard_report.py` and rendered here as React components."""),
  ("""`export_dashboard_json.py` reuses the pure `compute_*` functions from
`recsys-pipeline/services/python-modeling/analysis_dashboard_report.py`. The diagnostic sections
match the Python HTML dashboard; the seven measurement sections are exported for this app only
(`analysis_dashboard_report.py`'s HTML report does not render them):""",
   """`export_dashboard_json.py` reuses the pure `compute_*` functions from
`recsys-pipeline/services/python-modeling/analysis_dashboard_report.py`, which computes the
metrics and renders nothing. This app is the only surface that renders them:"""),
 ],
 "README.md": [
  ("""==> done. CSVs under /tmp/spark-recsys/movie-category-sim/report-categories ; dashboard at /tmp/spark-recsys/movie-category-sim/report-dashboard/index.html""",
   """==> done. CSVs under /tmp/spark-recsys/movie-category-sim/report-categories ; dashboard snapshot at frontend/data/dashboard.json (render it with: cd frontend && npm run dev)"""),
 ],
 "docs/recommendation_architecture/Analysis_Report.md": [
  ("""| Standalone self-contained HTML | `analysis_dashboard_report.py` | `<input>/../report-dashboard/index.html` |
""", ""),
  ("""The standalone HTML can be opened directly. The React app renders a static JSON snapshot and does
not query Spark or Redis in the browser.""",
   """The React app renders a static JSON snapshot and does not query Spark or Redis in the browser."""),
 ],
}
for name, pairs in edits.items():
    p = Path(name)
    t = p.read_text()
    for old, new in pairs:
        assert t.count(old) == 1, f"{name}: no unique match for {old[:60]!r}"
        t = t.replace(old, new)
    p.write_text(t)
    print(f"{name}: {len(pairs)} edit(s)")
PY
```

Expected: three lines confirming 2, 1 and 2 edits.

- [ ] **Step 3: Correct the Node-optional claim**

Viewing the dashboard now requires npm. The line says so, and says what still does not need it.

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("README.md")
t = p.read_text()
old = "- Node.js 18+ / npm (frontend dashboard, optional)\n"
new = "- Node.js 18+ / npm — required to view the analysis dashboard, which is the only surface that renders it. The pipeline and the JSON snapshot it exports need only Python.\n"
assert t.count(old) == 1
p.write_text(t.replace(old, new))
PY
grep -n 'Node.js' README.md
```

Expected: the new line.

- [ ] **Step 4: Verify no live reference to the deleted artifact survives**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
grep -rn 'report-dashboard\|render_html\|svg_bar\|standalone.*HTML\|self-contained' \
  --include='*.py' --include='*.sh' --include='*.md' --include='*.mjs' . \
  | grep -v '.superpowers/\|.planning/\|node_modules'
echo "--- only test_dashboard_single_renderer.py should appear above ---"
```

Expected: matches only in `recsys-pipeline/integration-tests/python_modeling/test_dashboard_single_renderer.py`, whose docstring and `RENDERING_SYMBOLS` name them on purpose.

- [ ] **Step 5: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
python3 -m pytest integration-tests/test_doc_links.py -q 2>&1 | tail -2
(cd frontend && npm run validate:data)
bash -n scripts/run-movie-category-sim.sh && echo "sim syntax clean"
wc -l services/python-modeling/analysis_dashboard_report.py
```

Expected: `563 passed, 2 skipped`; `4 passed` for the doc-link guard; `validate:data` exits 0; `sim syntax clean`; the compute module at 493 lines or fewer.

- [ ] **Step 6: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add README.md recsys-pipeline/README.md recsys-pipeline/frontend/README.md \
        recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md
git commit -m "$(cat <<'MSG'
docs: retire the standalone HTML dashboard from the documentation

Two whole sections described running the deleted script -- "Optional
reference: standalone HTML dashboard" and "Consolidated standalone HTML" --
and the artifact table offered it as a deliverable. Neither heading had an
inbound link.

The root README's Node entry said "(frontend dashboard, optional)". That was
true while a Python renderer existed; with the React dashboard as the only
surface, viewing results requires npm. The line now says that, and says the
pipeline and its JSON export still need only Python.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

## Self-Review

**Spec coverage.** Renderer deletion, `argparse`, docstring → Task 1 steps 3-4. Five tests → Task 1 step 5. Sim step and banner → Task 2 steps 3-4. Nine doc sites → Task 3 steps 1-3 (two whole sections plus five inline edits plus the Node line). The guard → distributed into Tasks 1-2, the recorded deviation. Spec acceptance items 1-8 map to Task 3 step 5 (items 1, 4, 5, 7), Task 1 step 4 (item 2), Task 1 step 6 (item 3), Task 3 step 4 (item 6), and a final `git diff --check` in the PR step.

**Placeholders.** None. Every step carries its exact script, and every replacement asserts its match count before writing, so a drifted line fails loudly instead of silently doing nothing.

**Type consistency.** `REPO`, `MODELING`, `SIM`, `RENDERING_SYMBOLS` are defined once in Task 1 and reused by name in Task 2. The banner string in Task 2 step 4 and the quoted sample output in Task 3 step 2 are byte-identical apart from `$SIM_ROOT` expanding to the concrete path — which is what the README quotes today, so the substitution pattern is unchanged.

**One risk worth stating.** Task 1 step 3 slices by line number and asserts both the file length (742) and the content of line 494 (`def _esc(`) before writing. If either assertion fires, the file has moved since this plan was written and the boundary must be re-derived — do not adjust the index and retry.
