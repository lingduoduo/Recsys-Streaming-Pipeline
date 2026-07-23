# Modern Analysis Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the generated dashboard as a responsive light product-analytics UI while retaining one portable HTML file.

**Architecture:** Keep rendering in `analysis_dashboard_report.py`. Add semantic wrappers to existing output and provide the full visual system as inline CSS; computations and CLI behavior remain unchanged.

**Tech Stack:** Python 3, pandas, inline HTML/CSS/SVG, pytest

## Global Constraints

- Preserve `index.html` and the current CLI.
- Add no JavaScript, external assets, or runtime dependencies.
- Keep metrics, ordering, content, and escaping behavior unchanged.
- Support narrow screens and reduced-motion preferences.

---

### Task 1: Semantic product-analytics structure

**Files:**
- Modify: `services/python-modeling/analysis_dashboard_report.py:238-274`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: existing `html_table`, `section`, `na_card`, and `render_html` signatures.
- Produces: the same signatures with `table-shell`, `report-card`, `insight`, `status-card`, `page-shell`, and `hero` hooks.

- [ ] **Step 1: Write the failing test**

```python
def test_render_html_uses_modern_product_analytics_structure():
    import analysis_dashboard_report as dash
    import pandas as pd
    table = dash.html_table(pd.DataFrame([{"metric": "ctr", "value": 0.42}]))
    page = dash.render_html("Analysis Dashboard", [
        dash.section("Engagement", "CTR 42%", table),
        dash.na_card("Ranking", "no embeddings"),
    ])
    for marker in ('<meta name="viewport"', 'class="page-shell"',
                   'class="hero"', 'class="report-card"',
                   'class="insight"', 'class="table-shell"',
                   'class="report-card status-card"'):
        assert marker in page
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_render_html_uses_modern_product_analytics_structure -v`

Expected: FAIL on the first absent structural marker.

- [ ] **Step 3: Implement minimal semantic markup**

Wrap tables in `<div class="table-shell">`. Emit normal sections as `<section class="report-card">` with `section-heading`, `insight`, and `section-body` children. Emit unavailable sections as `<section class="report-card status-card">`. Add viewport metadata, a `page-shell`, an indigo `hero`, the escaped title, descriptive subtitle, `OFFLINE REPORT` badge, and `report-grid` around sections. Continue passing all data-derived strings through `_esc`.

- [ ] **Step 4: Verify GREEN**

Run the Step 2 command. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): add semantic analytics layout"
```

---

### Task 2: Responsive visual system and chart palette

**Files:**
- Modify: `services/python-modeling/analysis_dashboard_report.py:203-290`
- Test: `integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: Task 1 hooks and existing `svg_bar`/`svg_line` functions.
- Produces: responsive inline CSS and SVG charts using indigo, teal, amber, and rose.

- [ ] **Step 1: Write the failing visual test**

```python
def test_render_html_embeds_responsive_visual_system():
    import analysis_dashboard_report as dash
    page = dash.render_html("Dashboard", [dash.section("S", "H", "B")])
    bar = dash.svg_bar(["click"], [12], title="Funnel")
    line = dash.svg_line([5, 10], {"hybrid": [0.2, 0.4]}, title="Recall")
    assert "--canvas:#f5f7fb" in page
    assert "--indigo:#4f46e5" in page
    assert "@media (max-width:700px)" in page
    assert "prefers-reduced-motion:reduce" in page
    assert 'class="chart"' in bar and 'rx="6"' in bar
    assert 'class="chart"' in line and "#4f46e5" in line
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py::test_render_html_embeds_responsive_visual_system -v`

Expected: FAIL on the missing canvas variable.

- [ ] **Step 3: Modernize SVGs**

Add `class="chart" role="img"` to both SVG roots. Give bars `rx="6"` and fill `#4f46e5`. Set line colors to `#4f46e5`, `#0d9488`, `#f59e0b`, and `#e11d48`; use width 3 with round caps and joins.

- [ ] **Step 4: Implement inline CSS**

Start with these exact tokens:

```css
:root{--canvas:#f5f7fb;--surface:#fff;--ink:#111827;--muted:#64748b;--line:#e2e8f0;--indigo:#4f46e5;--indigo-soft:#eef2ff;--amber:#b45309;--amber-soft:#fffbeb;--shadow:0 12px 30px rgba(15,23,42,.07)}
```

Style a centered 1180px page, indigo gradient hero, white rounded cards, indigo insight pills, amber status cards, responsive charts, and scrollable tables with sticky uppercase headers, tabular numbers, zebra rows, and hover states. Add `@media (max-width:700px)` to stack the hero and reduce padding, plus `@media (prefers-reduced-motion:reduce)` to disable transitions and smooth scrolling.

- [ ] **Step 5: Verify the dashboard suite**

Run: `python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q`

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): modernize analytics report UI"
```

---

### Task 3: Regenerate and verify localhost results

**Files:**
- Generate: `report-dashboard/index.html` (local artifact; do not commit)
- Reference: `docs/superpowers/specs/2026-07-06-modern-analysis-dashboard-design.md`

**Interfaces:**
- Consumes: the dashboard CLI and `/tmp/spark-recsys/movie-category-sim/training-samples`.
- Produces: refreshed output at `http://localhost:8000`.

- [ ] **Step 1: Regenerate HTML**

```bash
REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py \
  --input /tmp/spark-recsys/movie-category-sim/training-samples \
  --outdir report-dashboard
```

Expected: `wrote report-dashboard/index.html`. If Redis is stopped, recall and ranking may render documented N/A cards.

- [ ] **Step 2: Verify content and portability**

```bash
rg -n "Engagement funnel|Keyword gap|Query intent|Recall|Ranking|report-card|table-shell" report-dashboard/index.html
rg -n "<script|https?://" report-dashboard/index.html
```

Expected: the first command finds all sections/hooks; the second exits 1 with no matches.

- [ ] **Step 3: Serve or refresh the report**

Run: `python -m http.server 8000 --directory report-dashboard`

Expected: `Serving HTTP on ... port 8000`. If port 8000 already hosts this directory, keep that process and reload.

- [ ] **Step 4: Verify the Web UI response**

Run: `curl -fsS http://localhost:8000/ | rg "Analysis Dashboard|report-card|table-shell"`

Expected: exit code 0 with matching HTML.

- [ ] **Step 5: Final checks**

```bash
python -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q
git diff --check
git status --short
```

Expected: tests pass, diff check is silent, and `report-dashboard/` is the only intentional generated output.
