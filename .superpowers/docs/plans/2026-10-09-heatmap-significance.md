# Heatmap Significance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The keyword heatmaps shade only cells whose CTR differs from their grid's rate by at least two standard errors; the rest render neutral, and each grid states how many differ against how many would by chance.

**Architecture:** `cross_tab` adds a per-cell `z` against the grid's pooled CTR. A pure `heatSignal(z)` in `heat-domain.mjs` classifies signal / noise / unknown; `RelevanceHeatmap` applies a `heat-noise` class and a per-grid count note.

**Tech Stack:** Python 3.12 + pandas (pytest); Next 15 / React 19; node-backed tests for the `.mjs` module.

**Spec:** `.superpowers/docs/specs/2026-10-09-heatmap-significance-design.md`

## Global Constraints

- `p0` is per grid: `Σ query_clicks / Σ movie_impressions` over that grid's cells.
- `z = (ctr − p0) / sqrt(p0(1 − p0) / n)` from the unrounded CTR, rounded to 2 dp; `None` when `p0` is 0 or 1.
- Threshold `SIGNAL_Z = 2`, inclusive; chance share for a two-sided |z| ≥ 2 is `0.0455`.
- A missing or non-finite `z` is `"unknown"` and shaded exactly as before this change.
- The shared colour domain, forced-diagonal outline and hatched never-served cells do not change.
- No new npm dependency. Branch guard before every commit. Baseline: `634 passed, 2 skipped`.
- The code PR targets `master` directly.

## Review Focus

1. **A snapshot that predates `z`** — every cell must keep its shade (not turn grey). Task 3's build on the current snapshot expects zero `heat-noise` cells.
2. **The forced diagonal** — a forced cell can also be noise; both classes must apply. Task 3 composes the class list rather than choosing one.
3. **`p0` of exactly 0 or 1** — no division by zero; `z` is `None`. Pinned in Task 1.
4. **RSC payload duplicating class names in the built HTML** — the build check counts only HTML `class="…"` attributes. Task 3/4 greps use `class="[^"]*heat-noise`.
5. **NaN reaching JSON** — pandas writes `None` into a float column as NaN; the exporter's `_json_safe` turns non-finite floats into `null`. Task 4 checks the snapshot has `null`, never `NaN`.

---

### Task 1: `z` on every grid cell

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py` (`compute_keyword` → `cross_tab`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Produces: column `z: float | None` on `grid`, `topic_grid`, `decade_grid`.

- [ ] **Step 1: Write the failing tests**

```python
def test_compute_keyword_grid_cells_carry_z_against_the_grid_rate():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    df = pd.DataFrame({
        "user_id": ["u1", "u2", "u3", "u4"], "session_id": ["s1", "s2", "s3", "s4"],
        "item_id": ["i1", "i2", "i3", "i4"], "label": [1.0, 1.0, 0.0, 0.0],
        "genres": [["Action"], ["Action"], ["Drama"], ["Drama"]],
    })
    rows = {r["keyword"]: r for _, r in dash.compute_keyword(df)["grid"].iterrows()}

    # p0 = 2/4; se = sqrt(0.25 / 2) = 0.3536; z = (1.0 - 0.5) / 0.3536 and its mirror.
    assert rows["Action"]["z"] == 1.41 and rows["Drama"]["z"] == -1.41


def test_compute_keyword_grid_z_is_none_without_variance():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    df = pd.DataFrame({"user_id": ["u1", "u2"], "session_id": ["s1", "s2"], "item_id": ["i1", "i2"],
                       "label": [0.0, 0.0], "genres": [["Action"], ["Drama"]]})

    assert dash.compute_keyword(df)["grid"]["z"].isna().all()
```

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_analysis_dashboard.py -k "carry_z or without_variance"`
Expected: FAIL with `KeyError: 'z'`.

- [ ] **Step 3: Implement**

In `cross_tab`, after the `g["ctr"] = ...` line:

```python
        # Each cell against its own grid's pooled rate, so the heatmap can shade only what
        # differs from it by more than sampling noise. One p0 per grid, matching one ramp.
        impressions = g["movie_impressions"].sum()
        p0 = g["query_clicks"].sum() / impressions if impressions else 0.0
        if 0 < p0 < 1:
            se = (p0 * (1 - p0) / g["movie_impressions"]) ** 0.5
            g["z"] = ((g["query_clicks"] / g["movie_impressions"] - p0) / se).round(2)
        else:
            g["z"] = None
```

- [ ] **Step 4: Run the suite**

Run: `python3 -m pytest -q`
Expected: `636 passed, 2 skipped`.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/analysis_dashboard_report.py recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(keyword): publish each heatmap cell's z against its grid's CTR"
```

### Task 2: `heatSignal` in `heat-domain.mjs`

**Files:**
- Modify: `recsys-pipeline/frontend/components/heat-domain.mjs`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_heat_domain.py`

**Interfaces:**
- Produces: `SIGNAL_Z = 2`, `CHANCE_SHARE = 0.0455`, `heatSignal(z) -> "signal" | "noise" | "unknown"`.

- [ ] **Step 1: Write the failing test**

In `run_js`, change the import line to
`import {{ percentile, heatDomain, heatScore, heatSignal }} from "{MODULE.as_uri()}";`, then append:

```python
def test_heat_signal_gates_on_two_standard_errors():
    """Colour is earned at |z| >= 2; a snapshot without z keeps its old shading."""
    got = run_js(
        'console.log(JSON.stringify([2.0, -2.5, 1.99, -1.0, null, undefined, NaN, "2"]'
        '.map(heatSignal)));'
    )
    assert got == ["signal", "signal", "noise", "noise", "unknown", "unknown", "unknown", "unknown"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_heat_domain.py`
Expected: FAIL — node's stderr reports `does not provide an export named 'heatSignal'`.

- [ ] **Step 3: Implement**

Append to `heat-domain.mjs`:

```js
// Whether a cell's colour is earned. A cell's CTR is compared with its own grid's pooled rate
// (z, computed by the exporter); within two standard errors the difference is what sampling
// alone produces, so the cell is drawn neutral. A snapshot exported before z existed has none,
// and keeps its old shading rather than turning grey on a test that was never run.
export const SIGNAL_Z = 2;
// Two-sided share of cells at |z| >= 2 when nothing differs: the chance expectation.
export const CHANCE_SHARE = 0.0455;

export function heatSignal(z) {
  if (typeof z !== "number" || !Number.isFinite(z)) return "unknown";
  return Math.abs(z) >= SIGNAL_Z ? "signal" : "noise";
}
```

- [ ] **Step 4: Run to verify**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_heat_domain.py && python3 -m pytest -q`
Expected: file passes; suite `637 passed, 2 skipped`.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/frontend/components/heat-domain.mjs recsys-pipeline/integration-tests/python_modeling/test_heat_domain.py
git commit -m "feat(dashboard): heatSignal gates heatmap colour on |z| >= 2"
```

### Task 3: Neutral noise cells and per-grid counts

**Files:**
- Modify: `recsys-pipeline/frontend/components/keyword-report.jsx` (import, `RelevanceHeatmap`, the three heatmap call sites, the shared fine print)
- Modify: `recsys-pipeline/frontend/app/globals.css` (after `table.rpt.heat-grid td.heat-cell`)

**Interfaces:**
- Consumes: `heatSignal`, `SIGNAL_Z`, `CHANCE_SHARE` (Task 2); cell `z` (Task 1).
- Produces: `SignalNote({ rows })`.

- [ ] **Step 1: Import and classify**

Change the import to `import { heatDomain, heatScore, heatSignal, SIGNAL_Z, CHANCE_SHARE } from "./heat-domain.mjs";`. In `RelevanceHeatmap`, replace the shaded-cell return with:

```jsx
                const t = heatScore(cell.ctr, domain);
                const forced = markDiagonal && crossValue === keyword;
                const signal = heatSignal(cell.z);
                const classes = ["num", "heat-cell", forced && "heat-forced", signal === "noise" && "heat-noise"]
                  .filter(Boolean).join(" ");
                return (
                  <td key={crossValue} className={classes}
                    style={{ "--token-score": t }}
                    title={`${crossValue} / ${keyword}: CTR ${share(cell.ctr)} over ${count(cell.movie_impressions)} impressions`
                      + (signal === "unknown" ? "" : ` · z = ${num(cell.z, 2)}`)}>
                    {share(cell.ctr)}
                  </td>
                );
```

Add `num` to the `./format` import if absent.

- [ ] **Step 2: Add `SignalNote` above `RelevanceHeatmap`**

```jsx
// How many of a grid's cells earned their colour, against how many would by chance. Silent for
// a snapshot without z, which is shaded the old way and makes no such claim.
function SignalNote({ rows }) {
  const tested = (rows ?? []).filter((r) => heatSignal(r.z) !== "unknown");
  if (!tested.length) return null;
  const signal = tested.filter((r) => heatSignal(r.z) === "signal").length;
  return (
    <p className="fine-print">
      {signal} of {tested.length} cells differ from this grid&apos;s CTR (|z| ≥ {SIGNAL_Z}); about{" "}
      {Math.round(CHANCE_SHARE * tested.length)} would by chance alone.
    </p>
  );
}
```

and render `<SignalNote rows={data.grid} />`, `<SignalNote rows={data.topic_grid} />`, `<SignalNote rows={data.decade_grid} />` directly after the matching `<RelevanceHeatmap … />`.

- [ ] **Step 3: Extend the shared fine print**

Append to the paragraph that begins "Colour spans CTR":

```jsx
        {" "}Only cells whose CTR differs from their grid&apos;s pooled rate by at least two standard
        errors are shaded; the rest sit on the neutral background with their rate still printed. At
        that threshold about 1 cell in 22 is shaded by chance, which each grid&apos;s count states.
```

- [ ] **Step 4: CSS**

After the `table.rpt.heat-grid td.heat-cell { … }` rule:

```css
/* Within two standard errors of the grid's rate: true, printed, but not a difference. */
table.rpt.heat-grid td.heat-cell.heat-noise {
  background: var(--accent-soft);
}
```

- [ ] **Step 5: Suite, then build on the current snapshot (no `z` yet)**

```bash
python3 -m pytest -q
cd recsys-pipeline/frontend && npm run build 2>&1 | grep -E 'Compiled|Failed|rror' | head -3
f=.next/server/app/demand/keyword.html
echo -n "noise cells (want 0): "; grep -oE 'class="[^"]*heat-noise' "$f" | wc -l | tr -d ' '
echo -n "signal notes (want 0): "; grep -c 'would by chance alone' "$f" || true
echo -n "shaded cells: "; grep -oE 'class="[^"]*heat-cell' "$f" | wc -l | tr -d ' '
```

Expected: `637 passed, 2 skipped`; 0 noise cells; 0 notes; shaded cells = 102 + 237 + 90 = 429.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/frontend/components/keyword-report.jsx recsys-pipeline/frontend/app/globals.css
git commit -m "feat(dashboard): heatmaps draw noise cells neutral and count the cells that differ"
```

### Task 4: Re-export the snapshot and record the evidence

**Files:**
- Modify: `recsys-pipeline/frontend/data/dashboard.json`, this plan's Verification section

Exporter only, against `/tmp/spark-recsys/movie-category-sim` with Redis up. If either is gone, stop and record that rather than re-simulating unasked.

- [ ] **Step 1: Re-export and diff**

```bash
cd recsys-pipeline
cp frontend/data/dashboard.json /tmp/claude-heat-before.json
R=/tmp/spark-recsys/movie-category-sim
REDIS_HOST=localhost REDIS_PORT=6379 python3 frontend/export_dashboard_json.py \
  --input $R/training-samples --output frontend/data/dashboard.json \
  --experiences $R/slates $( [ -s $R/live-metrics.json ] && echo --live-metrics $R/live-metrics.json )
(cd frontend && npm run validate:data)
grep -c NaN frontend/data/dashboard.json || true
python3 -c "
import json; a=json.load(open('/tmp/claude-heat-before.json')); b=json.load(open('frontend/data/dashboard.json'))
print('moved:', [k for k in a if a[k]!=b.get(k)])
for g in ('grid','topic_grid','decade_grid'):
    rows=b['keyword'][g]; sig=sum(abs(r['z'])>=2 for r in rows if r['z'] is not None)
    print(g, len(rows), 'signal', sig, 'noise', sum(r['z'] is not None for r in rows)-sig)
strip=lambda kw: {k:([{f:v for f,v in r.items() if f!='z'} for r in kw[k]] if k in ('grid','topic_grid','decade_grid') else kw[k]) for k in kw}
print('keyword unchanged apart from z:', strip(a['keyword'])==strip(b['keyword']))"
```

Expected: `NaN` count 0; moved = `['keyword', 'freshness']` (freshness only in its two content-age fields); signal 30 / 45 / 18; keyword unchanged apart from z: True.

- [ ] **Step 2: Build and inspect with real data**

```bash
cd frontend && npm run build 2>&1 | grep -E 'Compiled|Failed|rror' | head -3
f=.next/server/app/demand/keyword.html
echo -n "noise cells: "; grep -oE 'class="[^"]*heat-noise' "$f" | wc -l | tr -d ' '
grep -oE '[0-9]+ of [0-9]+ cells differ[^<]*' "$f"
```

Expected: noise cells = 72 + 192 + 72 = 336; three notes reading 30 of 102 / 45 of 237 / 18 of 90. Take a screenshot of the grids and inspect it.

- [ ] **Step 3: Commit the snapshot alone, then the evidence**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/data/dashboard.json
git commit -m "chore(dashboard): re-export with z on every heatmap cell"
```

Fill in Verification, tick the boxes, commit as `docs: record the heatmap-significance plan's verification evidence`.

## Verification

- Suite: _(record)_
- Build on the pre-z snapshot (Task 3): _(record)_
- Re-export (Task 4): _(record)_
- Real-data build (Task 4): _(record)_
