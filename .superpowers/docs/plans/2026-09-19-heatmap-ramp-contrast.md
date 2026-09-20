# Heatmap ramp contrast — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Map CTR to colour over a shared, robust p5–p95 domain instead of `ctr / max`, changing no value.

**Architecture:** The arithmetic moves into a pure `.mjs` module so `node` can test it directly; the component computes one domain from both grids and passes it to each heatmap.

**Tech Stack:** Plain ES module, React client component, pytest driving `node` for the numeric checks.

**Spec:** `.superpowers/docs/specs/2026-09-19-heatmap-ramp-contrast-design.md`

**Status:** executed on this branch as #267. Every task below is complete.

## Global Constraints

- Branch and PR only. `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- **`recsys-pipeline/frontend/data/dashboard.json` must not change.** Acceptance item 6 is `git diff --quiet` against master for that path. No export is run.
- No change to cell values, titles, hatching, the forced-diagonal outline, or the 70% accent cap.
- A dev server may be running on port 3000. Do not `rm -rf .next`.
- CI's python job has no `setup-node`, though ubuntu-latest ships one. The node-driven test must therefore **skip** when `node` is absent, never fail.
- Measured baseline on master: **590 passed, 2 skipped**. This plan adds 4 node tests plus 2 route guards → expect **596 passed, 2 skipped** (an earlier draft of this line said 594, having counted only the node tests) (595/1 skipped ratio shifts if node is missing).

## Pre-validated facts

- Current rendered spread — the numbers acceptance is measured against:
  - category, 102 cells: deciles `[0,0,0,0,1,5,25,33,22,16]`, 6/10 used, 32.4% crowding, range 0.48–1.00
  - topic, 237 cells: deciles `[0,0,0,1,10,48,76,57,38,7]`, 7/10 used, 32.1% crowding, range 0.40–1.00
- Pooled p5/p95 over both grids is ≈ 0.114 / 0.192, so the domain is a real interval, not degenerate.
- `RelevanceHeatmap` currently computes `const max = Math.max(...rows.map((r) => r.ctr ?? 0), 0);` and `const t = max === 0 ? 0 : (cell.ctr ?? 0) / max;`.
- `KeywordSection` renders the two heatmaps from `data.grid` and `data.topic_grid`.
- `validate_measurements.mjs` establishes `.mjs` in this directory.

## File Structure

| File | Responsibility |
|---|---|
| Create: `frontend/components/heat-domain.mjs` | `percentile`, `heatDomain`, `heatScore`. |
| Modify: `frontend/components/keyword-report.jsx` | Compute one domain, pass to both, state it in the legend. |
| Create: `integration-tests/python_modeling/test_heat_domain.py` | Node-driven numeric tests. |
| Modify: `integration-tests/python_modeling/test_dashboard_routes.py` | Shared-domain and legend guards. |

---

### Task 1: The mapping module

**Files:**
- Create: `recsys-pipeline/frontend/components/heat-domain.mjs`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_heat_domain.py`

**Interfaces:**
- Produces: `percentile(sorted, p)`, `heatDomain(groups, loPct?, hiPct?)`, `heatScore(ctr, domain)`. Task 2 imports all three.

- [x] **Step 1: Write the failing tests**

Create `recsys-pipeline/integration-tests/python_modeling/test_heat_domain.py`:

```python
"""The heat mapping is arithmetic, so it is tested by running it, not by scanning JSX.

CI's python job has no setup-node step; ubuntu-latest ships node anyway, so these skip
rather than fail if it is ever absent.
"""
import json
import shutil
import subprocess
from pathlib import Path

import pytest

MODULE = (Path(__file__).parents[2] / "frontend" / "components" / "heat-domain.mjs").resolve()
pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")


def run_js(body: str):
    """Import the real module in node and return whatever the snippet prints as JSON."""
    script = f'import {{ percentile, heatDomain, heatScore }} from "{MODULE.as_uri()}";\n{body}'
    out = subprocess.run(["node", "--input-type=module", "-e", script],
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_percentile_interpolates_linearly():
    # p50 of 1..5 is the middle element; p25 falls between the first and second.
    assert run_js('console.log(JSON.stringify(percentile([1,2,3,4,5], 50)))') == 3
    assert run_js('console.log(JSON.stringify(percentile([1,2,3,4,5], 25)))') == 2
    assert run_js('console.log(JSON.stringify(percentile([0,10], 50)))') == 5


def test_heat_domain_pools_every_group():
    """The two grids share one domain, so a value in either must be able to set an end."""
    got = run_js(
        'const a = [{ctr: 0.10}, {ctr: 0.20}];'
        'const b = [{ctr: 0.30}, {ctr: 0.40}];'
        # 0..100 percentiles so the ends are the pooled extremes, not interpolated ones.
        'console.log(JSON.stringify(heatDomain([a, b], 0, 100)));'
    )
    assert got == [0.10, 0.40], "a group-local domain would have returned [0.10, 0.20]"


def test_heat_score_clamps_outside_the_domain():
    """Clipping is the cost of the robust domain, so it must be exact at the ends."""
    got = run_js(
        'const d = [0.10, 0.20];'
        'console.log(JSON.stringify([heatScore(0.05, d), heatScore(0.10, d),'
        ' heatScore(0.15, d), heatScore(0.20, d), heatScore(0.25, d)]));'
    )
    assert got == [0, 0, 0.5, 1, 1]


def test_heat_score_is_zero_for_a_degenerate_domain():
    """One distinct value everywhere must not divide by zero or render NaN."""
    assert run_js('console.log(JSON.stringify(heatScore(0.1, [0.1, 0.1])))') == 0
    assert run_js('console.log(JSON.stringify(heatScore(null, [0.1, 0.2])))') == 0
```

- [x] **Step 2: Run to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_heat_domain.py -q 2>&1 | tail -3`
Expected: 4 failed — the module does not exist, so node exits non-zero and the assert on `returncode` fires.

- [x] **Step 3: Write the module**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
cat > components/heat-domain.mjs <<'JS'
// How a CTR becomes a shade.
//
// The obvious mapping, ctr / max(ctr), is zero-anchored, and real CTR occupies a narrow band
// well above zero -- so every cell lands in the top half of the ramp and the figure reads as
// uniformly warm. Measured on the shipped snapshot: the category grid used 6 of 10 deciles
// with a third of its cells in one of them.
//
// So the domain is the 5th-95th percentile of the values actually present, shared across every
// grid drawn together, with the ends clipped. Colour stays linear in CTR -- equal steps of
// colour are equal steps of CTR -- which a rank/quantile mapping would not preserve: that would
// give two cells differing by 0.01pp visibly different shades.

/** Linear-interpolated percentile of an ASCENDING array. */
export function percentile(sorted, p) {
  if (!sorted.length) return 0;
  const k = ((sorted.length - 1) * p) / 100;
  const lo = Math.floor(k);
  const hi = Math.min(lo + 1, sorted.length - 1);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (k - lo);
}

/**
 * Pool the ctr values of every row-array and return [lo, hi].
 * Shared across grids on purpose: two heatmaps in one section get read against each other,
 * and a per-grid domain would give the same shade two different meanings.
 */
export function heatDomain(groups, loPct = 5, hiPct = 95) {
  const values = [];
  for (const rows of groups) {
    for (const r of rows ?? []) {
      if (typeof r?.ctr === "number") values.push(r.ctr);
    }
  }
  if (!values.length) return [0, 0];
  values.sort((a, b) => a - b);
  return [percentile(values, loPct), percentile(values, hiPct)];
}

/** Position of ctr within the domain, clamped. Cells outside it saturate. */
export function heatScore(ctr, [lo, hi]) {
  if (typeof ctr !== "number" || !(hi > lo)) return 0;
  return Math.min(1, Math.max(0, (ctr - lo) / (hi - lo)));
}
JS
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest integration-tests/python_modeling/test_heat_domain.py -q 2>&1 | tail -2
```

Expected: 4 passed.

- [x] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/heat-domain.mjs \
        recsys-pipeline/integration-tests/python_modeling/test_heat_domain.py
git commit -m "feat(dashboard): a shared robust domain for heat"
```

---

### Task 2: Use it, and say so in the legend

**Files:**
- Modify: `recsys-pipeline/frontend/components/keyword-report.jsx`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`

**Interfaces:**
- Consumes: `heatDomain`, `heatScore`.

- [x] **Step 1: Write the failing guards**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`:

```python
def test_both_heatmaps_share_one_heat_domain():
    """Per-grid domains would give the same shade two meanings across two figures that
    sit in the same section and get read against each other."""
    report = (FRONTEND / "components" / "keyword-report.jsx").read_text(encoding="utf-8")
    assert "heatDomain" in report and "heatScore" in report, "the mapping module must be used"
    # One call, built from both grids.
    calls = re.findall(r"heatDomain\(([^)]*)\)", report)
    assert len(calls) == 1, f"expected exactly one heatDomain call, found {len(calls)}"
    assert "grid" in calls[0] and "topic_grid" in calls[0], (
        f"the domain must pool both grids, got heatDomain({calls[0]})"
    )
    assert "r.ctr ?? 0) / max" not in report, "the old ctr/max mapping must be gone"


def test_the_heatmap_legend_states_its_domain():
    """The endpoints are no longer implicit, and the extreme cells saturate, so the reader
    has to be told what the colour actually spans."""
    report = (FRONTEND / "components" / "keyword-report.jsx").read_text(encoding="utf-8")
    assert re.search(r"saturat", report, re.I), "the legend must say cells outside the domain saturate"
    # The stated range has to come from the domain, not be a hardcoded string.
    assert re.search(r"share\(\s*domain\[0\]\s*\)", report), "the legend must print the real low end"
    assert re.search(r"share\(\s*domain\[1\]\s*\)", report), "the legend must print the real high end"
```

- [x] **Step 2: Run to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q -k "heat_domain or legend" 2>&1 | tail -3`
Expected: 2 failed.

- [x] **Step 3: Wire it through**

Four edits to `keyword-report.jsx`:

1. Import: `import { heatDomain, heatScore } from "./heat-domain.mjs";`
2. `RelevanceHeatmap` takes `domain` instead of computing `max`:
   - delete `const max = Math.max(...rows.map((r) => r.ctr ?? 0), 0);`
   - replace `const t = max === 0 ? 0 : (cell.ctr ?? 0) / max;` with `const t = heatScore(cell.ctr, domain);`
3. In `KeywordSection`, before the return: `const domain = heatDomain([data.grid, data.topic_grid]);`
   Pass `domain={domain}` to both `RelevanceHeatmap` call sites.
4. Replace the token legend's static wording under the heatmaps with one line naming the domain:

```jsx
<p className="fine-print">
  Colour spans CTR {share(domain[0])}–{share(domain[1])}, the 5th–95th percentile across both
  grids; cells outside that range saturate. Each cell prints its own rate.
</p>
```

- [x] **Step 4: Verify the guards and the whole route file**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -2
```

Expected: 18 passed.

- [x] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/keyword-report.jsx \
        recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py
git commit -m "fix(dashboard): spread heat over a shared robust domain"
```

---

### Task 3: Measure the rendered result against acceptance

**Files:** none modified — this task proves the change worked.

- [x] **Step 1: Build**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
npm run build 2>&1 | grep -E 'Compiled|Failed|error' | head -3
```

- [x] **Step 2: Measure both grids from the rendered HTML**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
python3 - <<'PY'
import re
html = open('.next/server/app/demand/keyword.html', encoding='utf-8').read()
cells = re.findall(r'heat-cell[^>]*?--token-score:([0-9.]+)', html)
vals = [float(v) for v in cells]
# The category grid renders first, so its 102 cells precede the topic grid's 237.
for name, part, before in (("category", vals[:102], (6, 32.4)), ("topic", vals[102:], (7, 32.1))):
    dec = [0]*10
    for v in part:
        dec[min(9, int(v*10))] += 1
    used = sum(1 for c in dec if c)
    crowd = max(dec)/len(part)*100
    print(f"{name:9s} n={len(part):3d}  deciles {used}/10 (was {before[0]}/10)  "
          f"crowding {crowd:.1f}% (was {before[1]}%)  range {min(part):.2f}-{max(part):.2f}")
    print(f"           {dec}")
PY
```

Expected: category 10/10 with crowding ≤ 20%; topic still 10/10 and no worse than 32.1%.
**If the category grid does not reach 10/10, stop** — the change failed its primary acceptance case
and the domain choice needs revisiting rather than the numbers being explained away.

- [x] **Step 3: Prove no data moved**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git diff --quiet master -- recsys-pipeline/frontend/data/dashboard.json \
  && echo "dashboard.json identical to master - no value changed" \
  || { echo "FAIL: the snapshot moved; this PR must not touch it"; exit 1; }
```

- [x] **Step 4: Full gates**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
(cd frontend && npm run validate:data)
git -C .. diff --check && echo "diff --check clean"
```

Expected: `594 passed, 2 skipped`.

## Self-Review

**Spec coverage.** Acceptance 1 and 2 → Task 3 step 2, which prints the before/after side by side rather than asserting a bare pass. 3 → Task 1's clamp test. 4 → Task 2's shared-domain guard, which counts the `heatDomain` calls so a second per-grid call fails. 5 → Task 2's legend guard, which requires the printed endpoints to come from `domain[...]` rather than a literal. 6 → Task 3 step 3. 7 → Task 3 step 4.

**Placeholders.** None. Task 2 step 3 gives prose plus exact code for the edits, because the two substitutions are one-liners inside a function the diff already shows.

**Type consistency.** `heatDomain` returns `[lo, hi]`; `heatScore(ctr, domain)` destructures that pair; the component passes the same `domain` array to both call sites and reads `domain[0]`/`domain[1]` for the legend.

**Risks.** The rendered-HTML measurement assumes the category grid's 102 cells render before the topic grid's 237. That is true of the current JSX order and is the only place the plan depends on it; if the order is ever swapped the numbers will look obviously wrong (a 237-cell "category" grid) rather than silently mislead.

The node-driven tests are new to this repo. They skip rather than fail when node is missing, so the worst case in CI is a silent loss of coverage, not a red build — which is the right trade for a job that never declared node as a dependency.
