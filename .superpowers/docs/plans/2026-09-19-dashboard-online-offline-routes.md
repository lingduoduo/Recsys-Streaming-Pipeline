# Split the dashboard into online and offline prediction analysis — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single thirteen-section page with three routes — an overview scorecard, an online prediction analysis page, and an offline one — driven by a single map of section to route.

**Architecture:** `components/sections.jsx` divides into four contiguous line ranges with no interleaving, so the split is a mechanical extraction rather than a rewrite: formatters, scorecard, the seven measurement sections, the five diagnostics. A new `components/groups.js` holds the section-to-route map that the scorecard's tile links, the nav, and the guard test all read.

**Tech Stack:** Next.js 15.1.6 app router, React 19 server components with one client component for the nav, plain CSS in `app/globals.css`. Tests are Python (`pytest.ini` sets `testpaths = integration-tests`) scanning JSX sources, plus `npm run build` as the compile gate.

**Spec:** `.superpowers/docs/specs/2026-09-19-dashboard-online-offline-routes-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `validate_measurements.mjs`, `export_dashboard_json.py` and every Python compute function are untouched. This change is JSX, CSS, one JS module, and tests.
- `components/ui.jsx` keeps every export and signature it has, including `MetricTile`'s `href` prop.
- `components/keyword-report.jsx` is not split or moved; `/online` imports `KeywordSection` from it.
- Every section component keeps its exact exported name: `RelevanceSection`, `SatisfactionSection`, `FreshnessSection`, `DiversitySection`, `FairnessSection`, `SafetySection`, `LatencySection`, `EngagementSection`, `QuerySection`, `RecallSection`, `RankingSection`, `OpeSection`, `KeywordSection`, `Scorecard`.
- `HEADLINES`, `TITLES` and `LOW_COVERAGE` keep their exact names and shapes: six contract tests parse them out of the component sources by name.
- Only `components/nav.jsx` is a client component. The pages and every section stay server components.
- Measured baseline at this branch point: **570 passed, 2 skipped**. This plan adds four tests, so the final expected result is **574 passed, 2 skipped**.

## Pre-validated facts

Measured against the current file before this plan was written — do not re-derive:

- `components/sections.jsx` is 627 lines and splits into four contiguous ranges with nothing interleaved: **6-44** formatters plus `maxByField`, **46-118** scorecard support plus `Scorecard`, **120-462** `MeasurementSection` plus the seven measurement sections, **464-627** the five diagnostics. Lines 1-4 are the `./ui` import; 5, 45, 119 and 463 are blank separators.
- Each range's real dependencies, counted by call site rather than by word match:
  - `scorecard.jsx` calls `num`, `share`, `maxByField`; uses `MetricTile`.
  - `measurements.jsx` calls `num`, `share`, `maxByField`; uses `Section`, `NaCard`, `BarChart`, `GroupedBarChart`, `DataTable`, `MetricGrid`, `MetricCard`, `ChartGrid`.
  - `diagnostics.jsx` calls `num`, `pct`, `share`, `ci`, `count`, `rankBy` and references `COUNT_COLUMNS`, `RATE_COLUMNS`; uses `Section`, `NaCard`, `BarChart`, `DataTable`, `MetricGrid`, `MetricCard`, `ChartGrid`.
- Two word-match false positives to avoid importing: `pct` appears in `scorecard.jsx`'s range only inside the string `format: "pct"`, and `count` appears in `measurements.jsx`'s range only inside the column name `ndcg_evaluated_slate_count`. Neither is called.
- `test_dashboard_measurement_contract.py` reads `components/sections.jsx` as text at lines 277, 326, 351, 385, 431 and 487.

## File Structure

| File | Responsibility |
|---|---|
| Create: `frontend/components/groups.js` | `ROUTES` and `SECTION_ROUTE` — the only place that says where a section lives. |
| Create: `frontend/components/format.js` | Pure formatters: `num`, `pct`, `share`, `ci`, `count`, `rankBy`, `maxByField`, `COUNT_COLUMNS`, `RATE_COLUMNS`. No JSX. |
| Create: `frontend/components/scorecard.jsx` | `HEADLINES`, `TITLES`, `LOW_COVERAGE`, the `headline*` helpers, `Scorecard`. |
| Create: `frontend/components/measurements.jsx` | `MeasurementSection` and the seven sections built from it. |
| Create: `frontend/components/diagnostics.jsx` | `EngagementSection`, `QuerySection`, `RecallSection`, `RankingSection`, `OpeSection`. |
| Delete: `frontend/components/sections.jsx` | Split into the four files above. |
| Create: `frontend/components/nav.jsx` | The only client component: one link per `ROUTES` entry, active state from `usePathname`. |
| Modify: `frontend/app/layout.jsx` | Shared shell: hero with row count and input path, then `<Nav />`, then `{children}`. |
| Modify: `frontend/app/page.jsx` | Overview: `<Scorecard />` only. |
| Create: `frontend/app/online/page.jsx` | Nine sections under three subheadings. |
| Create: `frontend/app/offline/page.jsx` | Four sections under two subheadings. |
| Modify: `frontend/app/globals.css` | `.nav`, `.nav a`, `.nav a.nav-active`, `.group-heading`. |
| Modify: `frontend/integration-tests/.../test_dashboard_measurement_contract.py` | Six read sites use one concatenating helper. |
| Create: `frontend/integration-tests/.../test_dashboard_routes.py` | The four guard assertions. |
| Modify: `frontend/README.md` | Structure listing and Run section describe three routes. |

---

### Task 1: The contract tests stop depending on one file's name

**Files:**
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py` (lines 277, 326, 351, 385, 431, 487)

**Interfaces:**
- Consumes: the component files created in Task 1.
- Produces: a module-level `_component_sources()` helper the six sites call.

- [ ] **Step 1: Record what the six tests assert today**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_measurement_contract.py -q 2>&1 | tail -2`
Expected: 15 passed. This task changes *where* those tests read from, not what they assert, so the
count must be identical before and after. It goes first precisely because it is invariant across the
split: `_component_sources()` concatenates `components/*.jsx`, which is `sections.jsx` now and the four
new files after Task 2.

- [ ] **Step 2: Add the concatenating helper and repoint the six reads**

The assertion each test makes is "the dashboard's components declare this column", not "this one file does". Joining with newlines keeps line-anchored patterns honest.

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("integration-tests/python_modeling/test_dashboard_measurement_contract.py")
t = p.read_text()

old_read = '    sections = (_REPO / "recsys-pipeline" / "frontend" / "components" / "sections.jsx").read_text()'
count = t.count(old_read)
assert count == 6, f"expected 6 read sites, found {count}"
t = t.replace(old_read, "    sections = _component_sources()")

# The helper goes after the _REPO definition so it can use it.
anchor = "_REPO = "
index = t.index(anchor)
line_end = t.index("\n", index) + 1
helper = '''

def _component_sources() -> str:
    """Every dashboard component's source, concatenated.

    These tests check that the React sections declare the columns the Python exporter
    publishes -- a cross-language contract with no compiler to enforce it. They used to
    read components/sections.jsx, which was split by responsibility; the contract is
    about what the components declare, not which file declares it, so they read all of
    them. Joined with newlines so line-anchored patterns stay honest.
    """
    components = sorted((_REPO / "recsys-pipeline" / "frontend" / "components").glob("*.jsx"))
    assert components, "no dashboard components found"
    return "\\n".join(path.read_text(encoding="utf-8") for path in components)
'''
t = t[:line_end] + helper + t[line_end:]

# Two assertion messages name the retired file.
t = t.replace('"sections.jsx must declare a HEADLINES map"',
              '"the dashboard components must declare a HEADLINES map"')
t = t.replace('"no Relevance section in sections.jsx"',
              '"no Relevance section in the dashboard components"')
t = t.replace('"no Satisfaction section in sections.jsx"',
              '"no Satisfaction section in the dashboard components"')
p.write_text(t)
print("six read sites now use _component_sources()")
PY
grep -c '_component_sources()' integration-tests/python_modeling/test_dashboard_measurement_contract.py
```

Expected: `7` — the definition plus six call sites.

- [ ] **Step 3: Run the contract tests to verify they pass**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_measurement_contract.py -q 2>&1 | tail -2`
Expected: 15 passed.

- [ ] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py
git commit -m "$(cat <<'MSG'
test: read the contract from every dashboard component, not one file

Six sites read components/sections.jsx as text to check that React's declared
columns match what the exporter publishes. That file was split by
responsibility, so they now read every components/*.jsx concatenated -- which
is what the assertion always meant, and survives the next split.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: The shared mapping and the component split

**Files:**
- Create: `recsys-pipeline/frontend/components/groups.js`
- Create: `recsys-pipeline/frontend/components/format.js`
- Create: `recsys-pipeline/frontend/components/scorecard.jsx`
- Create: `recsys-pipeline/frontend/components/measurements.jsx`
- Create: `recsys-pipeline/frontend/components/diagnostics.jsx`
- Delete: `recsys-pipeline/frontend/components/sections.jsx`

**Interfaces:**
- Consumes: nothing.
- Produces: `ROUTES` (array of `{href, label}`) and `SECTION_ROUTE` (object mapping the 13 section keys to `"/online"` or `"/offline"`) from `./groups`; the formatters from `./format`; `Scorecard` from `./scorecard`; the seven measurement components from `./measurements`; the five diagnostic components from `./diagnostics`. Tasks 2, 3 and 5 import these by exactly these names.

- [ ] **Step 1: Write the mapping module**

```bash
cd recsys-pipeline/frontend
cat > components/groups.js <<'JS'
// Where each dashboard section lives. One map, three readers: Scorecard builds its tile
// hrefs from it, Nav renders ROUTES, and test_dashboard_routes.py checks that every key
// here is mounted on the page it names.
//
// The split is by what a number means, not where its data came from. "Online" sections
// describe what the serving path actually did; "offline" sections are models re-scored
// afterwards. Only latency is purely live telemetry -- satisfaction, freshness and safety
// are offline rows with live ones merged in, and intents and the off-policy arms are
// computed from logged Parquet.
export const ROUTES = [
  { href: "/", label: "Overview" },
  { href: "/online", label: "Online prediction analysis" },
  { href: "/offline", label: "Offline prediction analysis" },
];

export const SECTION_ROUTE = {
  query: "/online",
  keyword: "/online",
  engagement: "/online",
  satisfaction: "/online",
  freshness: "/online",
  diversity: "/online",
  fairness: "/online",
  safety: "/online",
  latency: "/online",
  recall: "/offline",
  ranking: "/offline",
  ope: "/offline",
  relevance: "/offline",
};
JS
node -e "import('./components/groups.js').then(m => console.log('sections mapped:', Object.keys(m.SECTION_ROUTE).length, '| routes:', m.ROUTES.length))"
```

Expected: `sections mapped: 13 | routes: 3`

- [ ] **Step 2: Extract the four ranges**

The ranges are contiguous and verified; each extraction asserts its own first line before writing, so a drifted file fails loudly instead of producing a scrambled module.

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path

src = Path("components/sections.jsx")
lines = src.read_text().splitlines(keepends=True)
assert len(lines) == 627, f"expected 627 lines, found {len(lines)}"

def block(start, end, first_expected):
    """Lines start..end inclusive, 1-indexed, asserting the first line's opening text."""
    assert lines[start - 1].startswith(first_expected), \
        f"line {start} is not {first_expected!r}: {lines[start - 1]!r}"
    return "".join(lines[start - 1:end]).rstrip("\n") + "\n"

fmt = block(6, 44, "const num =")
scorecard = block(46, 118, "// Which single number")
measurements = block(120, 462, "// One consistent presentation")
diagnostics = block(464, 627, "export function EngagementSection")

# format.js: the consts and maxByField become exports; nothing is imported.
fmt = fmt.replace("const num =", "export const num =", 1)
for name in ("pct", "share", "ci", "count", "rankBy", "COUNT_COLUMNS", "RATE_COLUMNS"):
    fmt = fmt.replace(f"\nconst {name} ", f"\nexport const {name} ", 1)
fmt = fmt.replace("\nfunction maxByField(", "\nexport function maxByField(", 1)
Path("components/format.js").write_text(
    "// Formatting and ranking helpers shared by every dashboard section. No JSX, no data access:\n"
    "// every function here turns a value into a string or orders rows.\n\n" + fmt)

Path("components/scorecard.jsx").write_text(
    'import { MetricTile } from "./ui";\n'
    'import { SECTION_ROUTE } from "./groups";\n'
    'import { num, share, maxByField } from "./format";\n\n' + scorecard)

Path("components/measurements.jsx").write_text(
    'import {\n'
    '  Section, NaCard, BarChart, GroupedBarChart, DataTable,\n'
    '  MetricGrid, MetricCard, ChartGrid,\n'
    '} from "./ui";\n'
    'import { num, share, maxByField } from "./format";\n\n' + measurements)

Path("components/diagnostics.jsx").write_text(
    'import {\n'
    '  Section, NaCard, BarChart, DataTable, MetricGrid, MetricCard, ChartGrid,\n'
    '} from "./ui";\n'
    'import {\n'
    '  num, pct, share, ci, count, rankBy, COUNT_COLUMNS, RATE_COLUMNS,\n'
    '} from "./format";\n\n' + diagnostics)

# A barrel keeps app/page.jsx resolving until Task 3 rewires the routes, so this
# commit builds on its own. Task 3 deletes it.
src.write_text(
    '// Temporary re-export barrel: app/page.jsx still imports from here until the routes\n'
    '// are rewired. Deleted in the commit that adds /online and /offline.\n'
    'export { Scorecard } from "./scorecard";\n'
    'export * from "./measurements";\n'
    'export * from "./diagnostics";\n')
print("split into format.js, scorecard.jsx, measurements.jsx, diagnostics.jsx (+ barrel)")
PY
wc -l components/format.js components/scorecard.jsx components/measurements.jsx components/diagnostics.jsx
```

Expected: four files, roughly 41, 76, 346 and 167 lines, plus `sections.jsx` reduced to a five-line barrel.

Then confirm this commit still builds: `npm run build 2>&1 | tail -4` succeeds, because `app/page.jsx`
resolves through the barrel.

- [ ] **Step 3: Verify every moved component is still exported exactly once**

```bash
cd recsys-pipeline/frontend
for name in Scorecard RelevanceSection SatisfactionSection FreshnessSection DiversitySection \
            FairnessSection SafetySection LatencySection EngagementSection QuerySection \
            RecallSection RankingSection OpeSection; do
  n=$(grep -rc "export function $name" components/*.jsx | awk -F: '{s+=$2} END {print s}')
  printf '%-22s %s\n' "$name" "$n"
done
```

Expected: `1` for all thirteen.

- [ ] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/
git commit -m "$(cat <<'MSG'
refactor(dashboard): split sections.jsx by responsibility

627 lines held the formatters, the scorecard, the seven measurement sections and
the five diagnostics, and every route would have imported all of it. The file
divided into four contiguous ranges with nothing interleaved, so this is an
extraction, not a rewrite.

groups.js is new and is the point: one map from section to route, read by the
scorecard's tile links, the nav, and the guard test. Placing a section is now a
one-line change in one file.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: The three routes and the nav

**Files:**
- Modify: `recsys-pipeline/frontend/app/layout.jsx`
- Modify: `recsys-pipeline/frontend/app/page.jsx`
- Create: `recsys-pipeline/frontend/app/online/page.jsx`
- Create: `recsys-pipeline/frontend/app/offline/page.jsx`
- Create: `recsys-pipeline/frontend/components/nav.jsx`
- Modify: `recsys-pipeline/frontend/app/globals.css`

**Interfaces:**
- Consumes: `ROUTES` from `./groups`; `Scorecard` from `./scorecard`; the seven from `./measurements`; the five from `./diagnostics`; `KeywordSection` from `./keyword-report`.
- Produces: the routes `/`, `/online`, `/offline`. Task 5's guard scans `app/**/page.jsx` for component names.

- [ ] **Step 1: Write the nav**

```bash
cd recsys-pipeline/frontend
cat > components/nav.jsx <<'JSX'
"use client";

import { usePathname } from "next/navigation";
import { ROUTES } from "./groups";

// The only client component in the app: it needs the current path to mark the active
// link. Every page and section stays a server component.
export function Nav() {
  const pathname = usePathname();
  return (
    <nav className="nav" aria-label="Dashboard sections">
      {ROUTES.map(({ href, label }) => {
        const active = pathname === href;
        return (
          <a key={href} href={href} className={active ? "nav-active" : undefined}
             aria-current={active ? "page" : undefined}>
            {label}
          </a>
        );
      })}
    </nav>
  );
}
JSX
```

- [ ] **Step 2: Move the hero into the shared layout**

```bash
cd recsys-pipeline/frontend
cat > app/layout.jsx <<'JSX'
import "./globals.css";
import data from "../data/dashboard.json";
import { Nav } from "../components/nav";

export const metadata = {
  title: "Recsys Analysis Dashboard",
  description: "Engagement, intent, retrieval, ranking, and offline policy evaluation.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <main className="page-shell">
          <header className="hero">
            <div>
              <span className="eyebrow">RECOMMENDER ANALYTICS</span>
              <h1>Recsys Analysis Dashboard</h1>
              <p>
                {data.rows.toLocaleString()} rows from <code>{data.input}</code>.
              </p>
            </div>
            <span className="report-badge">NEXT.JS</span>
          </header>
          <Nav />
          {children}
        </main>
      </body>
    </html>
  );
}
JSX
```

- [ ] **Step 3: Write the three pages**

```bash
cd recsys-pipeline/frontend
cat > app/page.jsx <<'JSX'
import data from "../data/dashboard.json";
import { Scorecard } from "../components/scorecard";

export default function Page() {
  return (
    <div className="report-grid">
      <Scorecard data={data} />
    </div>
  );
}
JSX

mkdir -p app/online app/offline

cat > app/online/page.jsx <<'JSX'
import data from "../../data/dashboard.json";
import {
  SatisfactionSection, FreshnessSection, DiversitySection, FairnessSection,
  SafetySection, LatencySection,
} from "../../components/measurements";
import { EngagementSection, QuerySection } from "../../components/diagnostics";
import { KeywordSection } from "../../components/keyword-report";

// What the serving path actually did. Every number here describes served traffic, as
// opposed to a model re-scored afterwards -- see components/groups.js.
export default function Page() {
  return (
    <div className="report-grid">
      <h2 className="group-heading">Demand — what was asked for</h2>
      <QuerySection data={data.query} />
      <KeywordSection data={data.keyword} />
      <EngagementSection data={data.engagement} />

      <h2 className="group-heading">Served quality — what came back</h2>
      <SatisfactionSection data={data.satisfaction} />
      <FreshnessSection data={data.freshness} />
      <DiversitySection data={data.diversity} />
      <FairnessSection data={data.fairness} />
      <SafetySection data={data.safety} />

      <h2 className="group-heading">Serving health</h2>
      <LatencySection data={data.latency} />
    </div>
  );
}
JSX

cat > app/offline/page.jsx <<'JSX'
import data from "../../data/dashboard.json";
import { RelevanceSection } from "../../components/measurements";
import { RecallSection, RankingSection, OpeSection } from "../../components/diagnostics";

// Models re-scored after the fact: what a policy or retriever would have done, measured
// against logged outcomes -- see components/groups.js.
export default function Page() {
  return (
    <div className="report-grid">
      <h2 className="group-heading">Retrieval and ranking</h2>
      <RecallSection data={data.recall} />
      <RankingSection data={data.ranking} />
      <RelevanceSection data={data.relevance} />

      <h2 className="group-heading">Policy</h2>
      <OpeSection data={data.ope} />
    </div>
  );
}
JSX
```

- [ ] **Step 4: Add the nav and subheading styles**

```bash
cd recsys-pipeline/frontend
cat >> app/globals.css <<'CSS'

/* Route nav in the shared layout. Three links; the active one is marked by class and by
   aria-current, because a colour change alone does not reach a screen reader. */
.nav {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin: 0 0 1.5rem;
  border-bottom: 1px solid var(--border);
  padding-bottom: 0.75rem;
}

.nav a {
  padding: 0.45rem 0.9rem;
  border-radius: 999px;
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--muted);
  text-decoration: none;
  border: 1px solid transparent;
}

.nav a:hover {
  color: var(--ink);
  border-color: var(--border);
}

.nav a.nav-active {
  color: #ffffff;
  background: #0f172a;
  border-color: #0f172a;
}

/* Groups the sections within a route. Sits in the report grid, spanning its full width. */
.group-heading {
  grid-column: 1 / -1;
  margin: 1.5rem 0 0;
  font-size: 0.8rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--muted);
}
CSS
grep -c 'nav-active\|group-heading' app/globals.css
```

Expected: `2` or more.

- [ ] **Step 5: Verify the build and the route list**

Run: `cd recsys-pipeline/frontend && npm run build 2>&1 | tail -20`
Expected: a successful build whose route table lists `/`, `/online` and `/offline`.

If a build error names a missing import, the extraction in Task 1 got a dependency wrong — fix the import header in the file named, do not add the symbol back to `format.js`.

- [ ] **Step 6: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git rm -q recsys-pipeline/frontend/components/sections.jsx
git add recsys-pipeline/frontend/app recsys-pipeline/frontend/components/nav.jsx
git commit -m "$(cat <<'MSG'
feat(dashboard): three routes instead of one thirteen-section page

The temporary re-export barrel goes with them: nothing imports sections.jsx now.

/ is the scorecard. /online holds the nine sections that describe served
traffic, under three subheadings. /offline holds the four that re-score a model
against logged outcomes.

The hero moves into the shared layout, which also renders the nav -- the only
client component, since marking the active link needs the current path.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 4: The scorecard's cross-page links

**Files:**
- Modify: `recsys-pipeline/frontend/components/scorecard.jsx`

**Interfaces:**
- Consumes: `SECTION_ROUTE` from `./groups`, already imported by Task 1's header.
- Produces: nothing.

- [ ] **Step 1: Confirm the anchors are currently page-local**

Run: `cd recsys-pipeline/frontend && grep -n 'href=' components/scorecard.jsx`
Expected: `href={`#${key}`}` — a bare fragment, which resolved when every section was on one page and now reaches nothing for twelve of the thirteen.

- [ ] **Step 2: Derive the href from the mapping**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/scorecard.jsx")
t = p.read_text()
old = "            href={`#${key}`}"
new = "            href={`${SECTION_ROUTE[key]}#${key}`}"
assert t.count(old) == 1, "the scorecard's href line does not match verbatim"
p.write_text(t.replace(old, new))
print("tile hrefs now resolve through SECTION_ROUTE")
PY
grep -n 'SECTION_ROUTE' components/scorecard.jsx
```

Expected: the import from Task 1 and the new href line.

- [ ] **Step 3: Verify each tile resolves to the page that holds its section**

```bash
cd recsys-pipeline/frontend
node -e "
import('./components/groups.js').then(({ SECTION_ROUTE }) => {
  const tiles = ['relevance','satisfaction','freshness','diversity','fairness','safety','latency'];
  for (const key of tiles) console.log(key.padEnd(14), SECTION_ROUTE[key] + '#' + key);
});
"
```

Expected: `relevance → /offline#relevance`, the other six → `/online#<key>`.

- [ ] **Step 4: Rebuild**

Run: `cd recsys-pipeline/frontend && npm run build 2>&1 | tail -6`
Expected: successful build.

- [ ] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/scorecard.jsx
git commit -m "$(cat <<'MSG'
fix(dashboard): point the scorecard tiles at the page holding each section

The tiles used bare `#key` anchors, which worked while all thirteen sections
shared one page. Six now resolve to /online and relevance to /offline, derived
from SECTION_ROUTE rather than written per tile -- so moving a section between
pages cannot leave a tile pointing at the wrong one.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 5: The guard and the documentation

**Files:**
- Create: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`
- Modify: `recsys-pipeline/frontend/README.md`

**Interfaces:**
- Consumes: `SECTION_ROUTE` from `components/groups.js`, parsed out of the source; the pages from Task 2.
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

```python
"""Every dashboard section is mounted on exactly one page, on the page its route names.

The dashboard was one page with thirteen sections. Splitting it across /online and
/offline meant moving components between files, which is how a section gets silently
dropped: nothing compiles a JSX import that no page renders, and the page still builds.

components/groups.js is the single source of truth. These tests hold the pages to it.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FRONTEND = REPO / "frontend"
GROUPS = FRONTEND / "components" / "groups.js"

# Section key -> the component that renders it. The key is what groups.js maps to a route
# and what dashboard.json calls the section; the component is what a page mounts.
COMPONENTS = {
    "query": "QuerySection", "keyword": "KeywordSection", "engagement": "EngagementSection",
    "satisfaction": "SatisfactionSection", "freshness": "FreshnessSection",
    "diversity": "DiversitySection", "fairness": "FairnessSection", "safety": "SafetySection",
    "latency": "LatencySection", "recall": "RecallSection", "ranking": "RankingSection",
    "ope": "OpeSection", "relevance": "RelevanceSection",
}


def _section_route():
    """SECTION_ROUTE parsed out of groups.js, so the test reads the same map the app does."""
    source = GROUPS.read_text(encoding="utf-8")
    body = re.search(r"export const SECTION_ROUTE = \{(.*?)\};", source, re.S)
    assert body, "groups.js must export a SECTION_ROUTE map"
    return dict(re.findall(r"(\w+):\s*\"([^\"]+)\"", body.group(1)))


def _pages():
    """Each route path mapped to its page source."""
    pages = {}
    for path in sorted(FRONTEND.glob("app/**/page.jsx")):
        route = "/" + str(path.parent.relative_to(FRONTEND / "app")).replace(".", "").strip("/")
        pages[route.rstrip("/") or "/"] = path.read_text(encoding="utf-8")
    return pages


def test_every_section_is_mounted_on_the_page_its_route_names():
    routes, pages = _section_route(), _pages()
    missing = []
    for key, route in sorted(routes.items()):
        component = COMPONENTS[key]
        if f"<{component}" not in pages.get(route, ""):
            missing.append(f"{key}: groups.js says {route}, but {route} does not mount <{component}")
    assert not missing, "sections not mounted where the map says:\n" + "\n".join(missing)


def test_no_section_is_mounted_on_two_pages():
    pages = _pages()
    duplicated = []
    for key, component in sorted(COMPONENTS.items()):
        mounted = [route for route, source in pages.items() if f"<{component}" in source]
        if len(mounted) > 1:
            duplicated.append(f"{component} on {', '.join(sorted(mounted))}")
    assert not duplicated, "sections mounted more than once:\n" + "\n".join(duplicated)


def test_every_exported_section_component_has_a_route():
    """A new section that nobody placed would otherwise render nowhere and fail nothing."""
    exported = set()
    for path in sorted((FRONTEND / "components").glob("*.jsx")):
        exported.update(re.findall(r"export function (\w+Section)\b", path.read_text(encoding="utf-8")))
    placed = set(COMPONENTS.values())
    unplaced = sorted(exported - placed)
    assert not unplaced, (
        "these section components are exported but appear in no route mapping: "
        + ", ".join(unplaced)
    )


def test_scorecard_tiles_link_through_the_route_map():
    """A bare `#key` href was correct on one page and reaches nothing across three."""
    source = (FRONTEND / "components" / "scorecard.jsx").read_text(encoding="utf-8")
    assert "SECTION_ROUTE[key]" in source, (
        "Scorecard must build tile hrefs from SECTION_ROUTE; a bare fragment only "
        "resolves when every section shares one page"
    )
```

- [ ] **Step 2: Run the tests to verify they pass against Tasks 1-3**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q`
Expected: 4 passed. These guard work already done, so they pass on first run; step 3 proves they can fail.

- [ ] **Step 3: Prove the guard catches a dropped section**

```bash
cd recsys-pipeline/frontend
cp app/offline/page.jsx /tmp/offline-page.bak
python3 - <<'PY'
from pathlib import Path
p = Path("app/offline/page.jsx")
t = p.read_text()
p.write_text(t.replace("      <OpeSection data={data.ope} />\n", ""))
PY
cd .. && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | grep -E 'ope:|passed|failed'
cp /tmp/offline-page.bak frontend/app/offline/page.jsx && rm /tmp/offline-page.bak
python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -1
```

Expected: a failure naming `ope: groups.js says /offline, but /offline does not mount <OpeSection`, then 4 passed after restoring.

- [ ] **Step 4: Update the frontend README**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("README.md")
t = p.read_text()
old = "│   ├── sections.jsx      # Scorecard + the measurement and diagnostic sections"
new = ("│   ├── groups.js         # SECTION_ROUTE: which route renders each section\n"
       "│   ├── format.js         # num / pct / share / ci / count / rankBy\n"
       "│   ├── scorecard.jsx     # the overview tiles, linked through SECTION_ROUTE\n"
       "│   ├── measurements.jsx  # the seven measurement sections\n"
       "│   ├── diagnostics.jsx   # engagement / query / recall / ranking / off-policy\n"
       "│   ├── nav.jsx           # route nav (the only client component)")
assert t.count(old) == 1, "the README structure line does not match verbatim"
t = t.replace(old, new)

anchor = "## Run\n"
assert t.count(anchor) == 1
routes = """## Routes

Three pages, split by what a number means rather than where its data came from:

| Route | Contents |
|---|---|
| `/` | The scorecard — seven tiles, each linking into the page that holds its section |
| `/online` | What the serving path did: intents, keyword gap, engagement, satisfaction, freshness, diversity, fairness, safety, latency |
| `/offline` | Models re-scored afterwards: recall@k, ranking AUC, relevance NDCG/MRR, off-policy evaluation |

`components/groups.js` maps each section to its route; moving one is a one-line change there.
Only `latency` is purely live telemetry — satisfaction, freshness and safety are offline rows with
live ones merged in when a backend was running.

## Run
"""
p.write_text(t.replace(anchor, routes))
print("README updated")
PY
```

- [ ] **Step 5: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
python3 -m pytest integration-tests/test_doc_links.py -q 2>&1 | tail -1
(cd frontend && npm run validate:data && npm run build 2>&1 | tail -12)
grep -rn 'sections\.jsx' --include='*.py' --include='*.md' --include='*.jsx' . | grep -v '.superpowers' || echo "no sections.jsx references remain"
git -C .. diff --name-only origin/master | grep -x 'recsys-pipeline/frontend/data/dashboard.json' && echo "FAIL: snapshot changed" || echo "snapshot untouched"
```

Expected: `574 passed, 2 skipped`; `5 passed`; `dashboard.json valid: 7 measurement sections, schema 2.0`; a build listing `/`, `/online`, `/offline`; no `sections.jsx` references; `snapshot untouched`.

- [ ] **Step 6: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py \
        recsys-pipeline/frontend/README.md
git commit -m "$(cat <<'MSG'
test: hold the pages to the route map, and document the three routes

Moving thirteen sections across three pages is how one gets dropped: no page
fails to build because a component went unmounted. Four assertions close that
-- every mapped section is mounted on the page its route names, none is mounted
twice, every exported section component is placed, and the scorecard builds its
hrefs from the map rather than bare fragments.

Verified by deleting <OpeSection /> from /offline and watching the first
assertion name it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

## Self-Review

**Spec coverage.** `groups.js` and the four-file split → Task 1. Routes, nav, layout, CSS → Task 2. Scorecard cross-page hrefs → Task 3. The six contract read sites → Task 4. The guard and the README → Task 5. Spec acceptance items 1-8 map to Task 2 step 5 and Task 5 step 5 (items 1, 2, 3, 5, 6, 7), Task 4 step 3 (item 4), and a final `git diff --check` in the PR step.

**Placeholders.** None. Every extraction asserts its boundary line and the file's total length before writing; every string replacement asserts its match count. The line ranges and per-file dependencies were measured before the plan was written — see Pre-validated facts.

**Type consistency.** `ROUTES` and `SECTION_ROUTE` are defined in Task 1 and consumed by name in Tasks 2, 3 and 5. `Nav` is defined in Task 2 and used only in `layout.jsx`. `_component_sources()` is defined and used within Task 4. `COMPONENTS`, `_section_route()` and `_pages()` are local to Task 5's module. The thirteen component names are listed identically in the Global Constraints, Task 1 step 3, and Task 5's `COMPONENTS` map.

**Task order exists to keep every commit building.** An earlier draft split the components first and deleted `sections.jsx` immediately, leaving `app/page.jsx` importing a file that no longer existed — that commit would not have built. Two changes fix it. The contract-test task moves first, because `_component_sources()` concatenates `components/*.jsx`, which is `sections.jsx` today and the four new files afterwards; it passes on both sides of the split and so can land before it. And the split leaves a five-line re-export barrel at `sections.jsx` so the untouched `app/page.jsx` still resolves, which Task 3 deletes when it rewires the routes. Every commit builds and every commit's tests pass.
