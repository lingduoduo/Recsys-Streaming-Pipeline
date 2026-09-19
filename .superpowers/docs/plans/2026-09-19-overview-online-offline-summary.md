# An overview covering both groups — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show all thirteen sections on the overview, grouped as Online and Offline, instead of seven measurement tiles that cover only part of one group.

**Architecture:** `scorecard.jsx` gains a `DIAGNOSTICS` spec beside `HEADLINES`, because the six diagnostics do not share the measurement envelope's shape — two expose a scalar, four need a maximum over a named row set. `Scorecard` then renders one tile block per entry in `GROUPS`, reading each section's group and label from the catalogue.

**Tech Stack:** React 19 server components, plain CSS, Python guard tests. No new dependencies, no client components.

**Spec:** `.superpowers/docs/specs/2026-09-19-overview-online-offline-summary-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- Only `frontend/components/scorecard.jsx`, `frontend/app/page.jsx`, `frontend/app/globals.css` and `integration-tests/python_modeling/test_dashboard_routes.py` change.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `HEADLINES` and `LOW_COVERAGE` keep their exact names and shapes — the contract tests parse `const HEADLINES = {` and the string `<= LOW_COVERAGE`.
- `TITLES` is removed; tiles read `SECTIONS[key].label`.
- `MetricTile` keeps its current props and markup.
- Every tile keeps its `SECTION_ROUTE` link.
- The overview stays a server component.
- Measured baseline: **581 passed, 2 skipped**. This plan adds one test, so the expected result is **582 passed, 2 skipped**.

## Pre-validated facts

Read out of the snapshot and the sources before this plan was written:

- Figures available for the six diagnostics: `engagement.ctr` = 0.2172; `query.average_query_length` = 14.98; `keyword.by_keyword[].divergence` over 18 rows; `recall.rows[].recall_at_k` over 9 rows; `ranking.rows[].auc` over 3 rows; `ope.rows[].lift_vs_logging` over 15 rows.
- Support figures: `engagement.funnel.impression` = 99415; `query.by_length` has 2 rows; `keyword.by_keyword` has 18; `recall.rows[].users_evaluated` = 200; `ranking.rows[].n` = 99415; `ope.rows[].n_events` = 25.
- `maxByField(rows, field)` already exists in `format.js` and is already imported by `scorecard.jsx`.
- `TITLES` is referenced only at `scorecard.jsx:21` (its definition) and `:66` (its use). Its seven labels are byte-identical to `SECTIONS[key].label`.
- The contract tests parse `const HEADLINES = {` at two sites and `"<= LOW_COVERAGE"` at one.
- `.sidebar-label` is the existing small-caps group-heading treatment.

## File Structure

| File | Responsibility |
|---|---|
| Modify: `frontend/components/scorecard.jsx` | `DIAGNOSTICS` spec, `diagnosticTile()`, grouped render, `TITLES` removed. |
| Modify: `frontend/app/page.jsx` | Header text: thirteen sections across two groups, not seven envelopes. |
| Modify: `frontend/app/globals.css` | The group heading above each tile block. |
| Modify: `integration-tests/python_modeling/test_dashboard_routes.py` | One assertion: every catalogue section has a tile spec. |

---

### Task 1: The diagnostic spec

**Files:**
- Modify: `recsys-pipeline/frontend/components/scorecard.jsx`

**Interfaces:**
- Consumes: `maxByField`, `num`, `share` from `./format`; `SECTIONS` from `./groups`.
- Produces: `DIAGNOSTICS` — `{[key]: {label, format, scalar?, rows?, field?, support?}}` — and `diagnosticTile(section, spec)` returning `{value, sampleSize}` or `null`. Task 2 renders from both.

- [ ] **Step 1: Add the spec and its reader**

`support` is a function in every entry so one signature covers both shapes: a scalar spec reads the
section, a row spec reads the winning row.

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/scorecard.jsx")
t = p.read_text()

anchor = "const LOW_COVERAGE = 0.5;"
addition = '''const LOW_COVERAGE = 0.5;

// The seven measurement sections share one envelope, which is why HEADLINES covers them all. The
// six diagnostics do not: two publish a scalar, four need the largest value over a named row set,
// and the row set is not always called `rows`. `support` supplies the tile's n= -- from the section
// for a scalar spec, from the winning row for a row spec -- so one signature covers both.
const DIAGNOSTICS = {
  engagement: {
    label: "CTR", format: "pct", scalar: "ctr",
    support: (section) => section.funnel?.impression,
  },
  query: {
    label: "avg query length", format: "num", scalar: "average_query_length",
    support: (section) => section.by_length?.length,
  },
  keyword: {
    label: "largest divergence", format: "num", rows: "by_keyword", field: "divergence",
    support: (section) => section.by_keyword?.length,
  },
  recall: {
    label: "best recall@k", format: "num", rows: "rows", field: "recall_at_k",
    support: (section, row) => row?.users_evaluated,
  },
  ranking: {
    label: "best AUC", format: "num", rows: "rows", field: "auc",
    support: (section, row) => row?.n,
  },
  ope: {
    label: "best lift", format: "pct", rows: "rows", field: "lift_vs_logging",
    support: (section, row) => row?.n_events,
  },
};

// null when the section is absent or the field never appears, so a missing input renders an N/A
// tile rather than a confident zero.
function diagnosticTile(section, spec) {
  if (!section) return null;
  if (spec.scalar) {
    const value = section[spec.scalar];
    if (value === null || value === undefined) return null;
    return { value, sampleSize: spec.support?.(section) };
  }
  const best = maxByField(section[spec.rows] ?? [], spec.field);
  if (!best) return null;
  return { value: best[spec.field], sampleSize: spec.support?.(section, best) };
}

function figure(value, format) {
  return format === "pct" ? share(value) : num(value, 3);
}'''
assert t.count(anchor) == 1
p.write_text(t.replace(anchor, addition))
print("DIAGNOSTICS, diagnosticTile and figure added")
PY
grep -n 'const DIAGNOSTICS\|function diagnosticTile\|function figure' components/scorecard.jsx
```

- [ ] **Step 2: Check every spec resolves against the real snapshot**

This is the step that catches a wrong field name, which would otherwise render as a silent N/A.

```bash
cd recsys-pipeline/frontend
node --input-type=module -e "
import data from './data/dashboard.json' with { type: 'json' };
const specs = {
  engagement: ['ctr', null],
  query: ['average_query_length', null],
  keyword: [null, ['by_keyword', 'divergence']],
  recall: [null, ['rows', 'recall_at_k']],
  ranking: [null, ['rows', 'auc']],
  ope: [null, ['rows', 'lift_vs_logging']],
};
for (const [key, [scalar, rowSpec]] of Object.entries(specs)) {
  const section = data[key];
  if (scalar) {
    console.log('  ' + key.padEnd(12), scalar, '=', section?.[scalar]);
  } else {
    const [path, field] = rowSpec;
    const rows = section?.[path] ?? [];
    const best = rows.reduce((b, r) => (r[field] != null && (b === null || r[field] > b[field]) ? r : b), null);
    console.log('  ' + key.padEnd(12), \`max \${field} over \${rows.length} rows =\`, best?.[field]);
  }
}
" 2>/dev/null
```

Expected: a value for all six — `ctr = 0.2172`, `average_query_length = 14.98`, and four maxima.
A `undefined` means the spec names a field the snapshot does not have.

- [ ] **Step 3: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/scorecard.jsx
git commit -m "$(cat <<'MSG'
feat(dashboard): a headline figure for each diagnostic section

HEADLINES covers the seven measurement sections because they share one
envelope. The six diagnostics do not: engagement and intents publish a scalar,
while keyword, recall, ranking and off-policy need the largest value over a row
set that is not always called `rows`.

DIAGNOSTICS names the shape per section and supplies the tile's n= through one
support function, read from the section for a scalar and from the winning row
otherwise. An absent section or a field that never appears returns null, so a
missing input becomes an N/A tile rather than a confident zero.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: The grouped overview

**Files:**
- Modify: `recsys-pipeline/frontend/components/scorecard.jsx` (`Scorecard`, remove `TITLES`)
- Modify: `recsys-pipeline/frontend/app/page.jsx`
- Modify: `recsys-pipeline/frontend/app/globals.css`

**Interfaces:**
- Consumes: `DIAGNOSTICS`, `diagnosticTile`, `figure` from Task 1; `GROUPS`, `SECTIONS`, `SECTION_ROUTE` from `./groups`.
- Produces: an overview rendering thirteen tiles under two headings.

- [ ] **Step 1: Import the catalogue and render by group**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/scorecard.jsx")
t = p.read_text()

old_import = 'import { SECTION_ROUTE } from "./groups";'
new_import = 'import { GROUPS, SECTIONS, SECTION_ROUTE } from "./groups";'
assert t.count(old_import) == 1
t = t.replace(old_import, new_import)

old_body = '''export function Scorecard({ data }) {
  return (
    <section className="scorecard">
      {Object.entries(HEADLINES).map(([key, spec]) => {
        const section = data[key];
        const available = section?.status === "available";
        const published = available && headlineFieldPublished(section, spec);
        const status = !published ? "na" : (section.coverage ?? 1) <= LOW_COVERAGE ? "low" : "ok";
        return (
          <MetricTile
            key={key}
            href={SECTION_ROUTE[key]}
            title={TITLES[key]}
            value={published ? headlineValue(section, spec) : "N/A"}
            label={spec.label}
            sampleSize={section?.sampleSize}
            status={status}
            reason={published ? null : section?.warnings?.[0] || "measurement unavailable"}
          />
        );
      })}
    </section>
  );
}'''
new_body = '''// One tile per section. A measurement section resolves through HEADLINES and carries coverage, so
// it can read "low"; a diagnostic resolves through DIAGNOSTICS and is either present or absent.
function tile(key, data) {
  const section = data[key];
  const title = SECTIONS[key].label;
  const common = { key, href: SECTION_ROUTE[key], title };

  const measurement = HEADLINES[key];
  if (measurement) {
    const available = section?.status === "available";
    const published = available && headlineFieldPublished(section, measurement);
    const status = !published ? "na" : (section.coverage ?? 1) <= LOW_COVERAGE ? "low" : "ok";
    return (
      <MetricTile
        {...common}
        value={published ? headlineValue(section, measurement) : "N/A"}
        label={measurement.label}
        sampleSize={section?.sampleSize}
        status={status}
        reason={published ? null : section?.warnings?.[0] || "measurement unavailable"}
      />
    );
  }

  const spec = DIAGNOSTICS[key];
  const resolved = diagnosticTile(section, spec);
  return (
    <MetricTile
      {...common}
      value={resolved ? figure(resolved.value, spec.format) : "N/A"}
      label={spec.label}
      sampleSize={resolved?.sampleSize}
      status={resolved ? "ok" : "na"}
      reason={resolved ? null : "no input for this section"}
    />
  );
}

export function Scorecard({ data }) {
  return (
    <div className="scorecard-groups">
      {GROUPS.map(({ key: group, label }) => (
        <section className="scorecard-group" key={group}>
          <h2 className="scorecard-label">{label}</h2>
          <div className="scorecard">
            {Object.entries(SECTIONS)
              .filter(([, catalogue]) => catalogue.group === group)
              .map(([key]) => tile(key, data))}
          </div>
        </section>
      ))}
    </div>
  );
}'''
assert t.count(old_body) == 1, "Scorecard's body does not match verbatim"
t = t.replace(old_body, new_body)

# TITLES duplicated SECTIONS labels and nothing else reads it.
import re
pattern = re.compile(r"\nconst TITLES = \{[^}]*\};\n", re.S)
t, n = pattern.subn("\n", t)
assert n == 1, f"TITLES: matched {n} times"
assert "TITLES" not in t
p.write_text(t)
print("Scorecard renders by group; TITLES removed")
PY
```

- [ ] **Step 2: Update the page header and add the group heading style**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("app/page.jsx")
t = p.read_text()
old = """        <h1>Measurement scorecard</h1>
        <p className="page-description">
          Seven measurement envelopes at a glance. Each tile opens that section&apos;s page.
        </p>"""
new = """        <h1>Scorecard</h1>
        <p className="page-description">
          All thirteen sections at a glance, grouped as the sidebar groups them. Each tile opens
          that section&apos;s page.
        </p>"""
assert t.count(old) == 1
p.write_text(t.replace(old, new))
print("page header updated")
PY
cat >> app/globals.css <<'CSS'

/* The overview's two tile blocks. The heading reuses the sidebar's small-caps treatment rather
   than introducing a second one. */
.scorecard-groups {
  display: grid;
  gap: 26px;
}

.scorecard-label {
  margin: 0 0 10px;
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.09em;
  text-transform: uppercase;
  color: var(--muted);
}
CSS
```

- [ ] **Step 3: Build and check the rendered overview**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
rm -rf .next
(npm run dev >/tmp/dev.log 2>&1 &)
until curl -sf -o /dev/null http://localhost:3000/ 2>/dev/null; do sleep 2; done
page=$(curl -s http://localhost:3000/)
echo -n "tiles: ";           grep -o 'class="metric-tile' <<<"$page" | wc -l | tr -d ' '
echo -n "group headings: ";  grep -o 'scorecard-label' <<<"$page" | wc -l | tr -d ' '
echo "tile hrefs:"
grep -oE 'href="/(online|offline)/[a-z]+"' <<<"$page" | sort | sed 's/^/  /'
pkill -f 'next dev'; pkill -f 'next-server'
```

Expected: 13 tiles, 2 headings, and thirteen distinct hrefs — nine `/online/...` and four
`/offline/...`.

- [ ] **Step 4: Run the suite**

Run: `cd recsys-pipeline && python3 -m pytest -q 2>&1 | tail -2`
Expected: `581 passed, 2 skipped` — unchanged; Task 3 adds the new assertion.

The contract tests must be among the passes: they parse `const HEADLINES = {` and `<= LOW_COVERAGE`,
both still present.

- [ ] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/scorecard.jsx recsys-pipeline/frontend/app/page.jsx \
        recsys-pipeline/frontend/app/globals.css
git commit -m "$(cat <<'MSG'
feat(dashboard): the overview covers both groups, not seven of thirteen

It showed seven measurement tiles -- six online sections and one offline, with
the six diagnostics absent -- so a reader wanting to know how retrieval,
ranking or off-policy evaluation was doing had to open three pages to find out.

Thirteen tiles now, under the two group headings the sidebar already uses, in
the catalogue's order. Each still links to its section.

TITLES goes with it: nothing but Scorecard read it, and its seven labels were
byte-identical to the catalogue's, so it was a second source of truth for the
same names.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: The guard

**Files:**
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`

**Interfaces:**
- Consumes: `FRONTEND` and `_catalogue()` from the existing module.

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`:

```python
def test_every_section_has_a_tile_on_the_overview():
    """A section added to the catalogue must not skip the summary.

    The overview is the only page showing every section at once, so a section with no
    tile is invisible there while looking complete -- nothing else would fail.
    """
    scorecard = (FRONTEND / "components" / "scorecard.jsx").read_text(encoding="utf-8")
    specced = set()
    for name in ("HEADLINES", "DIAGNOSTICS"):
        body = re.search(rf"const {name} = \{{(.*?)\n\}};", scorecard, re.S)
        assert body, f"scorecard.jsx must declare a {name} map"
        specced.update(re.findall(r"^  (\w+):", body.group(1), re.M))
    catalogue = set(_catalogue())
    missing = sorted(catalogue - specced)
    assert not missing, (
        "these sections are in the catalogue but have no overview tile: " + ", ".join(missing)
    )
```

- [ ] **Step 2: Run it**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -2`
Expected: 12 passed. The assertion holds once Tasks 1 and 2 have landed, so step 3 is what proves it
can fail.

- [ ] **Step 3: Prove the guard catches a missing tile**

```bash
cd recsys-pipeline/frontend
cp components/scorecard.jsx /tmp/scorecard.bak
python3 - <<'PY'
import re
from pathlib import Path
p = Path("components/scorecard.jsx")
t = p.read_text()
# Drop the ope entry from DIAGNOSTICS, as if a section were added without a tile.
t = re.sub(r"\n  ope: \{.*?\n  \},", "", t, count=1, flags=re.S)
p.write_text(t)
PY
cd .. && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py::test_every_section_has_a_tile_on_the_overview -q 2>&1 | grep -E 'no overview tile|passed|failed'
cp /tmp/scorecard.bak frontend/components/scorecard.jsx && rm /tmp/scorecard.bak
python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -1
```

Expected: a failure naming `ope`, then 12 passed after restoring.

- [ ] **Step 4: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
python3 -m pytest integration-tests/python_modeling/test_dashboard_measurement_contract.py -q 2>&1 | tail -1
(cd frontend && npm run validate:data)
git -C .. diff --name-only origin/master | grep -v '^\.superpowers/'
git -C .. diff --check && echo "diff --check clean"
```

Expected: `582 passed, 2 skipped`; `15 passed`; the data contract valid; four files listed; clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py
git commit -m "$(cat <<'MSG'
test: every catalogue section has an overview tile

The overview is the only page showing every section at once, so a section with
no tile is invisible there while the page still looks complete. Nothing else
would fail.

Verified by removing the ope entry from DIAGNOSTICS and watching the assertion
name it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

## Self-Review

**Spec coverage.** `DIAGNOSTICS` and its reader → Task 1. The grouped render, the header and the heading style → Task 2. `TITLES` removal → Task 2 step 1. The guard → Task 3. Spec acceptance items 1-8 map to Task 2 step 4 and Task 3 step 4 (items 1, 2, 6, 7, 8), Task 2 step 3 (items 3, 4, 5).

**Placeholders.** None. Every replacement asserts its match count, and Task 1 step 2 resolves each spec against the real snapshot rather than trusting the field names — which is where a typo would otherwise hide as a silent N/A tile.

**Type consistency.** `DIAGNOSTICS`, `diagnosticTile()` and `figure()` are defined in Task 1 and consumed by name in Task 2's `tile()`. `GROUPS`, `SECTIONS`, `SECTION_ROUTE` come from `./groups` and are imported once. The guard in Task 3 parses `HEADLINES` and `DIAGNOSTICS` by the same names Task 1 declares.

**One risk worth stating.** `tile()` assumes every key is in exactly one of `HEADLINES` or `DIAGNOSTICS`, and reads `spec.format` and `spec.label` off whichever matched. A key in neither would throw on `spec.label` at build time rather than rendering an N/A — which is loud, and Task 3's guard is what stops it reaching that point.
