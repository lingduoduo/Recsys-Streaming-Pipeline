# Teal accent and denser tables — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adopt the reference design's visual treatment — teal accent, right-aligned numerics, em-dash for a missing cell, tighter rows — without changing structure, content or interactivity.

**Architecture:** Two files. `globals.css` carries the accent rename and the table rules; `ui.jsx`'s `DataTable` marks numeric cells and renders the em-dash. The CSS custom-property guard added in #256 is what proves the rename is complete, so the rename is safe to do mechanically.

**Tech Stack:** Plain CSS, one React server component. No new dependencies, no client components, no Python changes.

**Spec:** `.superpowers/docs/specs/2026-09-19-dashboard-teal-table-polish-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- Only `frontend/app/globals.css` and `frontend/components/ui.jsx` change.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- Every section stays a server component. `DataTable` gains no interactivity and no `"use client"`.
- `num()`, `pct()`, `share()`, `ci()` and `count()` in `format.js` are unchanged — only table cells become em-dashes.
- The `--series-*` chart palette is unchanged.
- Measured baseline: **581 passed, 2 skipped**. This change adds no tests and removes none, so the expected result is **581 passed, 2 skipped** — it is CSS and one display string.

## Pre-validated facts

Read out of the files before this plan was written:

- `:root` declares `--canvas`, `--surface`, `--ink`, `--muted`, `--line`, `--indigo: #4f46e5`, `--indigo-soft: #eef2ff`, `--amber`, `--amber-soft`, `--shadow`, `--series-0: #4f46e5`, `--series-1`, `--series-2`.
- `var(--indigo` appears **7 times**: lines 105, 167, 534, 569, 570, 688, 737.
- `.insight` hardcodes `color: #3730a3` at line 106 — indigo ink for the indigo-soft background.
- `table.rpt th` already sets `text-transform: uppercase; letter-spacing: 0.045em; font-size: 0.68rem;` with `color: #475569`, and `table.rpt` already sets `font-variant-numeric: tabular-nums`. Neither needs changing.
- `table.rpt th` padding is `11px 13px`; `table.rpt td` padding is `10px 13px`; `td` has no `text-align`.
- `.hero`, `.hero h1`, `.hero p` and `.report-badge` have zero JSX references. `.eyebrow` has one and stays.
- No test asserts the literal `"N/A"` for a table cell.

## File Structure

| File | Responsibility |
|---|---|
| Modify: `frontend/app/globals.css` | Accent rename, table alignment and density, dead-rule removal. |
| Modify: `frontend/components/ui.jsx` | `DataTable` marks numeric cells and renders `—`. |

---

### Task 1: The accent

**Files:**
- Modify: `recsys-pipeline/frontend/app/globals.css`

**Interfaces:**
- Produces: `--accent` (`#0d9488`) and `--accent-soft` (`#f0fdfa`). Task 2 relies on the hover rule already pointing at `--accent-soft` through this rename.

- [ ] **Step 1: Confirm the guard currently passes**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py::test_every_css_variable_used_is_defined -q 2>&1 | tail -1`
Expected: 1 passed. This is the assertion that will catch an incomplete rename, so it has to be green before it can mean anything.

- [ ] **Step 2: Rename and recolour**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("app/globals.css")
t = p.read_text()

before = t.count("var(--indigo")
assert before == 7, f"expected 7 var(--indigo…) uses, found {before}"

assert t.count("  --indigo: #4f46e5;") == 1
assert t.count("  --indigo-soft: #eef2ff;") == 1
t = t.replace("  --indigo: #4f46e5;", "  --accent: #0d9488;")
t = t.replace("  --indigo-soft: #eef2ff;", "  --accent-soft: #f0fdfa;")

# Every remaining var(--indigo…) is a use site; --series-0 keeps its own #4f46e5.
t = t.replace("var(--indigo-soft)", "var(--accent-soft)")
t = t.replace("var(--indigo)", "var(--accent)")

# .insight's ink was chosen to sit on the soft background; teal needs the teal equivalent.
assert t.count("  color: #3730a3;") == 1
t = t.replace("  color: #3730a3;", "  color: #115e59;")

assert "var(--indigo" not in t
p.write_text(t)
print("accent renamed to teal across 7 use sites")
PY
grep -n '\-\-accent\|#115e59\|series-0' app/globals.css | head -6
```

Expected: the two declarations, the insight ink, and `--series-0: #4f46e5` still present.

- [ ] **Step 3: Prove the rename is complete**

```bash
cd recsys-pipeline
echo -n "surviving var(--indigo…): "; grep -c 'var(--indigo' frontend/app/globals.css
python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -1
```

Expected: `0`, then 11 passed. A missed use site would leave `var(--indigo)` undeclared and the
guard would name it.

- [ ] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/app/globals.css
git commit -m "$(cat <<'MSG'
style(dashboard): teal accent, matching the reference design

--indigo and --indigo-soft become --accent and --accent-soft at #0d9488 and
#f0fdfa, across all seven use sites -- links, the active sidebar item, the
brand mark, the insight pill, the keyword heatmap and the table hover.
.insight's ink moves from #3730a3 to #115e59, the teal counterpart of the
indigo chosen to sit on the soft background.

The --series-* chart palette keeps its own colours: it distinguishes series
from each other, not from the accent, and the reference design has no charts
to argue otherwise.

The CSS custom-property guard is what proves the rename is complete -- a missed
use site leaves var(--indigo) undeclared and it fails by name.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: Alignment, density and the em-dash

**Files:**
- Modify: `recsys-pipeline/frontend/app/globals.css`
- Modify: `recsys-pipeline/frontend/components/ui.jsx` (`DataTable`)

**Interfaces:**
- Consumes: `--accent-soft` from Task 1, already in the hover rule.
- Produces: a `.num` class on numeric cells.

- [ ] **Step 1: Tighten the rows and add the alignment class**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("app/globals.css")
t = p.read_text()

old_th = """table.rpt th {
  position: sticky;
  top: 0;
  padding: 11px 13px;"""
new_th = """table.rpt th {
  position: sticky;
  top: 0;
  padding: 8px 12px;"""
assert t.count(old_th) == 1
t = t.replace(old_th, new_th)

old_td = """table.rpt td {
  padding: 10px 13px;"""
new_td = """table.rpt td {
  padding: 7px 12px;"""
assert t.count(old_td) == 1
t = t.replace(old_td, new_td)

anchor = """table.rpt tbody tr:nth-child(even) {"""
addition = """/* Numbers read down a column, so they align right. The table already sets
   font-variant-numeric: tabular-nums, which keeps the digits themselves in step. */
table.rpt td.num,
table.rpt th.num {
  text-align: right;
}

table.rpt tbody tr:nth-child(even) {"""
assert t.count(anchor) == 1
p.write_text(t.replace(anchor, addition))
print("rows tightened, .num added")
PY
```

- [ ] **Step 2: Mark numeric cells and render the em-dash**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/ui.jsx")
t = p.read_text()
old = """              {cols.map((c) => {
                const format = formatters[c];
                const value = format ? format(r[c], r) : formatCell(r[c]);
                return <td key={c}>{value === null || value === undefined || value === "" ? "N/A" : value}</td>;
              })}"""
new = """              {cols.map((c) => {
                const format = formatters[c];
                const value = format ? format(r[c], r) : formatCell(r[c]);
                const missing = value === null || value === undefined || value === "";
                // Right-align on the raw type, not the formatted string: a formatter may return
                // "21.7%" or "103.8 ms", which are still numbers to a reader scanning a column.
                const numeric = typeof r[c] === "number";
                return (
                  <td key={c} className={numeric ? "num" : undefined}>
                    {/* An em-dash reads better than N/A inside a dense table and means the same.
                        Headline cards keep the explicit N/A -- see num() in format.js. */}
                    {missing ? "—" : value}
                  </td>
                );
              })}"""
assert t.count(old) == 1, "DataTable's cell loop does not match verbatim"
p.write_text(t.replace(old, new))
print("DataTable marks numeric cells and renders the em-dash")
PY
```

- [ ] **Step 3: Align the headers of numeric columns too**

A right-aligned column with a left-aligned header reads as a mistake.

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/ui.jsx")
t = p.read_text()
old = """            {cols.map((c) => (
              <th key={c}>{c.replaceAll("_", " ")}</th>
            ))}"""
new = """            {cols.map((c) => (
              // A header sits over its column, so it takes the column's alignment. The first row
              // decides: every row of a table column holds the same kind of value.
              <th key={c} className={typeof rows[0]?.[c] === "number" ? "num" : undefined}>
                {c.replaceAll("_", " ")}
              </th>
            ))}"""
assert t.count(old) == 1
p.write_text(t.replace(old, new))
print("numeric headers align with their columns")
PY
```

- [ ] **Step 4: Run the suite and build**

```bash
cd recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
cd frontend && npm run build 2>&1 | tail -4
```

Expected: `581 passed, 2 skipped` — unchanged — and a successful build. The six contract tests parse
`columns={[...]}` at the call sites, which this does not touch.

- [ ] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/app/globals.css recsys-pipeline/frontend/components/ui.jsx
git commit -m "$(cat <<'MSG'
style(dashboard): right-align numbers, tighten rows, dash empty cells

Numbers read down a column, so they align right -- headers included, since a
right-aligned column under a left-aligned header reads as a mistake. The
alignment keys off the raw value's type rather than the formatted string,
because a formatter may return "21.7%" or "103.8 ms" and those are still
numbers to someone scanning.

Row padding drops from 10px to 7px, which fits roughly half again as many rows
on a screen. The header casing and tabular figures were already right and are
untouched.

An empty table cell renders an em-dash instead of "N/A". Headline cards and
NaCard keep the explicit string: stating that a measurement is absent is the
point there, where in a dense table a dash says the same thing more quietly.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: The dead CSS

**Files:**
- Modify: `recsys-pipeline/frontend/app/globals.css`

**Interfaces:**
- Consumes: nothing. Produces: nothing.

- [ ] **Step 1: Confirm the rules are unreferenced**

```bash
cd recsys-pipeline/frontend
for name in hero report-badge eyebrow; do
  printf '%-14s JSX references: %s\n' "$name" \
    "$(grep -rho "\"$name\"\|className=\"$name" components/ app/ --include='*.jsx' | wc -l | tr -d ' ')"
done
```

Expected: `hero 0`, `report-badge 0`, `eyebrow 1` or more — `.eyebrow` stays.

- [ ] **Step 2: Remove them**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
import re
from pathlib import Path
p = Path("app/globals.css")
t = p.read_text()
for selector in (".hero", ".hero h1", ".hero p", ".report-badge"):
    pattern = re.compile(r"\n" + re.escape(selector) + r" \{[^}]*\}\n", re.S)
    t, n = pattern.subn("\n", t)
    assert n == 1, f"{selector}: matched {n} times"
p.write_text(t)
print("four dead rule blocks removed")
PY
grep -c 'hero\|report-badge' app/globals.css
```

Expected: `0`.

- [ ] **Step 3: Rebuild and verify the rendered result**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
rm -rf .next
(npm run dev >/tmp/dev.log 2>&1 &)
until curl -sf -o /dev/null http://localhost:3000/offline/ranking 2>/dev/null; do sleep 2; done
page=$(curl -s http://localhost:3000/offline/ranking)
echo -n "teal accent present: ";   grep -c '0d9488\|--accent' <<<"$page"
echo -n "right-aligned cells: ";   grep -o 'class="num"' <<<"$page" | wc -l | tr -d ' '
echo -n "em-dashes: ";             grep -o '—' <<<"$page" | wc -l | tr -d ' '
pkill -f 'next dev'; pkill -f 'next-server'
```

Expected: a non-zero count for each. The ranking table has `auc` and `logloss` columns that are
`null` for unscored signals, so both the alignment class and the dash appear on that page.

- [ ] **Step 4: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
(cd frontend && npm run validate:data)
git -C .. diff --name-only origin/master | grep -vE '\.superpowers/' 
git -C .. diff --check && echo "diff --check clean"
```

Expected: `581 passed, 2 skipped`; the data contract valid; exactly two frontend files listed; clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/app/globals.css
git commit -m "$(cat <<'MSG'
style(dashboard): drop the CSS for markup that no longer exists

.hero, .hero h1, .hero p and .report-badge lost their markup when #255 moved
the hero into the shared layout and #257 replaced that layout with the
sidebar. Both were my changes, so this is cleaning up after them rather than
tidying someone else's code. .eyebrow stays: the page header uses it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

## Self-Review

**Spec coverage.** The accent rename and `.insight` ink → Task 1. Alignment, density and the em-dash → Task 2. Dead CSS → Task 3. Spec acceptance items 1-8 map to Task 2 step 4 and Task 3 step 4 (items 1, 5, 7, 8), Task 1 step 3 (items 2, 3), Task 3 steps 1-2 (item 4), and Task 3 step 3 (item 6).

**Placeholders.** None. Every replacement asserts its match count, and the rename asserts the use-site count before touching anything. The pre-validated facts were read out of the files rather than assumed — two of them corrected the spec.

**Type consistency.** `--accent` and `--accent-soft` are introduced in Task 1 and used by name nowhere else in the plan, because the rename rewrites every existing use site in place. `.num` is defined in Task 2 step 1 and applied in steps 2 and 3, on `td` and `th` respectively.

**One risk worth stating.** Task 2 step 3 decides a column's alignment from `rows[0]`. If the first row of a table holds `null` where later rows hold numbers, that column's header will be left-aligned over right-aligned cells. The alternative — scanning every row for the first non-null — is more code for a case that does not arise in this snapshot, where a column is numeric or it is not. If it ever does, the fix is to scan rather than index.
