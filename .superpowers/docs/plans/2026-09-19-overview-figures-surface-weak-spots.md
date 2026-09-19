# Overview figures that surface weak spots — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Change three of the four row-based overview figures so they point at weaknesses rather than flattering, and fix one that was selecting on the wrong side of a signed field.

**Architecture:** One generalised selector in `format.js` replaces the ad-hoc maximum, and `DIAGNOSTICS` entries declare which extremum they take and which rows they consider. `maxByField` keeps its name and behaviour because three other callers depend on it.

**Tech Stack:** React 19 server components, plain JavaScript, Python guard tests. No new dependencies.

**Spec:** `.superpowers/docs/specs/2026-09-19-overview-figures-surface-weak-spots-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- Only `frontend/components/format.js`, `frontend/components/scorecard.jsx` and `integration-tests/python_modeling/test_dashboard_routes.py` change.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- `HEADLINES` and `LOW_COVERAGE` keep their exact names and shapes.
- `maxByField` keeps its exported name and behaviour — it has three other callers.
- `DIAGNOSTICS` keeps its name; the #260 guard parses it.
- Every tile keeps its `SECTION_ROUTE` link and its `support` count.
- Measured baseline: **582 passed, 2 skipped**. This plan adds one test, so the expected result is **583 passed, 2 skipped**.

## Execution record

Executed 2026-09-19 on `fix/overview-figures-surface-weak-spots`, three commits, all steps checked.
Final suite: **583 passed, 2 skipped**, from 582 — exactly the one test predicted. No deviations.

Both predictions the plan made about its own checks held. The `maxByField` parity check returned
true, so the three callers that depend on it — the fairness tile and two fairness KPIs — were
unaffected, confirmed again by the rendered Fairness tile still reading `0.049 largest CTR gap`.
And Task 3's guard fired on `ope` as written, whose maximum is correct; stating it turned an
inherited default into a recorded judgement.

Worth noting what the original request would have produced. "Use the worst instead of the best"
applied to all four would have made two figures *more* misleading: recall's minimum would have
reported k=5 rather than any quality signal, and off-policy's worst lift answers a question nobody
asks. Checking each against the data before implementing is what caught that.

## Pre-validated facts

Measured against the committed snapshot and the sources before writing:

- `ranking.rows[].auc` = 0.5595 (popularity), 0.5058 (position), 0.5319 (embedding). Worst is **0.5058, position**.
- `keyword.by_keyword[].divergence` spans **−0.0182 to +0.0137**; the largest magnitude is **−0.0182 (Documentary)**.
- `recall.rows` is method × k for k ∈ {5, 10, 20}. At k=10 the three methods are bm25 0.0351, embedding 0.0351, hybrid 0.0360 — best is **0.0360, hybrid**. Unfiltered, the maximum is 0.0696 at k=20.
- `ope.rows[].lift_vs_logging` spans −0.004 to 0.0; the maximum is **0.0**.
- `maxByField` has four call sites: `format.js:34` (definition), `scorecard.jsx:67` (diagnostic tile), `scorecard.jsx:78` (`headlineRow`, used by `HEADLINES.fairness`), `measurements.jsx:220` and `:230` (fairness KPIs).

## File Structure

| File | Responsibility |
|---|---|
| Modify: `frontend/components/format.js` | `pickByField`; `maxByField` delegates to it. |
| Modify: `frontend/components/scorecard.jsx` | `select` and `where` in `DIAGNOSTICS`; `diagnosticTile` honours both. |
| Modify: `integration-tests/python_modeling/test_dashboard_routes.py` | One assertion: a row-based tile must declare its extremum. |

---

### Task 1: One selection helper

**Files:**
- Modify: `recsys-pipeline/frontend/components/format.js`

**Interfaces:**
- Produces: `pickByField(rows, field, select = "max")` returning the extremal row or `null`. `select` is `"max"`, `"min"` or `"abs"`. Task 2 calls it.

- [x] **Step 1: Generalise the selector**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/format.js")
t = p.read_text()
old = '''// The row a section's headline is read from, when it is not a fixed index. Fairness emits one
// row per demographic dimension in DEFAULT_DIMENSIONS order, not in gap order, so rows[0] is
// whichever dimension sorts first — never "the largest gap" the tile claims to show.
export function maxByField(rows, field) {
  return rows.reduce(
    (best, row) =>
      row?.[field] !== null && row?.[field] !== undefined && (best === null || row[field] > best[field])
        ? row
        : best,
    null,
  );
}'''
new = '''// The extremal row by one field. `select` is "max", "min", or "abs" for the largest magnitude,
// which is what a signed field like keyword divergence needs: −0.0182 is a bigger gap than
// +0.0137, and taking the maximum would report the smaller one.
//
// A row whose field is null or undefined is skipped rather than treated as zero. Treating it as
// zero would let "worst AUC" name a signal that was never scored.
export function pickByField(rows, field, select = "max") {
  const rank = select === "abs" ? (value) => Math.abs(value) : (value) => value;
  const better = select === "min" ? (a, b) => a < b : (a, b) => a > b;
  return (rows ?? []).reduce((best, row) => {
    const value = row?.[field];
    if (value === null || value === undefined) return best;
    return best === null || better(rank(value), rank(best[field])) ? row : best;
  }, null);
}

// The row a section's headline is read from, when it is not a fixed index. Fairness emits one
// row per demographic dimension in DEFAULT_DIMENSIONS order, not in gap order, so rows[0] is
// whichever dimension sorts first — never "the largest gap" the tile claims to show.
export function maxByField(rows, field) {
  return pickByField(rows, field, "max");
}'''
assert t.count(old) == 1, "maxByField does not match verbatim"
p.write_text(t.replace(old, new))
print("pickByField added; maxByField delegates")
PY
```

- [x] **Step 2: Prove all three modes against the snapshot, and that maxByField is unchanged**

```bash
cd recsys-pipeline/frontend
node --input-type=module -e "
import data from './data/dashboard.json' with { type: 'json' };
import { pickByField, maxByField } from './components/format.js';
const ranking = data.ranking.rows, keyword = data.keyword.by_keyword;
console.log('  max auc      ', pickByField(ranking, 'auc', 'max').signal, pickByField(ranking, 'auc', 'max').auc.toFixed(4));
console.log('  min auc      ', pickByField(ranking, 'auc', 'min').signal, pickByField(ranking, 'auc', 'min').auc.toFixed(4));
console.log('  abs divergence', pickByField(keyword, 'divergence', 'abs').keyword, pickByField(keyword, 'divergence', 'abs').divergence);
const fair = data.fairness.rows;
console.log('  maxByField parity', maxByField(fair, 'ctr_max_min_gap') === pickByField(fair, 'ctr_max_min_gap', 'max'));
" 2>/dev/null
```

Expected: `max auc popularity 0.5595`, `min auc position 0.5058`, `abs divergence Documentary -0.0182`,
`maxByField parity true`.

- [x] **Step 3: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/format.js
git commit -m "$(cat <<'MSG'
refactor(dashboard): one selector for max, min and largest magnitude

pickByField takes the extremal row under "max", "min" or "abs". The last is
what a signed field needs: keyword divergence spans −0.0182 to +0.0137, so the
largest gap is the negative one and a maximum reports the smaller mismatch.

maxByField keeps its name and now delegates. It has three other callers --
headlineRow for the fairness tile and two fairness KPIs in measurements.jsx --
so its behaviour had to stay identical, which step 2 checked by comparing the
two on the fairness rows.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: The four rules

**Files:**
- Modify: `recsys-pipeline/frontend/components/scorecard.jsx`

**Interfaces:**
- Consumes: `pickByField` from `./format`.
- Produces: `DIAGNOSTICS` entries carrying optional `select` and `where`.

- [x] **Step 1: Teach diagnosticTile the two new keys**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/scorecard.jsx")
t = p.read_text()

old_import = 'import { num, share, maxByField } from "./format";'
new_import = 'import { num, share, maxByField, pickByField } from "./format";'
assert t.count(old_import) == 1
t = t.replace(old_import, new_import)

old = """  const best = maxByField(section[spec.rows] ?? [], spec.field);
  if (!best) return null;"""
new = """  // `where` narrows before the extremum is taken. Recall publishes one row per method per k, and
  // recall@k rises with k by construction, so an extremum over the unfiltered set reports which k
  // it picked rather than how retrieval performed.
  const rows = (section[spec.rows] ?? []).filter(spec.where ?? (() => true));
  const best = pickByField(rows, spec.field, spec.select ?? "max");
  if (!best) return null;"""
assert t.count(old) == 1
p.write_text(t.replace(old, new))
print("diagnosticTile honours where and select")
PY
```

- [x] **Step 2: Change the three rules**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/scorecard.jsx")
t = p.read_text()

edits = [
  # Signed field: the largest gap in the snapshot is −0.0182, not +0.0137.
  ('''  keyword: {
    label: "largest divergence", format: "num", rows: "by_keyword", field: "divergence",
    support: (section) => section.by_keyword?.length,
  },''',
   '''  keyword: {
    label: "largest gap", format: "num", rows: "by_keyword", field: "divergence", select: "abs",
    support: (section) => section.by_keyword?.length,
  },'''),
  # One row per method per k; pin k so the tile compares methods, not values of k.
  ('''  recall: {
    label: "best recall@k", format: "num", rows: "rows", field: "recall_at_k",
    support: (section, row) => row?.users_evaluated,
  },''',
   '''  recall: {
    label: "best recall@10", format: "num", rows: "rows", field: "recall_at_k",
    where: (row) => row.k === 10,
    support: (section, row) => row?.users_evaluated,
  },'''),
  # A near-random signal is the finding; the best signal hides it.
  ('''  ranking: {
    label: "best AUC", format: "num", rows: "rows", field: "auc",
    support: (section, row) => row?.n,
  },''',
   '''  ranking: {
    label: "worst AUC", format: "num", rows: "rows", field: "auc", select: "min",
    support: (section, row) => row?.n,
  },'''),
]
for old, new in edits:
    assert t.count(old) == 1, f"no unique match for {old.splitlines()[0]!r}"
    t = t.replace(old, new)
p.write_text(t)
print("keyword, recall and ranking rules changed; ope unchanged")
PY
grep -n 'select:\|where:' components/scorecard.jsx
```

Expected: `select: "abs"` on keyword, `where` on recall, `select: "min"` on ranking. `ope` shows
neither and defaults to `"max"`.

- [x] **Step 3: Verify the rendered tiles**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
rm -rf .next
(npm run dev >/tmp/dev.log 2>&1 &)
until curl -sf -o /dev/null http://localhost:3000/ 2>/dev/null; do sleep 2; done
curl -s http://localhost:3000/ | python3 -c "
import re, sys
h = sys.stdin.read()
for m in re.finditer(r'metric-title\">([^<]*).*?metric-value\">([^<]*).*?metric-label\">([^<]*)', h, re.S):
    title, value, label = m.groups()
    if title in ('Ranking quality', 'Keyword gap', 'Candidate recall', 'Off-policy evaluation', 'Fairness'):
        print(f'  {title:22} {value:>10}  {label}')
"
pkill -f 'next dev'; pkill -f 'next-server'
```

Expected: `Ranking quality 0.506 worst AUC`, `Keyword gap -0.018 largest gap`,
`Candidate recall 0.036 best recall@10`, `Off-policy evaluation 0.0% best lift`, and
`Fairness 0.049 largest CTR gap` — the last proving `maxByField` still behaves as before.

- [x] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/scorecard.jsx
git commit -m "$(cat <<'MSG'
fix(dashboard): overview figures that point at weaknesses

Ranking reported its best AUC, 0.560, while a signal sat at 0.506 -- barely
better than random. It now reports the worst, which is the thing worth seeing.

Keyword reported the largest divergence by value. The field is signed, spanning
−0.0182 to +0.0137, so the maximum was the *smaller* mismatch. It now selects
by magnitude and still displays the sign, which says which way the gap runs.

Recall reported the best recall@k over rows that are method x k. recall@k rises
with k by construction, so that figure reported k=20 rather than retrieval
quality; it now pins k=10 and compares the three methods.

Off-policy keeps its maximum: "is any policy better than logging" is the
question the section exists to answer, and the worst candidate answers nothing.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: The guard

**Files:**
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`

- [x] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`:

```python
def test_row_based_tiles_declare_which_extremum_they_take():
    """A default of "max" is the flattering choice, and it should be deliberate.

    Three of the four row-based tiles were wrong in #260: ranking reported its best
    AUC while a signal sat near random, keyword took the maximum of a signed field and
    so reported the smaller mismatch, and recall took an extremum over rows that vary
    by k, which reported k rather than quality. Each now says which extremum it means.
    """
    scorecard = (FRONTEND / "components" / "scorecard.jsx").read_text(encoding="utf-8")
    body = re.search(r"const DIAGNOSTICS = \{(.*?)\n\};", scorecard, re.S)
    assert body, "scorecard.jsx must declare a DIAGNOSTICS map"
    # Each entry runs from its key to the next key or the end of the map.
    entries = re.split(r"\n  (?=\w+: \{)", body.group(1))
    undeclared = []
    for entry in entries:
        name = re.match(r"\s*(\w+):", entry)
        if not name or "rows:" not in entry:
            continue
        if "select:" not in entry and "where:" not in entry:
            undeclared.append(name.group(1))
    assert not undeclared, (
        "these row-based tiles take an extremum without saying which, so they default to "
        f"the flattering maximum: {', '.join(undeclared)}"
    )
```

- [x] **Step 2: Run it**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -2`
Expected: FAIL naming `ope` — it is row-based and declares neither key.

This is the assertion doing its job on a real case: `ope`'s maximum is correct, but that is a
judgement the entry should state rather than inherit from a default.

- [x] **Step 3: Make ope's maximum explicit**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/scorecard.jsx")
t = p.read_text()
old = '''  ope: {
    label: "best lift", format: "pct", rows: "rows", field: "lift_vs_logging",
    support: (section, row) => row?.n_events,
  },'''
new = '''  ope: {
    // "max" on purpose: the section asks whether any policy beats logging, so the best
    // candidate is the answer and the worst one is noise.
    label: "best lift", format: "pct", rows: "rows", field: "lift_vs_logging", select: "max",
    support: (section, row) => row?.n_events,
  },'''
assert t.count(old) == 1
p.write_text(t.replace(old, new))
print("ope states its maximum")
PY
cd .. && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -1
```

Expected: 13 passed.

- [x] **Step 4: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
python3 -m pytest integration-tests/python_modeling/test_dashboard_measurement_contract.py -q 2>&1 | tail -1
(cd frontend && npm run validate:data && npm run build 2>&1 | tail -3)
git -C .. diff --name-only origin/master | grep -v '^\.superpowers/'
git -C .. diff --check && echo "diff --check clean"
```

Expected: `583 passed, 2 skipped`; `15 passed`; the data contract valid and a successful build; three
files listed; clean.

- [x] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py \
        recsys-pipeline/frontend/components/scorecard.jsx
git commit -m "$(cat <<'MSG'
test: a row-based tile must say which extremum it takes

Defaulting to the maximum is the flattering choice, and three of the four
row-based tiles were wrong because of it. The assertion makes the choice
deliberate rather than inherited.

It fired on ope, whose maximum is correct -- so ope now states it, with the
reason: the section asks whether any policy beats logging, so the best
candidate is the answer.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

## Self-Review

**Spec coverage.** `pickByField` and `maxByField`'s delegation → Task 1. The three changed rules and `ope` unchanged → Task 2. The guard, and `ope` stating its choice → Task 3. Spec acceptance items 1-7 map to Task 3 step 4 (items 1, 2, 5, 6, 7) and Task 2 step 3 (items 3, 4).

**Placeholders.** None. Every replacement asserts its match count; Task 1 step 2 exercises all three selector modes against the real snapshot and checks `maxByField` parity on the fairness rows before anything depends on it.

**Type consistency.** `pickByField(rows, field, select)` is defined in Task 1 and called in Task 2's `diagnosticTile`. `select` and `where` are the two keys added to `DIAGNOSTICS` in Task 2 and parsed by name in Task 3's assertion.

**One risk worth stating.** Task 3's assertion splits the `DIAGNOSTICS` body on `\n  key: {`, which assumes two-space indentation and one entry per key. That holds for the file as written and would break loudly — a split that fails produces entries with no `rows:` and the test passes vacuously. A vacuous pass is the failure mode worth knowing about: if the map is ever reformatted, confirm the test still fails when a `select` is removed.
