# Group the sections by the question they answer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Replace "Online prediction" and "Offline prediction" with three groups that describe what a reader is asking, so no group claims that descriptive analytics are model predictions.

**Architecture:** `components/groups.js` is the single source of truth — the sidebar, the dynamic route, `SECTION_ROUTE`, the overview and each page header all derive from it. So this is one data edit plus the documentation that restates it.

**Tech Stack:** Plain JavaScript data module, Markdown. No component changes, no test changes.

**Spec:** `.superpowers/docs/specs/2026-09-19-group-sections-by-question-design.md`

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- Only `frontend/components/groups.js` and `frontend/README.md` change.
- `frontend/data/dashboard.json` is **not** regenerated and not edited.
- Every section keeps its key, its label, its description and its component.
- `GROUPS`, `SECTIONS` and `SECTION_ROUTE` keep their exported names and shapes; `SECTION_ROUTE` stays derived.
- `groups.js` keeps importing nothing.
- **No test file is edited.** If a guard fails, that is a finding about the guard — stop and report it rather than editing the test to match.
- Measured baseline: **583 passed, 2 skipped**. This plan adds no tests, so the expected result is **583 passed, 2 skipped**.

## Execution record

Executed 2026-09-19 on `refactor/group-sections-by-question`, two commits, all steps checked.
Final suite: **583 passed, 2 skipped**, unchanged, with **no test edited**. No deviations.

Task 1 step 3 was the point of the exercise and it held. Thirteen sections changed group and every
route prefix changed, and not one guard failed — they read group membership from the catalogue
rather than naming a group, which is what #257's design intended and what this is the first change
to actually test.

One verification bug of my own, worth recording because it nearly passed as evidence: the first
attempt to list the sidebar's hrefs matched `class="sidebar-link…" href=…`, but React emits `href`
before `className`, so the pattern found nothing and printed an empty result. An empty result read
as "no links" when the truth was "wrong pattern". Corrected, all fourteen links resolve under the
new prefixes.

## Pre-validated facts

- The old group names appear in exactly two files: `frontend/components/groups.js` and `frontend/README.md` lines 21-22. Everything else derives from the catalogue.
- The `"offline"` matches in `test_dashboard_measurement_contract.py:201-203,429` are the `scope` field on measurement rows (`["offline", "live_service"]`), unrelated to route groups.
- `frontend/README.md`'s Routes table also still says the scorecard has "seven tiles"; #260 made it thirteen.
- The new membership: `demand` = query, keyword. `serving` = engagement, satisfaction, freshness, diversity, fairness, safety, latency. `models` = recall, ranking, relevance, ope.

## File Structure

| File | Responsibility |
|---|---|
| Modify: `frontend/components/groups.js` | Three groups; each section reassigned; header comment rewritten. |
| Modify: `frontend/README.md` | Routes table, and the stale tile count. |

---

### Task 1: The catalogue

**Files:**
- Modify: `recsys-pipeline/frontend/components/groups.js`

**Interfaces:**
- Produces: `GROUPS` = `[{key: "demand"|"serving"|"models", label}]`; `SECTIONS[key].group` ∈ those keys; `SECTION_ROUTE` derived as `/<group>/<key>`.

- [x] **Step 1: Rewrite the catalogue**

The section entries keep their labels and descriptions exactly; only `group` changes, and the order
follows the new groups so the sidebar and overview read top to bottom.

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/groups.js")
original = p.read_text()

# Every label and description is carried over unchanged; only grouping and order change.
for phrase in ("What users asked for", "Where catalog supply diverges",
               "Impression to click to order", "Observed satisfaction",
               "How recent the served catalog", "Genre spread across served slates",
               "Outcome parity across", "Candidate safety policy accounting",
               "Live request and stage latency", "recall@k and hit rate",
               "AUC and log loss", "Graded relevance", "Estimated value and lift"):
    assert phrase in original, f"description missing before rewrite: {phrase!r}"

p.write_text('''// The dashboard's catalogue: which sections exist, what they are called, and where each
// one lives. Pure data with no imports, because the sidebar is a client component and
// must not pull chart and table code into the browser bundle -- section-registry.jsx
// holds the component map and is imported only by the server page.
//
// Grouped by the question a reader arrives with. An earlier split was "online" and
// "offline" prediction, which mislabelled three sections: intents is what users searched
// for, keyword gap is catalog supply against demand, and the engagement funnel is observed
// outcomes -- none of them a prediction, and latency is serving health rather than one too.
//
// Where the data comes from is a separate matter and still worth knowing: latency is the
// only section sourced purely from live telemetry, while satisfaction, freshness and safety
// merge live rows into offline ones when a backend is running. Everything else, including
// the model-evaluation group, is computed from logged Parquet and Redis.
export const GROUPS = [
  { key: "demand", label: "Demand & content" },
  { key: "serving", label: "Serving & outcomes" },
  { key: "models", label: "Model evaluation" },
];

export const SECTIONS = {
  query: {
    group: "demand", label: "Intents",
    description: "What users asked for, and how query length moves click-through.",
  },
  keyword: {
    group: "demand", label: "Keyword gap",
    description: "Where catalog supply diverges from query demand.",
  },
  engagement: {
    group: "serving", label: "Engagement funnel",
    description: "Impression to click to order, broken down by query and genre.",
  },
  satisfaction: {
    group: "serving", label: "Satisfaction",
    description: "Observed satisfaction of the slates that were served.",
  },
  freshness: {
    group: "serving", label: "Freshness",
    description: "How recent the served catalog is, against the freshness window.",
  },
  diversity: {
    group: "serving", label: "Diversity",
    description: "Genre spread across served slates.",
  },
  fairness: {
    group: "serving", label: "Fairness",
    description: "Outcome parity across the supported demographic groups.",
  },
  safety: {
    group: "serving", label: "Safety",
    description: "Candidate safety policy accounting and unsafe exposure.",
  },
  latency: {
    group: "serving", label: "Latency",
    description: "Live request and stage latency reported by the backend.",
  },
  recall: {
    group: "models", label: "Candidate recall",
    description: "recall@k and hit rate for each retrieval method.",
  },
  ranking: {
    group: "models", label: "Ranking quality",
    description: "AUC and log loss for each scoring signal.",
  },
  relevance: {
    group: "models", label: "Relevance",
    description: "Graded relevance \\u2014 NDCG and MRR \\u2014 across labeled slates.",
  },
  ope: {
    group: "models", label: "Off-policy evaluation",
    description: "Estimated value and lift of each candidate policy on logged events.",
  },
};

// Derived so a section cannot be listed in one place and routed to another.
export const SECTION_ROUTE = Object.fromEntries(
  Object.entries(SECTIONS).map(([key, { group }]) => [key, `/${group}/${key}`]),
);
'''.replace("\\\\u2014", "—"))
print("catalogue regrouped")
PY
node -e "import('./components/groups.js').then(m => {
  console.log('groups:', m.GROUPS.map(g => g.key).join(', '));
  const counts = {};
  for (const s of Object.values(m.SECTIONS)) counts[s.group] = (counts[s.group] ?? 0) + 1;
  console.log('sections per group:', JSON.stringify(counts));
  console.log('sample routes:', m.SECTION_ROUTE.query, m.SECTION_ROUTE.satisfaction, m.SECTION_ROUTE.ope);
})" 2>/dev/null
```

Expected: `groups: demand, serving, models`, `{"demand":2,"serving":7,"models":4}`, and
`/demand/query /serving/satisfaction /models/ope`.

- [x] **Step 2: Confirm the em-dash survived**

The relevance description contains an em-dash, which the heredoc above writes as an escape.

Run: `cd recsys-pipeline/frontend && grep -n 'Graded relevance' components/groups.js`
Expected: `Graded relevance — NDCG and MRR — across labeled slates.` with real em-dashes, not `—`.

- [x] **Step 3: Run the suite with no test edited**

Run: `cd recsys-pipeline && python3 -m pytest -q 2>&1 | tail -2`
Expected: `583 passed, 2 skipped`.

This is the step that matters most. The guards were written to read group membership from the
catalogue rather than to name a group; if any of them hardcoded `"online"`, it fails here — and the
fix is the guard, not the catalogue. Stop and report rather than editing a test to match.

- [x] **Step 4: Build and check the routes**

```bash
cd recsys-pipeline/frontend && npm run build 2>&1 | tail -12
```

Expected: `● /[group]/[section]` with thirteen prerendered paths under `/demand`, `/serving` and
`/models`.

- [x] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/groups.js
git commit -m "$(cat <<'MSG'
refactor(dashboard): group sections by the question they answer

"Online prediction" covered nine sections, three of which are not predictions:
intents is what users searched for, keyword gap is catalog supply against
demand, and the engagement funnel is observed outcomes. Latency is serving
health rather than a prediction either. The axis fitted the other ten and the
rest were stretched to match.

Demand & content, Serving & outcomes, Model evaluation. Each name is a question
a reader actually arrives with, and none asserts that its members predict
anything.

Where the data comes from is a separate matter, and the comment keeps it:
latency alone is purely live telemetry, and satisfaction, freshness and safety
merge live rows into offline ones when a backend is running.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: The documentation

**Files:**
- Modify: `recsys-pipeline/frontend/README.md`

**Interfaces:**
- Consumes: the group keys and labels from Task 1.

- [x] **Step 1: Rewrite the Routes table**

It also still says the scorecard has seven tiles, which #260 made thirteen.

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("README.md")
t = p.read_text()

old = """| Route | Sidebar group | Shows |
|---|---|---|
| `/` | — | The scorecard, seven tiles, each linking to that section's page |
| `/online/<section>` | Online prediction | What the serving path did: intents, keyword gap, engagement, satisfaction, freshness, diversity, fairness, safety, latency |
| `/offline/<section>` | Offline prediction | Models re-scored afterwards: recall, ranking, relevance, off-policy evaluation |"""
new = """| Route | Sidebar group | Shows |
|---|---|---|
| `/` | — | The scorecard: one tile per section, grouped, each linking to that section's page |
| `/demand/<section>` | Demand & content | What users asked for and what the catalog offers: intents, keyword gap |
| `/serving/<section>` | Serving & outcomes | What was served and what followed: engagement funnel, satisfaction, freshness, diversity, fairness, safety, latency |
| `/models/<section>` | Model evaluation | How the models score when re-run: candidate recall, ranking quality, relevance, off-policy evaluation |"""
assert t.count(old) == 1, "the Routes table does not match verbatim"
t = t.replace(old, new)

old_note = """Only `latency` is purely live telemetry — satisfaction, freshness and safety are offline rows with
live ones merged in when a backend was running."""
new_note = """The groups say what question a section answers, not where its data came from. On that second
question: only `latency` is purely live telemetry, and satisfaction, freshness and safety are offline
rows with live ones merged in when a backend was running."""
assert t.count(old_note) == 1
p.write_text(t.replace(old_note, new_note))
print("Routes table rewritten")
PY
sed -n '/^## Routes/,/^## Run/p' README.md | head -14
```

- [x] **Step 2: Verify nothing still points at the old routes**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
grep -rn '/online/\|/offline/' frontend/ --include='*.md' --include='*.jsx' --include='*.js' 2>/dev/null | grep -v '\.next' || echo "no stale route references"
python3 -m pytest integration-tests/test_doc_links.py -q 2>&1 | tail -1
```

Expected: `no stale route references`, then 5 passed.

- [x] **Step 3: Verify the rendered result**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
rm -rf .next
(npm run dev >/tmp/dev.log 2>&1 &)
until curl -sf -o /dev/null http://localhost:3000/ 2>/dev/null; do sleep 2; done
home=$(curl -s http://localhost:3000/)
echo -n "overview tiles: ";      grep -o 'class="metric-tile' <<<"$home" | wc -l | tr -d ' '
echo -n "overview groups: ";     grep -o 'scorecard-label' <<<"$home" | wc -l | tr -d ' '
echo -n "sidebar links: ";       grep -o 'class="sidebar-link' <<<"$home" | wc -l | tr -d ' '
for route in /demand/query /serving/satisfaction /models/ope /online/query; do
  printf '  %-26s HTTP %s\n' "$route" "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:3000$route)"
done
pkill -f 'next dev'; pkill -f 'next-server'
```

Expected: 13 tiles, 3 group headings, 14 sidebar links (Overview plus thirteen), 200 for the three
new routes and **404 for `/online/query`**.

- [x] **Step 4: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
(cd frontend && npm run validate:data)
git -C .. diff --name-only origin/master | grep -v '^\.superpowers/'
git -C .. diff --check && echo "diff --check clean"
```

Expected: `583 passed, 2 skipped`; the data contract valid; exactly two frontend files; clean.

- [x] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/README.md
git commit -m "$(cat <<'MSG'
docs: describe the three groups, and stop saying seven tiles

The Routes table named the two old groups and still said the scorecard has
seven tiles; #260 made it thirteen.

It also separates the two questions the old wording ran together: the groups
say what a section answers, and the note about live telemetry says where the
data came from.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

## Self-Review

**Spec coverage.** The catalogue, its comment and the section order → Task 1. The Routes table and the live-telemetry note → Task 2. Spec acceptance items 1-8 map to Task 1 step 3 (item 1), Task 1 step 4 (item 2), Task 2 step 3 (items 3, 4), Task 2 step 2 (item 5), and Task 2 step 4 (items 6, 7, 8).

**Placeholders.** None. Task 1 asserts all thirteen descriptions are present before rewriting the file, so a description cannot be silently lost in a full-file write, and step 2 checks the one character that the heredoc could mangle.

**Type consistency.** `GROUPS` keys `demand`/`serving`/`models` are used as `SECTIONS[key].group` values in the same file and appear in Task 2's table and route checks unchanged.

**One risk worth stating.** Task 1 rewrites `groups.js` wholesale rather than patching it, because thirteen `group:` values and the ordering all change at once. The guard against losing content is the assertion that every description string is present beforehand — but that checks the *old* file, not the new one. Step 1's own output (counts per group) and step 3's test run are what confirm the new file is complete and correct.
