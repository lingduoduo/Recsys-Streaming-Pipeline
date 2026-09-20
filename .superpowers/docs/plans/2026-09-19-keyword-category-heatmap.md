# A category × keyword relevance heatmap — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Emit the category × keyword cross-tab that `compute_keyword` already builds and discards, and render it as a 6 × 18 heatmap in the Keyword gap section.

**Architecture:** A new `grid` key on `compute_keyword`'s result, exported without truncation, rendered with the same heat ramp the section's existing token chips use. The three top-ten tables and their `rank <= 10` cap are untouched, because lifting the cap for `l2` and `l3` would put thousands of rows in a committed snapshot.

**Tech Stack:** pandas in `analysis_dashboard_report.py`, React client component, plain CSS, and one `run-movie-category-sim.sh` run for the snapshot.

**Spec:** `.superpowers/docs/specs/2026-09-19-keyword-category-heatmap-design.md`

**Status:** executed and merged as #264/#265. Every task below is complete.

## Global Constraints

- Branch and pull request only. Nothing is committed to `master` directly. Run `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- Four commits, in this order: the computation, the export, the component, the regenerated snapshot. The snapshot is last and alone.
- `tops`, `by_keyword` and `by_subkeyword` keep their current shapes and truncations.
- `validate_measurements.mjs` is untouched.
- `MEASUREMENT_SCHEMA_VERSION` stays `"2.0"`.
- The heatmap reuses `color-mix(in oklab, var(--accent) calc(var(--token-score) * 70%), var(--accent-soft))` — the ramp is capped at 70% so dark ink stays at 5.18:1 contrast across the scale.
- `keyword-report.jsx` stays a client component.
- Measured baseline: **584 passed, 2 skipped**. This plan adds three tests, so the expected result is **587 passed, 2 skipped**.

## Pre-validated facts

- `GENRE_FAMILY` maps 18 genres onto 6 families: Action&Adventure, Comedy, Crime&Thriller, Drama&Romance, SciFi&Fantasy, Other. `l1(genres) = family_of(primary_genre(genres))`.
- The committed snapshot's `by_keyword` carries all 18 genres, so the grid's column axis is 18 wide and its row axis 6 tall — 108 cells maximum.
- `top_keywords(level)` already computes `groupby([level, "genres"])` with `movie_impressions`, `query_clicks` and `ctr`, then caps at `rank <= 10` per level value.
- `export_dashboard_json.py` reduces each level to `.head(10)`, which is why the snapshot holds one family.
- The simulation's Parquet is gone from `/tmp`; the snapshot must come from a fresh run.
- Prerequisites for that run are present: Docker running, JDK 17 (`corretto-17.0.12`), `SPARK_HOME=~/opt/spark-3.5.1-bin-hadoop3`, `sbt` on PATH, and `spark-recsys-job.jar` already built.

## File Structure

| File | Responsibility |
|---|---|
| Modify: `services/python-modeling/analysis_dashboard_report.py` | `compute_keyword` gains `grid`. |
| Modify: `frontend/export_dashboard_json.py` | Export `grid` untruncated. |
| Modify: `frontend/components/keyword-report.jsx` | The heatmap component and its placement. |
| Modify: `frontend/app/globals.css` | Heat cell, grid layout, absent-pair hatch. |
| Modify: `integration-tests/python_modeling/test_analysis_dashboard.py` | Two tests for `grid`. |
| Modify: `integration-tests/python_modeling/test_dashboard_routes.py` | One test for the absent-grid fallback. |
| Modify: `frontend/data/dashboard.json` | Regenerated, in its own commit. |

---

### Task 1: The computation

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py` (`compute_keyword`)
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Produces: `compute_keyword(df)["grid"]` — a DataFrame with columns `category`, `keyword`, `movie_impressions`, `query_clicks`, `ctr`, sorted by category then keyword. Task 2 exports it.

- [x] **Step 1: Write the failing tests**

Append to `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`:

```python
def test_compute_keyword_grid_crosses_category_with_keyword():
    """The heatmap's axes. `keyword` is a genre; `category` is that genre's family.

    A row is exploded across its genres, so a film tagged Action and Comedy contributes
    to both keyword columns under its own category -- the same convention top_keywords
    uses, so the heatmap and the taxonomy tables agree.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    df = pd.DataFrame({
        "user_id": ["u1", "u2", "u3"],
        "session_id": ["s1", "s2", "s3"],
        "item_id": ["i1", "i2", "i3"],
        "label": [1.0, 0.0, 1.0],
        "genres": [["Action", "Comedy"], ["Action"], ["Documentary"]],
    })
    grid = dash.compute_keyword(df)["grid"]
    rows = {(r["category"], r["keyword"]): r for _, r in grid.iterrows()}

    # Action is the primary genre of rows 1 and 2, so their family is Action&Adventure.
    assert ("Action&Adventure", "Action") in rows
    assert ("Action&Adventure", "Comedy") in rows, "the exploded second genre must appear"
    assert ("Other", "Documentary") in rows, "Documentary's family is Other"

    action = rows[("Action&Adventure", "Action")]
    assert action["movie_impressions"] == 2 and action["query_clicks"] == 1
    assert action["ctr"] == 0.5


def test_compute_keyword_grid_is_not_rank_capped():
    """tops caps at ten per family for its tables; the grid must not, or the heatmap
    would show a truncated row and look like a measurement."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    genres = ["Action", "Adventure", "War", "Western", "Comedy", "Children",
              "Crime", "Thriller", "Mystery", "Film-Noir", "Horror", "Drama"]
    # Every row's primary genre is Action, so all twelve keywords land in one family.
    df = pd.DataFrame({
        "user_id": [f"u{i}" for i in range(len(genres))],
        "session_id": [f"s{i}" for i in range(len(genres))],
        "item_id": [f"i{i}" for i in range(len(genres))],
        "label": [1.0] * len(genres),
        "genres": [["Action", g] if g != "Action" else ["Action"] for g in genres],
    })
    grid = dash.compute_keyword(df)["grid"]
    in_family = grid[grid["category"] == "Action&Adventure"]
    assert len(in_family) > 10, f"expected more than ten keywords, got {len(in_family)}"
```

- [x] **Step 2: Run them to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q -k grid 2>&1 | tail -3`
Expected: 2 failed with `KeyError: 'grid'`.

- [x] **Step 3: Emit the grid**

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("services/python-modeling/analysis_dashboard_report.py")
t = p.read_text()

anchor = '''    tops = {lvl: top_keywords(lvl) for lvl in ("l1", "l2", "l3")}'''
addition = '''    # The heatmap's cross-tab: every observed (family, genre) pair, with no rank cap.
    # top_keywords caps at ten per level because its consumers are top-ten tables; a
    # heatmap with a truncated row would read as "these are the only genres served".
    # Bounded by the genre vocabulary at 6 families x 18 genres, so it stays small
    # enough to ship in the snapshot -- which is not true of l2 (18x18) or l3 (~180x18).
    def category_grid():
        ex = lv[["l1", "genres", "label"]].explode("genres").dropna(subset=["genres"])
        ex = ex.assign(clk=(ex["label"] >= 1).astype(int))
        g = (ex.groupby(["l1", "genres"])
               .agg(movie_impressions=("clk", "size"), query_clicks=("clk", "sum"))
               .reset_index()
               .rename(columns={"l1": "category", "genres": "keyword"}))
        g["ctr"] = (g["query_clicks"] / g["movie_impressions"]).round(4)
        return g.sort_values(["category", "keyword"]).reset_index(drop=True)

    tops = {lvl: top_keywords(lvl) for lvl in ("l1", "l2", "l3")}'''
assert t.count(anchor) == 1
t = t.replace(anchor, addition)

old_return = '''    return {"headline": headline, "by_keyword": by_keyword,
            "by_subkeyword": by_subkeyword, "tops": tops}'''
new_return = '''    return {"headline": headline, "by_keyword": by_keyword,
            "by_subkeyword": by_subkeyword, "tops": tops, "grid": category_grid()}'''
assert t.count(old_return) == 1
p.write_text(t.replace(old_return, new_return))
print("compute_keyword emits grid")
PY
python3 -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q -k grid 2>&1 | tail -2
```

Expected: 2 passed.

- [x] **Step 4: Run the whole file, then commit**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q 2>&1 | tail -2`
Expected: 23 passed — the 21 existing plus these 2.

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "$(cat <<'MSG'
feat(dashboard): emit the category x keyword cross-tab

compute_keyword already grouped by (l1, genres) for its taxonomy tables and
capped the result at ten rows per family. The grid is the same cross-tab
without the cap: every observed (family, genre) pair, bounded by the genre
vocabulary at 6 x 18.

It is a separate key rather than a lifted cap, because the cap exists for
tables that display ten rows, and lifting it for l2 (18x18) or l3 (~180x18)
would put thousands of rows in a committed snapshot.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: The export

**Files:**
- Modify: `recsys-pipeline/frontend/export_dashboard_json.py`

**Interfaces:**
- Consumes: `compute_keyword(df)["grid"]`.
- Produces: `data.keyword.grid` — an array of `{category, keyword, movie_impressions, query_clicks, ctr}`.

- [x] **Step 1: Export it untruncated**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("export_dashboard_json.py")
t = p.read_text()
old = '''        "tops": {lvl: _records(kw["tops"][lvl].head(10)) for lvl in ("l1", "l2", "l3")},'''
new = '''        "tops": {lvl: _records(kw["tops"][lvl].head(10)) for lvl in ("l1", "l2", "l3")},
        # No head(): the heatmap needs every cell, and the grid is bounded at 6 x 18
        # by the genre vocabulary. The tops above stay truncated -- they feed tables.
        "grid": _records(kw["grid"]),'''
assert t.count(old) == 1
p.write_text(t.replace(old, new))
print("grid exported")
PY
python3 export_dashboard_json.py --help >/dev/null && echo "exporter still parses"
```

- [x] **Step 2: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/export_dashboard_json.py
git commit -m "$(cat <<'MSG'
feat(dashboard): export the keyword grid without truncating it

The three taxonomy tops keep their head(10) because they feed ten-row tables.
The grid does not: a heatmap missing cells reads as "these are the only genres
served under this category", which is a different and false claim.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 3: The component

**Files:**
- Modify: `recsys-pipeline/frontend/components/keyword-report.jsx`
- Modify: `recsys-pipeline/frontend/app/globals.css`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`

**Interfaces:**
- Consumes: `data.keyword.grid`.

- [x] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`:

```python
def test_the_keyword_heatmap_degrades_when_the_grid_is_absent():
    """Every snapshot exported before the grid existed lacks it.

    An empty table would read as "no genres were served"; the section has to say the
    snapshot predates the grid instead.
    """
    report = (FRONTEND / "components" / "keyword-report.jsx").read_text(encoding="utf-8")
    assert "grid" in report, "keyword-report.jsx must render data.keyword.grid"
    heatmap = re.search(r"function CategoryKeywordHeatmap\((.*?)\n\}", report, re.S)
    assert heatmap, "the heatmap must be its own component"
    assert re.search(r"re-?export|fresh export|older snapshot", heatmap.group(1), re.I), (
        "an absent grid must explain itself rather than render an empty table"
    )
```

- [x] **Step 2: Run it to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py::test_the_keyword_heatmap_degrades_when_the_grid_is_absent -q 2>&1 | tail -2`
Expected: FAIL — no such component.

- [x] **Step 3: Add the heatmap component**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("components/keyword-report.jsx")
t = p.read_text()

anchor = "function TokenHeatmap("
component = '''// Categories down the side, keywords across the top, CTR as heat. A pair the run never
// served is absent from the rows and renders hatched rather than at the cold end of the
// ramp -- never served and served-but-never-clicked are different facts.
function CategoryKeywordHeatmap({ rows }) {
  if (!rows?.length) {
    return (
      <p className="fine-print">
        No category grid in this snapshot — it predates the grid, so re-export to populate it.
      </p>
    );
  }
  const categories = [...new Set(rows.map((r) => r.category))].sort();
  const keywords = [...new Set(rows.map((r) => r.keyword))].sort();
  const cells = new Map(rows.map((r) => [`${r.category}\\u0000${r.keyword}`, r]));
  const max = Math.max(...rows.map((r) => r.ctr ?? 0), 0);

  return (
    <div className="table-shell">
      <table className="rpt heat-grid">
        <thead>
          <tr>
            <th>category</th>
            {keywords.map((k) => <th key={k} className="num">{k}</th>)}
          </tr>
        </thead>
        <tbody>
          {categories.map((category) => (
            <tr key={category}>
              <th scope="row">{category}</th>
              {keywords.map((keyword) => {
                const cell = cells.get(`${category}\\u0000${keyword}`);
                if (!cell) {
                  return <td key={keyword} className="num heat-absent" title={`${category} / ${keyword}: never served`} />;
                }
                const t = max === 0 ? 0 : (cell.ctr ?? 0) / max;
                return (
                  <td key={keyword} className="num heat-cell" style={{ "--token-score": t }}
                      title={`${category} / ${keyword}: CTR ${share(cell.ctr)} over ${count(cell.movie_impressions)} impressions`}>
                    {share(cell.ctr)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TokenHeatmap('''
assert t.count(anchor) == 1
t = t.replace(anchor, component, 1)

# Place it above the taxonomy tables, under its own subtitle.
old_tops = '''      {["l1", "l2", "l3"].map((level) => {'''
new_tops = '''      <h3 className="report-subtitle">Relevance by category and keyword</h3>
      <CategoryKeywordHeatmap rows={data.grid} />

      {["l1", "l2", "l3"].map((level) => {'''
assert t.count(old_tops) == 1
p.write_text(t.replace(old_tops, new_tops).replace("\\\\u0000", "\\u0000"))
print("heatmap component added")
PY
grep -n 'CategoryKeywordHeatmap\|heat-cell\|heat-absent' components/keyword-report.jsx | head
```

- [x] **Step 4: Style the cells**

```bash
cd recsys-pipeline/frontend
cat >> app/globals.css <<'CSS'

/* The category x keyword heatmap. Same ramp as .token-chip, capped at 70% of the hue so
   dark ink stays at 5.18:1 across the scale. */
table.rpt.heat-grid th[scope="row"] {
  position: sticky;
  left: 0;
  background: var(--surface);
  text-align: left;
  text-transform: none;
  letter-spacing: 0;
  font-size: 0.82rem;
  white-space: nowrap;
}

table.rpt.heat-grid td.heat-cell {
  background: color-mix(in oklab, var(--accent) calc(var(--token-score) * 70%), var(--accent-soft));
  font-variant-numeric: tabular-nums;
}

/* Never served, which is not the same as served and never clicked. */
table.rpt.heat-grid td.heat-absent {
  background: repeating-linear-gradient(135deg, #f8fafc, #f8fafc 4px, #eef2f7 4px, #eef2f7 8px);
}
CSS
```

- [x] **Step 5: Verify against the current snapshot, which has no grid**

```bash
cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -1
cd frontend && npm run build 2>&1 | tail -3
```

Expected: 15 passed, and a successful build. The snapshot has no `grid`, so the section shows the
re-export note — which is the fallback path working.

- [x] **Step 6: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/keyword-report.jsx recsys-pipeline/frontend/app/globals.css \
        recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py
git commit -m "$(cat <<'MSG'
feat(dashboard): a category x keyword relevance heatmap

Categories down the side, keywords across the top, CTR as heat over the same
ramp the token chips use. A pair the run never served renders hatched rather
than at the cold end of the scale: never served and served-but-never-clicked
are different facts, and a sequential ramp cannot say both.

A snapshot without the grid -- every one exported before this -- gets a note
saying so, because an empty table would read as "no genres were served".

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 4: The snapshot

**Files:**
- Modify: `recsys-pipeline/frontend/data/dashboard.json`

- [x] **Step 1: Confirm nothing is holding the ports**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
pkill -f 'next dev' 2>/dev/null; pkill -f 'next-server' 2>/dev/null
lsof -nP -iTCP:3000 -sTCP:LISTEN 2>/dev/null | tail -1 || echo "3000 free"
docker ps --format '{{.Names}}' | head -5 || echo "no containers"
```

The simulation begins with `docker compose down -v`, so leftover containers are handled — but a dev
server on 3000 is not, and the exporter step writes the file it would be serving.

- [x] **Step 2: Run the simulation**

Run from `recsys-pipeline`: `bash scripts/run-movie-category-sim.sh 2>&1 | tail -40`

Expected: it ends with the closing banner naming `recsys-pipeline/frontend/data/dashboard.json`.
This takes several minutes and brings up Kafka, ZooKeeper and Redis, runs Spark jobs, and rewrites the
snapshot. If it fails, stop — a half-written snapshot is worse than the old one, and `git checkout`
restores it.

- [x] **Step 3: Confirm the grid arrived**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
python3 -c "
import json
d = json.load(open('data/dashboard.json'))
grid = (d.get('keyword') or {}).get('grid') or []
cats = sorted({r['category'] for r in grid})
kws = sorted({r['keyword'] for r in grid})
print(f'grid rows: {len(grid)}')
print(f'categories: {len(cats)} -> {cats}')
print(f'keywords: {len(kws)}')
print('tops l1 rows still ten:', len(d['keyword']['tops']['l1']))
"
npm run validate:data
```

Expected: more than one category, and `tops.l1` still ten rows.

- [x] **Step 4: See it rendered**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
rm -rf .next
(npm run dev >/tmp/dev.log 2>&1 &)
until curl -sf -o /dev/null http://localhost:3000/demand/keyword 2>/dev/null; do sleep 2; done
page=$(curl -s http://localhost:3000/demand/keyword)
echo -n "heat cells: ";   grep -o 'heat-cell' <<<"$page" | wc -l | tr -d ' '
echo -n "absent cells: "; grep -o 'heat-absent' <<<"$page" | wc -l | tr -d ' '
echo -n "errors in log: "; grep -cE '⨯|Error:' /tmp/dev.log || echo 0
pkill -f 'next dev'; pkill -f 'next-server'
```

Expected: a non-zero heat-cell count, and zero errors.

- [x] **Step 5: Commit the snapshot alone**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/data/dashboard.json
git commit -m "$(cat <<'MSG'
chore(dashboard): regenerate the snapshot so it carries the keyword grid

From a fresh run-movie-category-sim.sh, because the previous run's Parquet is
gone from /tmp and the committed snapshot predates the grid.

Every number on every page moves. The snapshot is a property of the run that
produced it and this is a different run: traffic is resampled, freshness ages
shift with wall-clock, and the off-policy section reflects whatever replay
buffer this run left -- likely none, with no backend running.

Committed alone so the code review is not buried under it.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
MSG
)"
```

- [x] **Step 6: Run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest -q 2>&1 | tail -2
(cd frontend && npm run validate:data && npm run build 2>&1 | tail -3)
git -C .. diff --check && echo "diff --check clean"
```

Expected: `587 passed, 2 skipped`; the data contract valid; a successful build; clean.

## Self-Review

**Spec coverage.** The `grid` computation → Task 1. The untruncated export → Task 2. The heatmap, its absent-pair treatment and its fallback → Task 3. The regenerated snapshot → Task 4. Spec acceptance items 1-8 map to Task 4 step 6 (items 1, 4, 7, 8), Task 1 steps 1-4 (item 2), Task 4 step 3 (items 3, 6), and Task 4 step 4 (item 5).

**Placeholders.** None. Every replacement asserts its match count, and Task 1's tests state the expected arithmetic (2 impressions, 1 click, CTR 0.5) rather than asserting the shape alone.

**Type consistency.** `grid` is produced in Task 1 with columns `category`, `keyword`, `movie_impressions`, `query_clicks`, `ctr`; Task 2 exports those names; Task 3's component reads `r.category`, `r.keyword`, `r.ctr`, `r.movie_impressions`. `--token-score` is the same custom property the existing `.token-chip` rule uses, so the CSS-variable guard still finds it declared.

**Two risks worth stating.** The simulation is the only step that cannot be undone by `git checkout` alone — it rewrites `/tmp` state and cycles Docker — and if it half-fails the snapshot may be written but wrong. Step 2 says to stop rather than push through, and the snapshot is restorable with `git checkout` since it is committed.

The heatmap's cells are not comparable to each other in the way a heatmap implies: CTR over nine impressions sits beside CTR over nine thousand, coloured identically. The title attribute carries the impression count for that reason, but colour is what a reader takes in, and colour does not carry support.
