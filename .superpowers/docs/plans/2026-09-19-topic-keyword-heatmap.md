# A topic × keyword relevance heatmap — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** A second heatmap crossing primary genre (`l2`) with keyword, beside the category grid, with its structural diagonal marked.

**Architecture:** `topic_grid` mirrors the `grid` key added in #264. The existing heatmap component is parameterised by its row field and used twice. The snapshot is refreshed by re-running the exporter alone against the last simulation's surviving Parquet.

**Tech Stack:** pandas, React client component, plain CSS.

**Spec:** `.superpowers/docs/specs/2026-09-19-topic-keyword-heatmap-design.md`

**Status:** executed and merged as #266. Every task below is complete.

## Global Constraints

- Branch and PR only. `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- `tops`, `grid`, `by_keyword`, `by_subkeyword` keep their current shapes.
- `MEASUREMENT_SCHEMA_VERSION` stays `"2.0"`; `validate_measurements.mjs` untouched.
- Heat stays `ctr / max(ctr)`, zero-anchored, ramp capped at 70% of `--accent`.
- **A dev server is running on port 3000 and must not be disturbed.** Do not `rm -rf .next`. `npm run build` writes the same `.next` the dev server reads, so run it only once, at the end, and expect the dev server to recompile afterwards.
- Measured baseline on this branch: **587 passed, 2 skipped**. This plan adds 3 tests → expect **590 passed, 2 skipped**.

## Pre-validated facts

- `top_keywords("l2")` already groups `["l2", "genres"]` with the needed aggregates; only the `rank <= 10` cap and the exporter's `.head(10)` stand between it and the heatmap.
- 18 primary genres × 18 genres = 324 cells maximum.
- `GENRE_FAMILY` has 18 keys, so both axes draw from the same 18-genre vocabulary — which is why the diagonal exists.
- The last simulation's Parquet survives at `/tmp/spark-recsys/movie-category-sim/training-samples` (4 files) and Redis is still up with that run's movie metadata, so the exporter can run alone.
- The exporter invocation the sim uses: `REDIS_HOST=localhost REDIS_PORT=6379 python3 frontend/export_dashboard_json.py --input <OUT_DIR> --output frontend/data/dashboard.json` plus `--experiences <SLATE_DIR>` when that directory exists.

## File Structure

| File | Responsibility |
|---|---|
| Modify: `services/python-modeling/analysis_dashboard_report.py` | `topic_grid`. |
| Modify: `frontend/export_dashboard_json.py` | Export it untruncated. |
| Modify: `frontend/components/keyword-report.jsx` | One parameterised heatmap, two call sites, diagonal marking. |
| Modify: `frontend/app/globals.css` | The diagonal outline. |
| Modify: `integration-tests/python_modeling/test_analysis_dashboard.py` | 2 tests. |
| Modify: `integration-tests/python_modeling/test_dashboard_routes.py` | 1 test for the diagonal marking. |
| Modify: `frontend/data/dashboard.json` | Re-exported, own commit. |

---

### Task 1: `topic_grid`

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Produces: `compute_keyword(df)["topic_grid"]` — columns `topic`, `keyword`, `movie_impressions`, `query_clicks`, `ctr`, sorted by topic then keyword.

- [x] **Step 1: Write the failing tests**

```python
def test_compute_keyword_topic_grid_crosses_primary_genre_with_keyword():
    """`topic` is the item's primary genre (l2); `keyword` is any genre it carries."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    df = pd.DataFrame({
        "user_id": ["u1", "u2"],
        "session_id": ["s1", "s2"],
        "item_id": ["i1", "i2"],
        "label": [1.0, 0.0],
        # Primary genre is the first: Comedy, then Horror.
        "genres": [["Comedy", "Romance"], ["Horror"]],
    })
    grid = dash.compute_keyword(df)["topic_grid"]
    rows = {(r["topic"], r["keyword"]): r for _, r in grid.iterrows()}

    assert ("Comedy", "Comedy") in rows, "the forced diagonal cell"
    assert ("Comedy", "Romance") in rows, "the co-carried genre"
    assert ("Horror", "Horror") in rows
    # The topic axis is the PRIMARY genre, so Romance never becomes a topic here.
    assert not any(t == "Romance" for t, _ in rows), "Romance is secondary, not a topic"
    assert rows[("Comedy", "Romance")]["ctr"] == 1.0


def test_compute_keyword_topic_grid_is_not_rank_capped():
    """324 cells is shippable; a capped row would read as the only genres served."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    others = ["Adventure", "War", "Western", "Comedy", "Children", "Crime",
              "Thriller", "Mystery", "Film-Noir", "Horror", "Drama", "Romance"]
    df = pd.DataFrame({
        "user_id": [f"u{i}" for i in range(len(others))],
        "session_id": [f"s{i}" for i in range(len(others))],
        "item_id": [f"i{i}" for i in range(len(others))],
        "label": [1.0] * len(others),
        "genres": [["Action", g] for g in others],
    })
    grid = dash.compute_keyword(df)["topic_grid"]
    action = grid[grid["topic"] == "Action"]
    assert len(action) > 10, f"expected more than ten keywords, got {len(action)}"
```

- [x] **Step 2: Run to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q -k topic_grid 2>&1 | tail -3`
Expected: 2 failed, `KeyError: 'topic_grid'`.

- [x] **Step 3: Generalise the grid helper**

The `category_grid()` closure added in #264 becomes one helper taking the level and its output name,
so the two grids cannot drift apart:

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("services/python-modeling/analysis_dashboard_report.py")
t = p.read_text()

old = '''    def category_grid():
        ex = lv[["l1", "genres", "label"]].explode("genres").dropna(subset=["genres"])
        ex = ex.assign(clk=(ex["label"] >= 1).astype(int))
        g = (ex.groupby(["l1", "genres"])
               .agg(movie_impressions=("clk", "size"), query_clicks=("clk", "sum"))
               .reset_index()
               .rename(columns={"l1": "category", "genres": "keyword"}))
        g["ctr"] = (g["query_clicks"] / g["movie_impressions"]).round(4)
        return g.sort_values(["category", "keyword"]).reset_index(drop=True)'''
new = '''    def cross_tab(level, row_name):
        ex = lv[[level, "genres", "label"]].explode("genres").dropna(subset=["genres"])
        ex = ex.assign(clk=(ex["label"] >= 1).astype(int))
        g = (ex.groupby([level, "genres"])
               .agg(movie_impressions=("clk", "size"), query_clicks=("clk", "sum"))
               .reset_index()
               .rename(columns={level: row_name, "genres": "keyword"}))
        g["ctr"] = (g["query_clicks"] / g["movie_impressions"]).round(4)
        return g.sort_values([row_name, "keyword"]).reset_index(drop=True)'''
assert t.count(old) == 1
t = t.replace(old, new)

old_ret = '''    return {"headline": headline, "by_keyword": by_keyword,
            "by_subkeyword": by_subkeyword, "tops": tops, "grid": category_grid()}'''
new_ret = '''    # l1 x genre is 6 x 18; l2 x genre is 18 x 18. Both fit the snapshot. l3 would be
    # ~180 x 18 and does not, which is why no l3 grid exists.
    return {"headline": headline, "by_keyword": by_keyword,
            "by_subkeyword": by_subkeyword, "tops": tops,
            "grid": cross_tab("l1", "category"),
            "topic_grid": cross_tab("l2", "topic")}'''
assert t.count(old_ret) == 1
p.write_text(t.replace(old_ret, new_ret))
print("cross_tab emits both grids")
PY
python3 -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q 2>&1 | tail -2
```

Expected: 25 passed — the 23 on master plus these 2. The two `grid` tests from #264 must still pass,
proving the refactor preserved the l1 behaviour.

- [x] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): cross primary genre with keyword as topic_grid"
```

---

### Task 2: Export it

**Files:**
- Modify: `recsys-pipeline/frontend/export_dashboard_json.py`

- [x] **Step 1: Add the key**

```bash
cd recsys-pipeline/frontend
python3 - <<'PY'
from pathlib import Path
p = Path("export_dashboard_json.py")
t = p.read_text()
old = '''        "grid": _records(kw["grid"]),'''
new = '''        "grid": _records(kw["grid"]),
        "topic_grid": _records(kw["topic_grid"]),'''
assert t.count(old) == 1
p.write_text(t.replace(old, new))
print("topic_grid exported")
PY
python3 export_dashboard_json.py --help >/dev/null && echo "exporter still parses"
```

- [x] **Step 2: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/export_dashboard_json.py
git commit -m "feat(dashboard): export topic_grid untruncated"
```

---

### Task 3: One component, two heatmaps, marked diagonal

**Files:**
- Modify: `recsys-pipeline/frontend/components/keyword-report.jsx`
- Modify: `recsys-pipeline/frontend/app/globals.css`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py`

**Interfaces:**
- Consumes: `data.grid`, `data.topic_grid`.

- [x] **Step 1: Write the failing test**

```python
def test_the_topic_heatmap_marks_its_structural_diagonal():
    """An item whose primary genre is Action always carries Action, so every (X, X) cell
    is forced. Unmarked, the bright diagonal reads as a finding."""
    report = (FRONTEND / "components" / "keyword-report.jsx").read_text(encoding="utf-8")
    assert "topic_grid" in report, "the section must render data.topic_grid"
    assert "heat-forced" in report, "the diagonal needs its own class to be distinguishable"
    css = (FRONTEND / "app" / "globals.css").read_text(encoding="utf-8")
    assert "heat-forced" in css, "heat-forced must be styled, or the marking is invisible"
    # The reader has to be told what the outline means.
    assert re.search(r"forced|always carries|by construction", report, re.I), (
        "the legend must say why those cells are outlined"
    )
```

- [x] **Step 2: Run to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q -k diagonal 2>&1 | tail -3`
Expected: FAIL on `topic_grid`.

- [x] **Step 3: Parameterise the component**

Rename `CategoryKeywordHeatmap` to `RelevanceHeatmap({ rows, rowKey, rowLabel, markDiagonal })`:
- `rowKey` — `"category"` or `"topic"`; replaces the hardcoded `r.category`.
- `rowLabel` — the corner header text.
- `markDiagonal` — when true, a cell whose row value equals its keyword also gets `heat-forced`.

Keep the empty-rows fallback exactly as it is; it is what the #264 guard tests.

- [x] **Step 4: Both call sites**

Below the existing category heatmap:

```jsx
<h3 className="report-subtitle">Relevance by topic and keyword</h3>
<p className="fine-print">
  Topic is the item&apos;s primary genre. Outlined cells are forced: an item whose primary
  genre is Action always carries Action, so those cells are filled by construction rather
  than measured.
</p>
<RelevanceHeatmap rows={data.topic_grid} rowKey="topic" rowLabel="topic" markDiagonal />
```

- [x] **Step 5: Style the marking**

```css
/* The forced diagonal: row value == keyword. True, but not a measurement. */
table.rpt.heat-grid td.heat-forced {
  outline: 2px solid var(--ink);
  outline-offset: -2px;
}
```

- [x] **Step 6: Verify**

```bash
cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_dashboard_routes.py -q 2>&1 | tail -1
```

Expected: 16 passed. Do not build yet — the snapshot has no `topic_grid`, so the topic heatmap
correctly shows its fallback note. Build once, in Task 4, after the data lands.

- [x] **Step 7: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/components/keyword-report.jsx recsys-pipeline/frontend/app/globals.css \
        recsys-pipeline/integration-tests/python_modeling/test_dashboard_routes.py
git commit -m "feat(dashboard): a topic x keyword heatmap with its diagonal marked"
```

---

### Task 4: Re-export the snapshot

**Files:**
- Modify: `recsys-pipeline/frontend/data/dashboard.json`

- [x] **Step 1: Confirm the inputs are still there**

```bash
find /tmp/spark-recsys/movie-category-sim/training-samples -name '*.parquet' | wc -l
docker ps --format '{{.Names}}' | grep redis || echo "REDIS DOWN - a fresh sim would be needed"
```

If the Parquet is gone or Redis is down, stop and say so: the fallback is a full
`run-movie-category-sim.sh` (~25 min), which also churns every other number.

- [x] **Step 2: Run the exporter alone**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
SIM_ROOT=/tmp/spark-recsys/movie-category-sim
args=(--input "$SIM_ROOT/training-samples" --output frontend/data/dashboard.json)
[[ -d "$SIM_ROOT/slates" ]] && args+=(--experiences "$SIM_ROOT/slates")
REDIS_HOST=localhost REDIS_PORT=6379 python3 frontend/export_dashboard_json.py "${args[@]}" 2>&1 \
  | grep -vE "INFO|WARN|^[0-9]{2}/"
(cd frontend && npm run validate:data)
```

- [x] **Step 3: Confirm the new key, and that nothing else moved**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
python3 - <<'PY'
import json, subprocess
new = json.load(open('data/dashboard.json'))
old = json.loads(subprocess.run(
    ['git', 'show', 'HEAD:recsys-pipeline/frontend/data/dashboard.json'],
    capture_output=True, text=True, cwd='/Users/linghuang/Git/Recsys-Streaming-Pipeline').stdout)
tg = new['keyword']['topic_grid']
topics = sorted({r['topic'] for r in tg}); kws = sorted({r['keyword'] for r in tg})
print(f'topic_grid: {len(tg)} cells, {len(topics)} topics x {len(kws)} keywords')
print(f'forced diagonal cells present: {sum(1 for r in tg if r["topic"] == r["keyword"])}')
# Which sections actually changed, so the diff can be explained rather than waved at.
for k in sorted(set(new) | set(old)):
    if k == 'keyword':
        continue
    if new.get(k) != old.get(k):
        print(f'  CHANGED: {k}')
PY
```

Expected: 324 or fewer cells, 18 forced diagonal cells, and the only changed sections being
freshness-age fields. Report precisely which sections moved.

- [x] **Step 4: Build and inspect the rendered page**

The dev server on port 3000 shares `.next`; it will recompile after this. Do not delete `.next`.

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
npm run build 2>&1 | grep -E 'Compiled|Failed|error' | head -3
f=.next/server/app/demand/keyword.html
echo -n "heat cells (both grids): "; grep -o 'heat-cell' "$f" | wc -l | tr -d ' '
echo -n "forced diagonal: ";        grep -o 'heat-forced' "$f" | wc -l | tr -d ' '
echo -n "hatched: ";                grep -o 'heat-absent' "$f" | wc -l | tr -d ' '
echo -n "fallback notes (want 0): "; grep -c 'predates the grid' "$f" || true
```

Expected: forced diagonal 18, no fallback notes.

- [x] **Step 5: Commit the snapshot alone, then run every gate**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/data/dashboard.json
git commit -m "chore(dashboard): re-export the snapshot with topic_grid"
cd recsys-pipeline && python3 -m pytest -q 2>&1 | tail -2
```

Expected: `590 passed, 2 skipped`.

## Self-Review

**Spec coverage.** Acceptance 1 → Task 1; 2 → Task 2; 3 → Task 3; 4 → the #264 guard, which Task 3 step 3 preserves deliberately; 5 → Task 4 step 3, which diffs every section rather than asserting nothing moved; 6 → Task 4 steps 2, 4 and 5.

**Placeholders.** None. Task 3 describes the component change in prose plus the exact JSX and CSS, rather than a full file dump, because the file is already open in the diff and a wholesale rewrite would obscure a rename.

**Type consistency.** `cross_tab(level, row_name)` produces `row_name` as the row column; Task 2 exports `topic_grid`; Task 3 reads it via `rowKey="topic"`. The `grid`/`category` path is unchanged and its #264 tests are the regression check.

**Risk worth stating.** Task 1 refactors a function that #264 just shipped. The two `grid` tests from #264 are the guard: if the refactor changes l1 behaviour they fail, and Task 1 step 3 runs the whole file for exactly that reason.

**The diagonal marking is a judgement call.** Outlining is the lightest treatment that still says "do not read this as a finding". Blanking those cells would be wrong — their values are true — and dropping the topic axis to exclude the primary genre would make each row's support incomparable to the others.
