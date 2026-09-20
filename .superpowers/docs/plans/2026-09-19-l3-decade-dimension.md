# Fixing L3 and adding decade as a dimension — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry `release_year` into the analysis frame so L3 means something, and add decade as a dimension in its own right.

**Architecture:** One extra projection in `load_samples`, reusing the Redis fetch already made for genres; two new keys on `compute_keyword` built from the existing `dist()` and `cross_tab()` helpers.

**Tech Stack:** pandas, React client component, one full simulation run.

**Spec:** `.superpowers/docs/specs/2026-09-19-l3-decade-dimension-design.md`

## Global Constraints

- Branch and PR only. `test "$(git branch --show-current)" != "master" || exit 1` in the same shell invocation as every commit.
- **L1 and L2 semantics do not change.** `GENRE_FAMILY`, `primary_genre`, `family_of`, `dist()` and `tops` keep their current behaviour; the existing tests for them are the regression guard.
- `decade()` is reused as-is, never reimplemented.
- The year is read with `.get("release_year")`. The existing `fetch_movie_meta` monkeypatch supplies dicts without that key, and indexing would break a passing test.
- Redis unreachable must stay graceful: `release_year` all-`None`, L3 back to `·unknown`.
- `MEASUREMENT_SCHEMA_VERSION` stays `"2.0"`; no new dashboard section.
- A dev server may be live on 3000. Do not `rm -rf .next`.
- Baseline on master: **599 passed, 2 skipped**.

## Pre-validated facts

- `fetch_movie_meta` returns `{item_id, genres, release_year}` and swallows every exception, returning `[]`.
- `load_samples`'s only production caller is `export_dashboard_json.py:77`.
- `compute_keyword` already has `lv` (with `l1`/`l2`/`l3`), `dist(col)` over `d`, and `cross_tab(level, row_name)`.
- Redis currently holds 400 movies with years spanning five decades (89/87/74/105/45), so the fix has data to prove itself against.
- `/tmp/spark-recsys/movie-category-sim/training-samples` is **gone**, so the snapshot needs a full sim run.
- `test_both_heatmaps_share_one_heat_domain` asserts exactly one `heatDomain(` call containing `grid` and `topic_grid`; adding a third argument to that same call keeps it passing.

## File Structure

| File | Responsibility |
|---|---|
| Modify: `services/python-modeling/analysis_dashboard_report.py` | Carry the year; `by_decade`; `decade_grid`. |
| Modify: `frontend/export_dashboard_json.py` | Export both new keys. |
| Modify: `frontend/components/keyword-report.jsx` | Decade table + heatmap; pool three grids. |
| Modify: `integration-tests/python_modeling/test_analysis_dashboard.py` | Year-carrying and decade-independence tests. |
| Modify: `frontend/data/dashboard.json` | Regenerated, own commit. |
| Modify: `frontend/README.md` | Document the third grid. |

---

### Task 1: Carry the year, and prove L3 comes alive

- [ ] **Step 1: Write the failing tests** — append to `test_analysis_dashboard.py`:

```python
def test_load_samples_carries_release_year_from_redis(tmp_path, monkeypatch):
    """The year was never missing -- it was projected away. l3 depends on it."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash
    import feature_derivations as genre_meta

    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id": ["u1", "u2"], "session_id": ["s1", "s2"],
        "item_id": ["item_1", "item_2"], "label": [1.0, 0.0],
        "genres": [["Action"], ["Comedy"]],
    }).to_parquet(parquet, index=False)
    monkeypatch.setattr(genre_meta, "fetch_movie_meta", lambda host, port: [
        {"item_id": "item_1", "genres": ["Action"], "release_year": 1994},
        {"item_id": "item_2", "genres": ["Comedy"], "release_year": 2011},
    ])

    out = dash.load_samples(str(parquet), "redis.test", 6380)
    assert out["release_year"].tolist() == [1994, 2011]


def test_l3_carries_a_real_decade_when_metadata_is_available(tmp_path, monkeypatch):
    """Acceptance for the whole change: l3 stops being a relabelled copy of l2."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash
    import feature_derivations as genre_meta

    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id": ["u1", "u2"], "session_id": ["s1", "s2"],
        "item_id": ["item_1", "item_2"], "label": [1.0, 1.0],
        "genres": [["Action"], ["Action"]],
    }).to_parquet(parquet, index=False)
    monkeypatch.setattr(genre_meta, "fetch_movie_meta", lambda host, port: [
        {"item_id": "item_1", "genres": ["Action"], "release_year": 1994},
        {"item_id": "item_2", "genres": ["Action"], "release_year": 2011},
    ])

    df = dash.load_samples(str(parquet), "redis.test", 6380)
    l3 = set(dash.compute_keyword(df)["tops"]["l3"]["l3"])
    assert l3 == {"Action·1990s", "Action·2010s"}, f"got {l3}"
    assert not any("unknown" in v for v in l3)


def test_l3_still_falls_back_to_unknown_without_metadata(tmp_path, monkeypatch):
    """The fix adds a capability, not a dependency: no Redis, same behaviour as before."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash
    import feature_derivations as genre_meta

    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id": ["u1"], "session_id": ["s1"], "item_id": ["item_1"],
        "label": [1.0], "genres": [["Action"]],
    }).to_parquet(parquet, index=False)
    monkeypatch.setattr(genre_meta, "fetch_movie_meta", lambda host, port: [])

    df = dash.load_samples(str(parquet), "redis.test", 6380)
    assert list(dash.compute_keyword(df)["tops"]["l3"]["l3"]) == ["Action·unknown"]


def test_carrying_the_year_leaves_l1_and_l2_untouched(tmp_path, monkeypatch):
    """L1/L2 derive from genres alone and must not move."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash
    import feature_derivations as genre_meta

    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id": ["u1", "u2"], "session_id": ["s1", "s2"],
        "item_id": ["item_1", "item_2"], "label": [1.0, 0.0],
        "genres": [["Action", "Comedy"], ["Documentary"]],
    }).to_parquet(parquet, index=False)
    monkeypatch.setattr(genre_meta, "fetch_movie_meta", lambda host, port: [
        {"item_id": "item_1", "genres": ["Action", "Comedy"], "release_year": 1994},
        {"item_id": "item_2", "genres": ["Documentary"], "release_year": 2011},
    ])

    df = dash.load_samples(str(parquet), "redis.test", 6380)
    kw = dash.compute_keyword(df)
    assert set(kw["tops"]["l1"]["l1"]) == {"Action&Adventure", "Other"}
    assert set(kw["tops"]["l2"]["l2"]) == {"Action", "Documentary"}
```

- [ ] **Step 2: Run them** — `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q -k "release_year or l3 or untouched" 2>&1 | tail -3`. Expected: the year and real-decade tests fail; the fallback and L1/L2 tests already pass.

- [ ] **Step 3: Carry the year**

```bash
cd recsys-pipeline
python3 - <<'PY'
from pathlib import Path
p = Path("services/python-modeling/analysis_dashboard_report.py")
t = p.read_text()
old = '''    missing_genres = ~df["genres"].map(bool)
    if missing_genres.any():
        from feature_derivations import fetch_movie_meta
        meta = {m["item_id"]: m["genres"] for m in fetch_movie_meta(host, port)}
        enriched = df.loc[missing_genres, "item_id"].astype(str).map(lambda i: meta.get(i, []))
        for idx, genres in enriched.items():
            df.at[idx, "genres"] = list(genres)
    return df'''
new = '''    # One Redis scan serves both hydrations. The year matters because l3 is
    # primary_genre x decade, and without it every l3 value ends in `unknown` -- which made l3 a
    # relabelled copy of l2 for as long as it has existed. fetch_movie_meta already returned the
    # year; this function used to project it away.
    missing_genres = ~df["genres"].map(bool)
    need_year = "release_year" not in df.columns
    if missing_genres.any() or need_year:
        from feature_derivations import fetch_movie_meta
        meta = {str(m["item_id"]): m for m in fetch_movie_meta(host, port)}
        if missing_genres.any():
            enriched = df.loc[missing_genres, "item_id"].astype(str).map(
                lambda i: (meta.get(i) or {}).get("genres") or [])
            for idx, genres in enriched.items():
                df.at[idx, "genres"] = list(genres)
        if need_year:
            # .get, not [...]: a caller's fake metadata need not carry the key, and an
            # unreachable Redis yields {} -- both must leave the year None, not raise.
            df["release_year"] = df["item_id"].astype(str).map(
                lambda i: (meta.get(i) or {}).get("release_year"))
    return df'''
assert t.count(old) == 1
p.write_text(t.replace(old, new))
print("release_year carried")
PY
python3 -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q 2>&1 | tail -2
```

Expected: 29 passed (25 on master plus these 4).

- [ ] **Step 4: Commit** — `git commit -m "fix(dashboard): carry release_year so l3 means something"`

---

### Task 2: Decade as a dimension

- [ ] **Step 1: Write the failing tests**

```python
def test_decade_grouping_is_independent_of_genre():
    """Decade is the only dimension here not derived from the genre string. Two items sharing a
    decade must group together whatever their genres, and an item's decade must not move when its
    genres do -- which is what makes a decade x keyword cell a real joint observation rather than
    structure imposed by a shared derivation."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    df = pd.DataFrame({
        "user_id": ["u1", "u2", "u3"], "session_id": ["s1", "s2", "s3"],
        "item_id": ["i1", "i2", "i3"], "label": [1.0, 1.0, 0.0],
        "genres": [["Action"], ["Documentary"], ["Action"]],
        "release_year": [1994, 1997, 2011],
    })
    kw = dash.compute_keyword(df)

    by = {r["decade"]: r for _, r in kw["by_decade"].iterrows()}
    # Action 1994 and Documentary 1997 share a decade despite sharing no genre or family.
    assert by["1990s"]["movie_impressions"] == 2
    assert by["2010s"]["movie_impressions"] == 1

    # The same genre spans two decades, so the axis is not a genre relabelling.
    grid = kw["decade_grid"]
    action = {r["decade"] for _, r in grid.iterrows() if r["keyword"] == "Action"}
    assert action == {"1990s", "2010s"}

    # And no cell is forced: unlike topic_grid, decade and keyword share no derivation.
    assert not any(r["decade"] == r["keyword"] for _, r in grid.iterrows())


def test_by_decade_reports_the_same_metrics_as_by_keyword():
    """Standalone means comparable: the decade breakdown carries the keyword column set."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    df = pd.DataFrame({
        "user_id": ["u1", "u2"], "session_id": ["s1", "s2"], "item_id": ["i1", "i2"],
        "label": [2.0, 0.0], "genres": [["Action"], ["Comedy"]],
        "release_year": [1994, 2011],
    })
    kw = dash.compute_keyword(df)
    shared = set(kw["by_keyword"].columns) - {"keyword"}
    assert shared <= set(kw["by_decade"].columns), (
        f"by_decade is missing {sorted(shared - set(kw['by_decade'].columns))}"
    )


def test_decade_uses_the_shared_derivation():
    """decade() is reused, not reimplemented, so bucketing cannot drift between callers."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    import feature_derivations as fd

    df = pd.DataFrame({
        "user_id": ["u1"], "session_id": ["s1"], "item_id": ["i1"],
        "label": [1.0], "genres": [["Action"]], "release_year": [2007],
    })
    got = list(dash.compute_keyword(df)["by_decade"]["decade"])
    assert got == [fd.decade(2007)] == ["2000s"]
```

- [ ] **Step 2: Run them** — expected: 3 failed on `KeyError: 'by_decade'` / `'decade_grid'`.

- [ ] **Step 3: Add both keys**

In `compute_keyword`: add `decade=` to the `d = df.assign(...)` call using `mc.decade` over the same
`year` series `lv` uses; add `dec=` to the `lv = df.assign(...)` call; then

```python
by_decade = dist("decade")
...
"by_decade": by_decade,
"decade_grid": cross_tab("dec", "decade"),
```

`year` is currently computed after `d`, so move its definition above `d` — it is a pure expression
with no dependency on `d`.

- [ ] **Step 4: Verify** — `python3 -m pytest integration-tests/python_modeling/test_analysis_dashboard.py -q`. Expected: 32 passed.

- [ ] **Step 5: Commit** — `git commit -m "feat(dashboard): decade as a standalone dimension"`

---

### Task 3: Export and render

- [ ] **Step 1** — `export_dashboard_json.py`: add `"by_decade": _records(kw["by_decade"])` and `"decade_grid": _records(kw["decade_grid"])`, neither truncated (5 rows and ≤90 rows).

- [ ] **Step 2** — `keyword-report.jsx`: pool three grids in the single `heatDomain` call; add a subtitle, the `by_decade` DataTable, and `<RelevanceHeatmap rows={data.decade_grid} crossKey="decade" crossLabel="decade" domain={domain} />` (no `markDiagonal` — there is no diagonal to force).

- [ ] **Step 3** — `frontend/README.md`: add the third row to the heatmap table and note that the decade grid has no forced cells because the axis is independent.

- [ ] **Step 4: Verify** — route tests pass (the domain guard still sees one `heatDomain` call containing both named grids); build succeeds. The snapshot has no `decade_grid` yet, so the decade heatmap shows its re-export note — the fallback working.

- [ ] **Step 5: Commit**

---

### Task 4: Regenerate and compare

- [ ] **Step 1: Capture the before state**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
python3 - <<'PY' > /tmp/l3_before.txt
import json
d = json.load(open('data/dashboard.json'))['keyword']
l3 = sorted({r['l3'] for r in d['tops']['l3']})
print(f"l3 distinct values: {len(l3)}")
print(f"l3 sample: {l3[:6]}")
print(f"all end in unknown: {all(v.endswith('unknown') for v in l3)}")
print(f"by_decade present: {'by_decade' in d}")
print(f"decade_grid present: {'decade_grid' in d}")
PY
cat /tmp/l3_before.txt
```

- [ ] **Step 2: Confirm ports are clear, then run the simulation**

```bash
pgrep -f 'next dev|next-server' && echo "STOP: a dev server holds .next" || echo "clear"
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
export JAVA_HOME="/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_HOME="$HOME/opt/spark-3.5.1-bin-hadoop3"
bash scripts/run-movie-category-sim.sh 2>&1 | tail -20
```

~25 minutes. `count=N/0` sitting still is the drain's floor, not a stall. If it fails, stop —
`git checkout` restores the snapshot.

- [ ] **Step 3: Compare**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/frontend
python3 - <<'PY'
import json, collections
d = json.load(open('data/dashboard.json'))['keyword']
l3 = sorted({r['l3'] for r in d['tops']['l3']})
print(f"l3 distinct: {len(l3)}   still all unknown: {all(v.endswith('unknown') for v in l3)}")
print(f"l3 sample:   {l3[:6]}")
decs = sorted({v.split('·')[1] for v in l3})
print(f"decades in l3: {decs}")
print()
print("by_decade:")
for r in d['by_decade']:
    print(f"   {r['decade']:>8}  impressions {r['movie_impressions']:>6}  ctr {r['ctr']:.4f}  "
          f"share {r['movie_share']:.3f}  divergence {r['divergence']:+.4f}")
g = d['decade_grid']
print(f"\ndecade_grid: {len(g)} cells over {len({r['decade'] for r in g})} decades "
      f"x {len({r['keyword'] for r in g})} keywords")
print(f"forced cells (decade == keyword): {sum(1 for r in g if r['decade'] == r['keyword'])}")
PY
diff <(cat /tmp/l3_before.txt) /dev/null | head -8
```

Report the before/after side by side. **If L3 still ends in `unknown`, stop** — the fix did not take
and the cause is upstream of the report.

- [ ] **Step 4: Commit the snapshot alone, then run every gate**

Expected: `606 passed, 2 skipped` (599 + 4 + 3).

## Self-Review

**Spec coverage.** Acceptance 1 → Task 1 step 1's year test; 2 → the real-decade test; 3 → the fallback test; 4 → the L1/L2 test; 5 → `test_decade_uses_the_shared_derivation`; 6 → `test_decade_grouping_is_independent_of_genre`, which checks both directions (different genres sharing a decade, one genre spanning decades) and the absence of forced cells; 7 → `test_by_decade_reports_the_same_metrics_as_by_keyword`, which compares column sets rather than listing them; 8 → the same independence test; 9 → Task 4; 10 → Task 4 step 4.

**Placeholders.** None for tests or the year fix. Task 2 step 3 and Task 3 describe edits in prose plus the exact added expressions, because they are single-line insertions into calls the diff already shows.

**Type consistency.** `by_decade` has a `decade` column from `dist("decade")`; `decade_grid` has `decade` and `keyword` from `cross_tab("dec", "decade")`; the component reads `crossKey="decade"`.

**Ordering risk.** `year` is defined after `d` today, and `d` now needs it. Moving the definition up is safe because it reads only `df`, but it must actually be moved rather than duplicated, or the two copies can drift.

**The riskiest step is the simulation**, as before: it is the only step `git checkout` cannot undo by itself, and a half-failure could leave a written-but-wrong snapshot. Step 3 says to stop rather than explain away an L3 that is still `unknown`.
