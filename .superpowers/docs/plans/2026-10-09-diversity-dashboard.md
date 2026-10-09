# Diversity Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The Diversity section can catch a recommender that narrows what people see: a long tail that is not ~80% by construction, catalog-level spread, per-slate distributions instead of a ten-row sample, and trends over the run.

**Architecture:** `compute_diversity` gains a distinct-item long-tail cutoff, seven catalog-level aggregate fields and three envelope keys (`distributions`, `genre_exposure`, `series` + `series_bucket_seconds`). Bucketing is shared with satisfaction through an extracted `_time_buckets`. The exporter publishes the aggregate row only. `DiversitySection` is rebuilt on existing chart components.

**Tech Stack:** Python 3.12 + pandas (pytest); Next 15 / React 19, no chart library.

**Spec:** `.superpowers/docs/specs/2026-10-09-diversity-dashboard-design.md`

## Global Constraints

- A missing measurement is `None`/`null`, never 0.
- An item without `item_id` counts as its own distinct item (key `#<position>` within the run's item list).
- `request_ts` is epoch seconds; series bucketing follows the whole-second rule already in `_satisfaction_series` (width `ceil((span + 1) / 24)` for integer stamps, else `span / 24`; at most 24 buckets).
- `compute_diversity` keeps returning `[aggregate, *slate_rows]`; only the exporter drops slate rows.
- No new npm dependency.
- Branch guard before every commit: `test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }`
- Python tests run from `recsys-pipeline/`: `python3 -m pytest -q`. Baseline on master: `621 passed, 2 skipped`.
- The code PR targets `master` directly (no stacking).

## Review Focus

1. **`_time_buckets` extraction drifting satisfaction output** — the satisfaction series tests must pass unchanged; Task 1 runs them before and after.
2. **Duplicate DataFrame index labels** — bucketing must select rows positionally (`to_numpy()` masks), never by `.loc` on labels. Task 1's implementation uses positional masks; Task 5 likewise.
3. **Slates with `user_id` absent** — `median_items_per_user` and `user_repeat_rate` are `None`, the other catalog fields still publish. Pinned in Task 3.
4. **The column-contract test** — `slate_id` must leave `DiversitySection`'s columns once slate rows stop being published, and every new column must be on the aggregate row. Task 7 runs `test_dashboard_columns_match_the_published_measurement_keys`.
5. **Histogram edges** — a value of exactly 1.0 lands in bin 0.9; float products like `0.7 * 10` must not drop a value into the bin below. Pinned in Task 4 by entropy 1.0 and distance 0.6667.

---

### Task 1: Extract `_time_buckets` from the satisfaction series

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/quality_measurements.py` (`_satisfaction_series`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`

**Interfaces:**
- Produces: `_time_buckets(stamps: pd.Series) -> tuple[pd.Series, list[float], float] | None` — `stamps` numeric with no nulls; returns (bucket index per stamp, bucket starts unrounded, width unrounded), or `None` when empty or zero span.

- [x] **Step 1: Write the failing test**

Add `_time_buckets` to the `from quality_measurements import (...)` block, then append:

```python
def test_time_buckets_use_whole_second_widths_and_need_a_span():
    index, starts, width = _time_buckets(pd.Series([0, 1, 65, 66]))

    assert width == 3.0 and len(starts) == 23
    assert list(index) == [0, 0, 21, 22]
    assert _time_buckets(pd.Series([7, 7])) is None
    assert _time_buckets(pd.Series([], dtype=float)) is None
```

- [x] **Step 2: Run to verify it fails**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py`
Expected: collection error `ImportError: cannot import name '_time_buckets'`.

- [x] **Step 3: Implement — extract, then call it from satisfaction**

Add above `_satisfaction_series`:

```python
def _time_buckets(stamps: pd.Series) -> tuple[pd.Series, list[float], float] | None:
    """Bucket index per stamp over the observed span, the bucket starts, and the width.

    Whole-second stamps (what the joiner publishes) get a whole-second width: a fractional one
    gives buckets 2 or 3 distinct seconds apiece, a sawtooth in every count from the width alone.
    The bucket count then falls to fit, at most SERIES_BUCKETS. None without a span.
    """
    if stamps.empty or stamps.max() <= stamps.min():
        return None
    start = float(stamps.min())
    span = float(stamps.max()) - start
    if (stamps % 1 == 0).all():
        width = float(math.ceil((span + 1) / SERIES_BUCKETS))
        buckets = math.ceil((span + 1) / width)
    else:
        width = span / SERIES_BUCKETS
        buckets = SERIES_BUCKETS
    # Float division can put the maximum at exactly `buckets`; it belongs to the last bucket.
    index = ((stamps - start) // width).clip(upper=buckets - 1).astype(int)
    return index, [start + i * width for i in range(buckets)], width
```

Replace the body of `_satisfaction_series` with:

```python
    """Per-bucket engagement over the observed impression_ts span (epoch seconds)."""
    if "impression_ts" not in samples:
        return [], None
    stamps = pd.to_numeric(samples["impression_ts"], errors="coerce")
    observed = stamps.notna().to_numpy()
    buckets = _time_buckets(stamps[observed])
    if buckets is None:
        return [], None
    index, starts, width = buckets
    timed, index = samples[observed], index.to_numpy()
    series = []
    for position, start in enumerate(starts):
        part = timed[index == position]
        ratings = _numeric_column(part, "rating")
        series.append({
            "bucket_start": round(start, 1),
            "impressions": len(part),
            "users": _distinct(part, "user_id"),
            "ctr": _mean(_numeric_column(part, "clicked")),
            "order_rate": _mean(_numeric_column(part, "ordered")),
            "mean_rating": _mean(ratings),
            "ratings": len(ratings),
        })
    return series, round(width, 1)
```

- [x] **Step 4: Run to verify — new test and every satisfaction test**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py`
Expected: all pass (37).

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/quality_measurements.py recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py
git commit -m "refactor(measurements): extract the whole-second time bucketing for reuse"
```

### Task 2: Long tail over distinct items

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/quality_measurements.py` (`compute_diversity`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`

**Interfaces:**
- Produces: `_item_key(item: Mapping, position: int) -> str`; `_distinct_item_cutoff(items: list[Mapping], percentile: float) -> float | None`.

- [x] **Step 1: Write the failing test**

```python
def test_diversity_long_tail_cutoff_counts_each_item_once():
    """An exposure-weighted quantile puts ~percentile of exposures below it by construction."""
    items = ([{"item_id": "low", "popularity": 10.0}] * 8
             + [{"item_id": "mid", "popularity": 50.0}, {"item_id": "high", "popularity": 100.0}])
    row = compute_diversity(pd.DataFrame([{"request_id": "r1", "items": items}]))["rows"][0]

    # Over exposures the cutoff would be 18.0 and the share 0.8; over distinct items it is 80.0.
    assert row["long_tail_popularity_cutoff"] == 80.0
    assert row["long_tail_exposure_share"] == 0.9
```

- [x] **Step 2: Run to verify it fails**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py -k counts_each_item_once`
Expected: FAIL, `18.0 == 80.0`.

- [x] **Step 3: Implement**

In `compute_diversity`, replace the `popularities = [...]` and `cutoff = ...` lines with:

```python
    all_items = [item for _, items in slate_inputs for item in items]
    cutoff = _distinct_item_cutoff(all_items, long_tail_percentile)
```

and delete the later `all_items = ...` line. Change `"popularity_coverage": _ratio(len(popularities), len(all_items)),` to:

```python
        "popularity_coverage": _ratio(
            sum(_numeric_item_value(item, "popularity") is not None for item in all_items), len(all_items)),
```

Add after `compute_diversity`:

```python
def _item_key(item: Mapping[str, object], position: int) -> str:
    """The item's id; an item without one counts as its own distinct item."""
    return _string_value(item.get("item_id")) or f"#{position}"


def _distinct_item_cutoff(items: list[Mapping[str, object]], percentile: float) -> float | None:
    """Popularity quantile over distinct served items, each counted once.

    Taken over exposures instead, the quantile puts ~percentile of exposures below it by
    construction, so the long-tail share could not move whatever the recommender did.
    """
    first_seen: dict[str, float] = {}
    for position, item in enumerate(items):
        popularity = _numeric_item_value(item, "popularity")
        if popularity is not None:
            first_seen.setdefault(_item_key(item, position), popularity)
    return float(pd.Series(list(first_seen.values())).quantile(percentile)) if first_seen else None
```

- [x] **Step 4: Run to verify**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py`
Expected: all pass, including `test_diversity_reports_entropy_jaccard_and_long_tail` (two id-less items → cutoff 81.0, share 0.5).

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/quality_measurements.py recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py
git commit -m "fix(diversity): take the long-tail cutoff over distinct items, not exposures"
```

### Task 3: Catalog-level spread, and the catalog size it needs

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/quality_measurements.py` (`compute_diversity`)
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py` (`load_slates`, `build_measurement_dashboard`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`, `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`

**Interfaces:**
- Consumes: `_item_key` (Task 2).
- Produces: `compute_diversity(slates, long_tail_percentile=0.80, catalog_size: int | None = None)`; aggregate keys `items_served, catalog_size, catalog_coverage, exposure_gini, top_decile_exposure_share, median_items_per_user, user_repeat_rate`; `slate_inputs` entries become `(slate_id, items, user_id, request_ts)`; test fixture `_catalog_slates()`; `load_slates(...).attrs["catalog_size"]`.

- [x] **Step 1: Write the failing tests**

In `test_quality_measurements.py`:

```python
_GENRES = {"a": ["drama"], "b": ["comedy"], "c": ["drama", "comedy"], "d": ["action"]}
_POPULARITY = {"a": 10.0, "b": 20.0, "c": 30.0, "d": 40.0}


def _catalog_slates():
    # Exposures a:6, b:2, c:1, d:1. u1 sees `a` twice; u3 sees nothing but `a`.
    def item(key):
        return {"item_id": key, "genres": _GENRES[key], "popularity": _POPULARITY[key]}
    return pd.DataFrame([
        {"request_id": "r1", "user_id": "u1", "request_ts": 10, "items": [item("a"), item("b"), item("c")]},
        {"request_id": "r2", "user_id": "u1", "request_ts": 11, "items": [item("a"), item("d")]},
        {"request_id": "r3", "user_id": "u2", "request_ts": 12, "items": [item("a"), item("b")]},
        {"request_id": "r4", "user_id": "u3", "request_ts": 13, "items": [item("a"), item("a"), item("a")]},
    ])


def test_diversity_measures_spread_across_the_catalog_and_each_user():
    row = compute_diversity(_catalog_slates(), catalog_size=8)["rows"][0]

    assert (row["items_served"], row["catalog_size"], row["catalog_coverage"]) == (4, 8, 0.5)
    # Sorted counts [1, 1, 2, 6]: Gini 16 / 40; the top decile is the single top item.
    assert (row["exposure_gini"], row["top_decile_exposure_share"]) == (0.4, 0.6)
    # Distinct per user 4, 2, 1 over exposures 5, 2, 3.
    assert (row["median_items_per_user"], row["user_repeat_rate"]) == (2.0, 0.3)


def test_diversity_spread_is_none_without_the_identities_it_needs():
    slates = _catalog_slates().drop(columns=["user_id"])
    row = compute_diversity(slates)["rows"][0]

    assert row["catalog_size"] is None and row["catalog_coverage"] is None
    assert row["median_items_per_user"] is None and row["user_repeat_rate"] is None
    assert row["items_served"] == 4  # item spread still measurable
```

In `test_analysis_dashboard.py`:

```python
def test_load_slates_records_the_catalog_size(tmp_path, monkeypatch):
    pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    import feature_derivations as genre_meta
    import ranking_eval_report

    path = tmp_path / "slates.json"
    path.write_text(json.dumps([{"request_id": "r1", "items": [{"item_id": "m1"}]}]))
    monkeypatch.setattr(genre_meta, "fetch_movie_meta", lambda host, port: [
        {"item_id": f"m{i}", "genres": ["Drama"]} for i in range(3)])
    monkeypatch.setattr(ranking_eval_report, "fetch_popularity", lambda host, port: {})

    assert dash.load_slates(str(path)).attrs["catalog_size"] == 3
```

- [x] **Step 2: Run to verify they fail**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py integration-tests/python_modeling/test_analysis_dashboard.py -k "spread or catalog_size"`
Expected: FAIL — `TypeError: ... unexpected keyword argument 'catalog_size'`, `KeyError: 'items_served'`, `KeyError: 'catalog_size'`.

- [x] **Step 3: Implement**

`compute_diversity`: change the signature to add `catalog_size: int | None = None`, and build the inputs with user and time in one pass:

```python
    slate_inputs = [
        (_slate_id(index, row), _items(row), _string_value(row.get("user_id")),
         _numeric_value(row.get("request_ts")))
        for index, row in slates.iterrows()
    ]
    slate_inputs = [entry for entry in slate_inputs if entry[1]]
```

Update the two unpackings that read `slate_inputs`: `all_items = [item for _, items, _, _ in slate_inputs for item in items]` and `for slate_id, items, _, _ in slate_inputs` in `slate_rows`. In `aggregate`, after `"long_tail_popularity_cutoff"`, add `**_catalog_spread(slate_inputs, catalog_size),`.

Add after `_distinct_item_cutoff`:

```python
def _catalog_spread(slate_inputs: list[tuple], catalog_size: int | None) -> dict[str, object]:
    """Spread across the catalog and across each user's history -- what per-slate averages miss.

    A recommender showing every user the same five genre-diverse items scores perfectly per
    slate; Gini, coverage and repeat rate are what move.
    """
    exposures: Counter[str] = Counter()
    seen_by_user: dict[str, list[str]] = {}
    for _, items, user, _ in slate_inputs:
        ids = [item_id for item in items if (item_id := _string_value(item.get("item_id")))]
        exposures.update(ids)
        if user:
            seen_by_user.setdefault(user, []).extend(ids)
    counts = sorted(exposures.values())
    total, n = sum(counts), len(counts)
    distinct = [len(set(ids)) for ids in seen_by_user.values()]
    seen = sum(len(ids) for ids in seen_by_user.values())
    return {
        "items_served": n or None,
        "catalog_size": catalog_size,
        "catalog_coverage": _ratio(n, catalog_size) if n and catalog_size else None,
        "exposure_gini": (_round(sum((2 * rank - n - 1) * c for rank, c in enumerate(counts, 1)) / (n * total))
                          if n else None),
        "top_decile_exposure_share": _round(sum(counts[-max(1, n // 10):]) / total) if n else None,
        "median_items_per_user": _median(distinct),
        "user_repeat_rate": _round(1 - sum(distinct) / seen) if seen else None,
    }
```

`analysis_dashboard_report.py` — in `load_slates`, before `return slates` at the end:

```python
    if genres:
        # The full catalog, so the diversity section can say what share of it was served.
        slates.attrs["catalog_size"] = len(genres)
```

and in `build_measurement_dashboard`:

```python
        "diversity": (quality.compute_diversity(slates, float(cfg["long_tail_percentile"]),
                                                slates.attrs.get("catalog_size"))
                      if slates is not None else no_slates),
```

- [x] **Step 4: Run the suite**

Run: `python3 -m pytest -q`
Expected: `626 passed, 2 skipped`.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/ recsys-pipeline/integration-tests/python_modeling/
git commit -m "feat(diversity): catalog coverage, exposure Gini, top-decile share and user repeat rate"
```

### Task 4: Per-slate distributions and genre exposure vs served share

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/quality_measurements.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`

**Interfaces:**
- Consumes: `_catalog_slates()` (Task 3), `_item_key` (Task 2).
- Produces: envelope keys `distributions: {"normalized_genre_entropy": [...], "intra_list_genre_distance": [...]}` (10 × `{"bin_start", "count"}`), `genre_exposure: [{"genre", "exposure_share", "served_share"}]`; helpers `_unit_histogram(values) -> list[dict]`, `_genre_exposure(items) -> list[dict]`.

- [x] **Step 1: Write the failing tests**

```python
def test_diversity_publishes_per_slate_distributions():
    hist = compute_diversity(_catalog_slates())["distributions"]

    # Entropy: r1, r2, r3 are 1.0 (last bin); r4 is all-drama, 0.0.
    assert [b["count"] for b in hist["normalized_genre_entropy"]] == [1, 0, 0, 0, 0, 0, 0, 0, 0, 3]
    # Distance: r4 0.0, r1 0.6667, r2 and r3 1.0.
    assert [b["count"] for b in hist["intra_list_genre_distance"]] == [1, 0, 0, 0, 0, 0, 1, 0, 0, 2]
    assert hist["intra_list_genre_distance"][6]["bin_start"] == 0.6


def test_diversity_compares_genre_exposure_with_what_is_served():
    shares = compute_diversity(_catalog_slates())["genre_exposure"]

    # Exposures mention drama 7, comedy 3, action 1; distinct items drama 2, comedy 2, action 1.
    assert shares == [
        {"genre": "drama", "exposure_share": 0.6364, "served_share": 0.4},
        {"genre": "comedy", "exposure_share": 0.2727, "served_share": 0.4},
        {"genre": "action", "exposure_share": 0.0909, "served_share": 0.2},
    ]
```

- [x] **Step 2: Run to verify they fail**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py -k "distributions or genre_exposure"`
Expected: FAIL with `KeyError: 'distributions'` and `KeyError: 'genre_exposure'`.

- [x] **Step 3: Implement**

Replace the final `return available(...)` of `compute_diversity` with:

```python
    result = available(
        "Catalog diversity across slates",
        [aggregate, *slate_rows],
        len(slate_inputs),
        genre_coverage or 0.0,
    )
    result["distributions"] = {
        key: _unit_histogram([entry[key] for entry in slate_rows])
        for key in ("normalized_genre_entropy", "intra_list_genre_distance")
    }
    result["genre_exposure"] = _genre_exposure(all_items)
    return result
```

Add after `_catalog_spread`:

```python
def _unit_histogram(values: list[float | None]) -> list[dict[str, object]]:
    """Ten bins over [0, 1]; exactly 1.0 lands in the last; None is skipped."""
    counts = [0] * 10
    for value in values:
        if value is not None:
            counts[min(int(round(value * 10, 9)), 9)] += 1
    return [{"bin_start": index / 10, "count": count} for index, count in enumerate(counts)]


def _genre_exposure(items: list[Mapping[str, object]]) -> list[dict[str, object]]:
    """Each genre's share of exposures beside its share of the distinct items served."""
    exposed = Counter(genre for item in items for genre in _genres(item))
    served_items: dict[str, list[str]] = {}
    for position, item in enumerate(items):
        served_items.setdefault(_item_key(item, position), _genres(item))
    served = Counter(genre for genres in served_items.values() for genre in genres)
    exposed_total, served_total = sum(exposed.values()), sum(served.values())
    rows = [{"genre": genre,
             "exposure_share": _ratio(count, exposed_total),
             "served_share": _ratio(served[genre], served_total)}
            for genre, count in exposed.items()]
    return sorted(rows, key=lambda row: (-row["exposure_share"], row["genre"]))
```

`round(value * 10, 9)` absorbs float error such as `0.7 * 10 == 7.000000000000001` and `0.3 * 10 == 3.0000000000000004` without moving a true boundary.

- [x] **Step 4: Run to verify**

Run: `python3 -m pytest -q`
Expected: `628 passed, 2 skipped`.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/quality_measurements.py recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py
git commit -m "feat(diversity): per-slate histograms and genre exposure vs served share"
```

### Task 5: Diversity over time

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/quality_measurements.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`

**Interfaces:**
- Consumes: `_time_buckets` (Task 1), `slate_inputs` 4-tuples (Task 3), `_catalog_slates()`.
- Produces: envelope keys `series: [{"bucket_start", "slates", "normalized_genre_entropy", "intra_list_genre_distance", "long_tail_exposure_share", "items_served"}]`, `series_bucket_seconds: float | None`; helper `_diversity_series(slate_inputs, slate_rows)`.

- [x] **Step 1: Write the failing tests**

```python
def test_diversity_series_buckets_slates_by_request_time():
    result = compute_diversity(_catalog_slates())
    series = result["series"]

    # request_ts 10..13 -> four 1 s buckets, one slate each.
    assert result["series_bucket_seconds"] == 1.0 and [b["slates"] for b in series] == [1, 1, 1, 1]
    # Cutoff over distinct items is 34.0, so every item in r1 is in the tail.
    assert series[0] == {"bucket_start": 10.0, "slates": 1, "normalized_genre_entropy": 1.0,
                         "intra_list_genre_distance": 0.6667, "long_tail_exposure_share": 1.0,
                         "items_served": 3}
    assert series[3]["normalized_genre_entropy"] == 0.0 and series[3]["items_served"] == 1


def test_diversity_series_is_empty_without_request_times():
    result = compute_diversity(_catalog_slates().drop(columns=["request_ts"]))

    assert result["series"] == [] and result["series_bucket_seconds"] is None
```

- [x] **Step 2: Run to verify they fail**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py -k diversity_series`
Expected: FAIL with `KeyError: 'series'`.

- [x] **Step 3: Implement**

In `compute_diversity`, before `return result`:

```python
    result["series"], result["series_bucket_seconds"] = _diversity_series(slate_inputs, slate_rows)
```

Add after `_genre_exposure`:

```python
def _diversity_series(slate_inputs: list[tuple], slate_rows: list[dict]) -> tuple[list[dict[str, object]], float | None]:
    """Per-bucket slate diversity over the observed request_ts span (epoch seconds).

    The long-tail share is each slate's share against the run-wide cutoff, so buckets compare.
    """
    stamps = pd.Series([request_ts for *_, request_ts in slate_inputs], dtype=float)
    observed = stamps.notna().to_numpy()
    buckets = _time_buckets(stamps[observed])
    if buckets is None:
        return [], None
    index, starts, width = buckets
    timed = [(entry, row) for entry, row, keep in zip(slate_inputs, slate_rows, observed) if keep]
    members: list[list[tuple]] = [[] for _ in starts]
    for bucket, pair in zip(index.to_numpy(), timed):
        members[bucket].append(pair)
    series = []
    for start, pairs in zip(starts, members):
        rows = [row for _, row in pairs]
        series.append({
            "bucket_start": round(start, 1),
            "slates": len(pairs),
            "normalized_genre_entropy": _mean(row["normalized_genre_entropy"] for row in rows),
            "intra_list_genre_distance": _mean(row["intra_list_genre_distance"] for row in rows),
            "long_tail_exposure_share": _mean(row["long_tail_exposure_share"] for row in rows),
            "items_served": len({item_id for (_, items, _, _), _ in pairs for item in items
                                 if (item_id := _string_value(item.get("item_id")))}),
        })
    return series, round(width, 1)
```

- [x] **Step 4: Run to verify**

Run: `python3 -m pytest -q`
Expected: `630 passed, 2 skipped`.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/quality_measurements.py recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py
git commit -m "feat(diversity): entropy, distance, long tail and items served over the run"
```

### Task 6: The exporter publishes the aggregate only

**Files:**
- Modify: `recsys-pipeline/frontend/export_dashboard_json.py:57-71,81`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py:112-123`

**Interfaces:**
- Produces: `_aggregate_diversity_row(diversity: dict) -> dict`; removes `SLATE_ROW_LIMIT` and `_bounded_slate_rows`.

- [x] **Step 1: Replace the bounded-rows test with the failing one**

Replace `test_export_bounds_diversity_slate_rows_and_says_so` with:

```python
def test_export_publishes_only_the_diversity_aggregate():
    import export_dashboard_json as exporter

    rows = [{"scope": "aggregate"}] + [{"scope": "slate", "slate_id": f"r{i}"} for i in range(25)]
    published = exporter._aggregate_diversity_row({"status": "available", "rows": rows, "warnings": []})

    # The distributions and series cover every slate; a sample of slate rows adds nothing.
    assert published["rows"] == [{"scope": "aggregate"}] and published["warnings"] == []
    missing = {"status": "unavailable", "rows": [], "warnings": ["missing slate experiences"]}
    assert exporter._aggregate_diversity_row(missing) == missing
```

- [x] **Step 2: Run to verify it fails**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_analysis_dashboard.py -k diversity_aggregate`
Expected: FAIL with `AttributeError: module 'export_dashboard_json' has no attribute '_aggregate_diversity_row'`.

- [x] **Step 3: Implement**

Replace `SLATE_ROW_LIMIT = 10` and the whole `_bounded_slate_rows` function with:

```python
def _aggregate_diversity_row(diversity: dict) -> dict:
    """Publish the diversity aggregate only.

    The distributions and series cover every slate, so a sample of per-slate rows would put
    request ids in the committed snapshot and tell a reader nothing they can use.
    """
    if diversity["status"] != "available":
        return diversity
    return {**diversity, "rows": diversity["rows"][:1]}
```

and in `build`, `measurements["diversity"] = _aggregate_diversity_row(measurements["diversity"])`.

- [x] **Step 4: Run the suite**

Run: `python3 -m pytest -q`
Expected: `630 passed, 2 skipped` (one test replaced, none added).

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/frontend/export_dashboard_json.py recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py
git commit -m "feat(dashboard): publish the diversity aggregate, not a sample of slates"
```

### Task 7: `DiversitySection`

**Files:**
- Modify: `recsys-pipeline/frontend/components/measurements.jsx` (`DiversitySection`)

**Interfaces:**
- Consumes: `GroupedBarChart`, `LineChart`, `BarChart` (`./ui`); `count`, `duration`, `num`, `share` (`./format`) — all already imported; envelope keys from Tasks 3–5; `MeasurementSection`'s `chart(rows, data)`.

- [x] **Step 1: Replace `DiversitySection` entirely**

```jsx
export function DiversitySection({ data }) {
  return (
    <MeasurementSection
      title="Diversity"
      data={data}
      columns={[
        "scope", "items_served", "catalog_size", "catalog_coverage", "exposure_gini",
        "top_decile_exposure_share", "median_items_per_user", "user_repeat_rate",
        "unique_genres_at_k", "normalized_genre_entropy", "intra_list_genre_distance",
        "long_tail_exposure_share", "long_tail_popularity_cutoff", "genre_coverage",
        "popularity_coverage",
      ]}
      kpis={(rows) => {
        const row = rows.find((r) => r.scope === "aggregate") || rows[0] || {};
        return [
          { label: "genre entropy", value: num(row.normalized_genre_entropy, 3) },
          { label: "intra-list distance", value: num(row.intra_list_genre_distance, 3) },
          { label: "long-tail share", value: share(row.long_tail_exposure_share) },
          { label: "catalog coverage", value: share(row.catalog_coverage),
            detail: `${count(row.items_served)} of ${count(row.catalog_size)} items` },
          { label: "exposure Gini", value: num(row.exposure_gini, 3) },
          { label: "user repeat rate", value: share(row.user_repeat_rate) },
        ];
      }}
      description="Genre spread within a slate, spread across the catalog and each user's history, and long-tail exposure."
      chart={(rows, data) => {
        const histograms = data.distributions ?? {};
        const binLabel = (b) => `${b.bin_start.toFixed(1)}–${(b.bin_start + 0.1).toFixed(1)}`;
        const genres = data.genre_exposure ?? [];
        const series = data.series ?? [];
        const width = data.series_bucket_seconds;
        const labels = series.map((b) => `+${duration(b.bucket_start - series[0].bucket_start)}`);
        const caption = `${series.length} × ${duration(width)} buckets over ${duration(width * series.length)}`;
        return (
          <ChartGrid>
            {[["normalized_genre_entropy", "Per-slate genre entropy"],
              ["intra_list_genre_distance", "Per-slate intra-list distance"]].map(([key, title]) => {
              const bins = histograms[key] ?? [];
              return bins.length ? (
                <BarChart key={key} title={title} valueFormatter={count}
                  labels={bins.map(binLabel)} values={bins.map((b) => b.count)} />
              ) : null;
            })}
            {genres.length ? (
              <GroupedBarChart title="Genre share: exposures vs served items" percentage
                labels={genres.map((g) => g.genre)}
                series={[{ name: "exposures", values: genres.map((g) => g.exposure_share) },
                         { name: "served items", values: genres.map((g) => g.served_share) }]} />
            ) : null}
            {series.length ? (
              <>
                <LineChart title="Genre spread over time" labels={labels} caption={caption}
                  valueFormatter={(v) => num(v, 3)}
                  series={[{ name: "entropy", values: series.map((b) => b.normalized_genre_entropy) },
                           { name: "intra-list distance",
                             values: series.map((b) => b.intra_list_genre_distance) }]} />
                <LineChart title="Long-tail share over time" percentage labels={labels} caption={caption}
                  series={[{ name: "long-tail share",
                             values: series.map((b) => b.long_tail_exposure_share) }]} />
              </>
            ) : <p className="na">Time series unavailable (no request_ts span).</p>}
          </ChartGrid>
        );
      }}
    >
      <p className="fine-print">
        The long-tail cutoff is the configured popularity percentile (80th by default) over distinct
        served items. Taken over exposures it would put that share of exposures below it by
        construction. Catalog coverage, Gini and repeat rate measure spread across the catalog and
        across each user&apos;s history, which per-slate averages cannot see. The sim serves
        near-uniform slates, so every figure here sits near its ideal; the section exists to catch a
        recommender that narrows what people see.
      </p>
    </MeasurementSection>
  );
}
```

- [x] **Step 2: Run the contract tests and the suite**

Run: `python3 -m pytest -q integration-tests/python_modeling/test_dashboard_measurement_contract.py && python3 -m pytest -q`
Expected: contract file passes (`slate_id` is gone from the requested columns); suite `630 passed, 2 skipped`.

- [x] **Step 3: Build against the current snapshot**

The committed snapshot predates the new keys, so this exercises every absent-data branch.

```bash
cd recsys-pipeline/frontend && npm run build 2>&1 | grep -E 'Compiled|Failed|rror' | head -3
f=.next/server/app/serving/diversity.html
echo -n "KPI cards (want 6): "; grep -o 'class="metric-card"' "$f" | wc -l | tr -d ' '
echo -n "series fallback (want 1 rendered): "; grep -o '<p class="na">Time series unavailable' "$f" | wc -l | tr -d ' '
```

- [x] **Step 4: Commit**

```bash
git add recsys-pipeline/frontend/components/measurements.jsx
git commit -m "feat(dashboard): diversity shows catalog spread, distributions and trends"
```

### Task 8: Re-export the snapshot and record the evidence

**Files:**
- Modify: `recsys-pipeline/frontend/data/dashboard.json`, this plan's Verification section

Exporter only, against the surviving run (`/tmp/spark-recsys/movie-category-sim`) with Redis still up — no re-simulation. If the Parquet or Redis is gone, stop and record that instead of re-simulating unasked.

- [x] **Step 1: Re-export and diff against the committed snapshot**

```bash
cd recsys-pipeline
cp frontend/data/dashboard.json /tmp/claude-diversity-before.json
R=/tmp/spark-recsys/movie-category-sim
REDIS_HOST=localhost REDIS_PORT=6379 python3 frontend/export_dashboard_json.py \
  --input $R/training-samples --output frontend/data/dashboard.json \
  --experiences $R/slates $( [ -s $R/live-metrics.json ] && echo --live-metrics $R/live-metrics.json )
(cd frontend && npm run validate:data)
python3 -c "
import json; a=json.load(open('/tmp/claude-diversity-before.json')); b=json.load(open('frontend/data/dashboard.json'))
print('moved:', [k for k in a if a[k]!=b.get(k)])
d=b['diversity']; r=d['rows'][0]; print({k:r[k] for k in ('long_tail_exposure_share','long_tail_popularity_cutoff','items_served','catalog_coverage','exposure_gini','top_decile_exposure_share','median_items_per_user','user_repeat_rate')})
print('rows', len(d['rows']), 'warnings', d['warnings'], 'buckets', len(d['series']), d['series_bucket_seconds'])"
```

Expected: moved = `['diversity', 'freshness']` (freshness only in its two content-age fields); `long_tail_exposure_share` ≈ 0.76; `items_served` 400; `catalog_coverage` 1.0; Gini ≈ 0.038; one row; no warnings.

- [x] **Step 2: Build and inspect with real data**

```bash
cd frontend && npm run build 2>&1 | grep -E 'Compiled|Failed|rror' | head -3
f=.next/server/app/serving/diversity.html
for t in 'Per-slate genre entropy' 'Per-slate intra-list distance' 'Genre share: exposures vs served items'; do echo -n "$t: "; grep -c "$t" "$f"; done
echo -n "line charts (want 2): "; grep -o 'class="chart-card line-chart"' "$f" | wc -l | tr -d ' '
echo -n "fallback (want 0): "; grep -c 'Time series unavailable' "$f" || true
```

- [x] **Step 3: Commit the snapshot alone, then the evidence**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/frontend/data/dashboard.json
git commit -m "chore(dashboard): re-export with the new diversity measures"
```

Fill in Verification below, tick the boxes, commit as `docs: record the diversity plan's verification evidence`.

## Verification

- **Suite:** `python3 -m pytest -q` → `630 passed, 2 skipped` (621 + 9 new items; the bounded-rows
  test was replaced, not added).
- **Fallback build (Task 7, snapshot without the new keys):** `Compiled successfully`; 6 KPI cards;
  one rendered "Time series unavailable" line.
- **Re-export (Task 8):** moved `diversity` and `freshness` only; freshness only in
  `mean_content_age_days` / `median_content_age_days`. Diversity aggregate: long tail 0.7605 (cutoff
  44.0; was the circular 0.796), items served 400 / catalog 400 (coverage 1.0), exposure Gini
  0.0383, top decile 0.1124, median items per user 261, user repeat rate 0.3789; one row, no
  warnings; series 23 × 3 s. Per-slate means bit-identical to before. Entropy histogram
  `[0,0,0,0,0,0,0,1,64,16661]`; distance `[0,0,1,0,0,3,17,266,3368,13071]`.
- **Real-data build (Task 8):** both histograms, the genre chart and 2 line charts present; 0
  fallbacks; screenshot inspected; no horizontal overflow at 390 px.

### Observed after the fact

`user_repeat_rate` 0.379 is what uniform-random serving produces: ~418 exposures per user drawn
from 400 items give an expected `400 · (1 − (399/400)^418) ≈ 259` distinct (measured median 261),
so a repeat rate of ≈ 0.38 with no recommender bias at all. The figure needs that baseline beside it
to be read.
