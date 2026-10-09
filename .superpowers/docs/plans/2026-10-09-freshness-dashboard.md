# Freshness Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Freshness measures content age at exposure (not export), states the uncertainty of the fresh-vs-established CTR gap, compares fresh exposure with fresh supply, and breaks exposure down by age band.

**Architecture:** `_timestamp_observations` / `_boolean_freshness_observations` return one observation frame (`fresh, age, clicked, reward, item_id, at_exposure`); `_freshness_result` derives the row, a clustered CTR gap, supply fields and `age_bands` from it. `FreshnessSection` adds band charts and hides the empty reward chart.

**Tech Stack:** Python 3.12 + pandas/numpy (pytest); Next 15 / React 19.

**Spec:** `.superpowers/docs/specs/2026-10-09-freshness-dashboard-design.md`

## Global Constraints

- `impression_ts` is epoch seconds; a non-finite one falls back to `now`.
- `fresh = age ≤ window_days`; bands `0-7 d` [0, 7], `8-30 d` (7, 30], `31-90 d` (30, 90], `91-365 d` (90, 365], `> 1 y` (365, ∞).
- Clustered SE: `Σ_i (s_i − c·n_i)² / n² · k/(k−1)` over `k ≥ 2` movies, `c` the cohort mean (gap) or overall `p0` (bands).
- Missing is `None`, never 0. `_boolean_value` keeps its per-value contract.
- Branch guard before every commit. Baseline `644 passed, 2 skipped`. Code PR targets `master`.

## Review Focus

1. **Existing freshness tests** carry no `impression_ts` — they must pass unchanged through the export-time fallback. Task 1 runs the file.
2. **Nullable boolean columns** (`pd.NA`) through `Series.map(_boolean_value)` — pinned by the existing `test_freshness_accepts_homogeneous_and_nullable_pandas_boolean_columns`.
3. **Mixed tz / ISO strings vs datetime `published_at`** — tests pass ISO strings, the exporter passes tz-aware datetimes from `_with_published_timestamps`; both go through `pd.to_datetime(..., utc=True)`.
4. **A movie crossing a band edge during the window** counts in both bands' `items`, so `item_share` can sum slightly above 1. Documented in the spec; not "fixed".
5. **The column-contract test** — every new summary column must be on the offline row; the band table's `columns` must come after the summary `columns` in the JSX.

---

### Task 1: Age at exposure, one observation frame

**Files:** Modify `recsys-pipeline/services/python-modeling/quality_measurements.py` (`compute_freshness`, `_freshness_result`, `_timestamp_observations`, `_boolean_freshness_observations`); Test `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`.

**Interfaces:** Produces `_observation_frame(samples, fresh, age, at_exposure) -> pd.DataFrame`; both observation functions return `pd.DataFrame | None`; row field `age_at_exposure_coverage`; fixture `_fresh_samples(ts=True)`.

- [ ] **Step 1: Failing tests**

```python
_T0 = int(pd.Timestamp("2026-07-30", tz="UTC").timestamp())


def _fresh_samples(ts=True):
    # Ages at exposure: m1 7 d (fresh, 0-7 d), m2 30 d (fresh, 8-30 d), m3 31 d, m4 575 d.
    rows = []
    for item, published, clicks in (("m1", "2026-07-23", (1, 1)), ("m2", "2026-06-30", (0, 1)),
                                    ("m3", "2026-06-29", (0, 0)), ("m4", "2025-01-01", (1, 0))):
        for clicked in clicks:
            rows.append({"item_id": item, "published_at": f"{published}T00:00:00Z", "clicked": clicked,
                         **({"impression_ts": _T0} if ts else {})})
    return pd.DataFrame(rows)


def test_freshness_ages_at_exposure_not_export():
    """Ageing against export time made a run exported 30 days later look entirely stale."""
    first = compute_freshness(_fresh_samples(), datetime(2026, 8, 1, tzinfo=timezone.utc))
    later = compute_freshness(_fresh_samples(), datetime(2026, 9, 1, tzinfo=timezone.utc))

    assert first["rows"] == later["rows"]
    row = first["rows"][0]
    assert (row["fresh_share"], row["median_content_age_days"], row["age_at_exposure_coverage"]) == (0.5, 30.5, 1.0)


def test_freshness_falls_back_to_export_time_without_impression_ts():
    now = datetime(2026, 7, 30, tzinfo=timezone.utc)
    assert compute_freshness(_fresh_samples(ts=False), now)["rows"][0]["age_at_exposure_coverage"] == 0.0
    mixed = _fresh_samples().astype({"impression_ts": float})
    mixed.loc[0, "impression_ts"] = float("nan")
    assert compute_freshness(mixed, now)["rows"][0]["age_at_exposure_coverage"] == 0.875
```

- [ ] **Step 2: Run — expect FAIL** `KeyError: 'age_at_exposure_coverage'` (and a differing row pair).

`python3 -m pytest -q integration-tests/python_modeling/test_quality_measurements.py -k "at_exposure or export_time"`

- [ ] **Step 3: Implement**

`compute_freshness` body after the guards:

```python
    timestamped = _timestamp_observations(samples, now, window_days)
    if timestamped is not None and len(timestamped):
        return _freshness_result(timestamped, len(samples), "published_at")

    boolean_rows = _boolean_freshness_observations(samples)
    if boolean_rows is not None and len(boolean_rows):
        return _freshness_result(boolean_rows, len(samples), "boolean_new_release")
    return unavailable("missing published_at and new_release freshness signals")
```

Replace `_timestamp_observations` and `_boolean_freshness_observations` with:

```python
def _observation_frame(samples: pd.DataFrame, fresh, age, at_exposure) -> pd.DataFrame:
    """One freshness observation per sample: cohort, age, outcomes and the movie it was."""
    def column(name):
        if name not in samples:
            return [None] * len(samples)
        return samples[name].map(_numeric_value).to_numpy(dtype=object)
    return pd.DataFrame({
        "fresh": np.asarray(fresh, dtype=object),
        "age": np.asarray(age, dtype=float),
        "clicked": column("clicked"),
        "reward": column("reward"),
        "item_id": (samples["item_id"].map(_string_value).to_numpy(dtype=object)
                    if "item_id" in samples else [None] * len(samples)),
        "at_exposure": np.asarray(at_exposure, dtype=bool),
    })


def _timestamp_observations(samples: pd.DataFrame, now: datetime, window_days: int) -> pd.DataFrame | None:
    """Samples with a parseable published_at, aged at exposure wherever impression_ts exists.

    Ageing against `now` made freshness depend on the export date: the same run exported 30 days
    later had every item "age out" though it was fresh when shown. `now` is only the fallback.
    """
    if "published_at" not in samples:
        return None
    now_timestamp = pd.Timestamp(now)
    now_timestamp = (now_timestamp.tz_localize("UTC") if now_timestamp.tzinfo is None
                     else now_timestamp.tz_convert("UTC"))
    published = pd.to_datetime(samples["published_at"], utc=True, errors="coerce")
    stamps = (pd.to_numeric(samples["impression_ts"], errors="coerce") if "impression_ts" in samples
              else pd.Series(np.nan, index=samples.index))
    exposed = pd.to_datetime(stamps.where(np.isfinite(stamps)), unit="s", utc=True)
    age = ((exposed.fillna(now_timestamp) - published).dt.total_seconds() / 86_400).clip(lower=0.0)
    frame = _observation_frame(samples, fresh=(age <= window_days).to_numpy(), age=age.to_numpy(),
                               at_exposure=exposed.notna().to_numpy())
    return frame[published.notna().to_numpy()].reset_index(drop=True)


def _boolean_freshness_observations(samples: pd.DataFrame) -> pd.DataFrame | None:
    if "new_release" not in samples:
        return None
    fresh = samples["new_release"].map(_boolean_value)
    frame = _observation_frame(samples, fresh=fresh.to_numpy(dtype=object), age=np.full(len(samples), np.nan),
                               at_exposure=np.zeros(len(samples), dtype=bool))
    frame = frame[fresh.notna().to_numpy()].reset_index(drop=True)
    frame["fresh"] = frame["fresh"].astype(bool)
    return frame
```

Replace `_freshness_result` with:

```python
def _freshness_result(obs: pd.DataFrame, total: int, source: str) -> dict[str, object]:
    is_fresh = obs["fresh"].to_numpy(dtype=bool)
    fresh, established = obs[is_fresh], obs[~is_fresh]
    ages = [float(age) for age in obs["age"] if pd.notna(age)]

    def observed(frame, name):
        return [float(value) for value in frame[name] if value is not None and pd.notna(value)]

    row = {
        "freshness_source": source,
        "fresh_share": _ratio(len(fresh), len(obs)),
        "freshness_coverage": _ratio(len(obs), total),
        "age_at_exposure_coverage": (_ratio(int(obs["at_exposure"].sum()), len(obs))
                                     if source == "published_at" else None),
        "mean_content_age_days": _mean(ages),
        "median_content_age_days": _median(ages),
        "fresh_ctr": _mean(observed(fresh, "clicked")),
        "fresh_ctr_coverage": _ratio(len(observed(fresh, "clicked")), len(fresh)),
        "established_ctr": _mean(observed(established, "clicked")),
        "established_ctr_coverage": _ratio(len(observed(established, "clicked")), len(established)),
        "fresh_mean_reward": _mean(observed(fresh, "reward")),
        "fresh_reward_coverage": _ratio(len(observed(fresh, "reward")), len(fresh)),
        "established_mean_reward": _mean(observed(established, "reward")),
        "established_reward_coverage": _ratio(len(observed(established, "reward")), len(established)),
    }
    return available("Fresh-item exposure", [row], total, _ratio(len(obs), total) or 0.0)
```

- [ ] **Step 4: Run the file, then the suite** — expect all pass; suite `646 passed, 2 skipped`.
- [ ] **Step 5: Commit** `fix(freshness): age content at exposure, not at export`

### Task 2: The CTR gap with an item-clustered SE

**Files:** same module and test file. **Interfaces:** Consumes Task 1's frame; produces `_clustered_mean(frame, column, center=None) -> tuple[float, float] | None`, `_fresh_ctr_gap(fresh, established) -> dict`; row fields `fresh_ctr_diff`, `fresh_ctr_diff_se`, `fresh_ctr_diff_z`.

- [ ] **Step 1: Failing test**

```python
def test_freshness_ctr_gap_is_item_clustered():
    row = compute_freshness(_fresh_samples(), datetime(2026, 8, 1, tzinfo=timezone.utc))["rows"][0]

    # Fresh 3/4 over m1, m2; established 1/4 over m3, m4. Each cohort's clustered SE is
    # sqrt((0.5**2 + 0.5**2) / 4**2 * 2) = 0.25, so the gap's SE is 0.3536 and z = 0.5 / 0.3536.
    assert (row["fresh_ctr_diff"], row["fresh_ctr_diff_se"], row["fresh_ctr_diff_z"]) == (0.5, 0.3536, 1.41)
    no_ids = compute_freshness(_fresh_samples().drop(columns=["item_id"]), datetime(2026, 8, 1, tzinfo=timezone.utc))
    assert no_ids["rows"][0]["fresh_ctr_diff_z"] is None
```

- [ ] **Step 2: Run — expect FAIL** `KeyError: 'fresh_ctr_diff'`.
- [ ] **Step 3: Implement** — add after `_freshness_result`:

```python
def _clustered_mean(frame: pd.DataFrame, column: str, center: float | None = None) -> tuple[float, float] | None:
    """Mean of a column and its SE with movies as clusters; None without two movies.

    A cohort is a few dozen movies seen many times, so impressions are not independent. Residuals
    are about `center` when given (a test against a reference rate), else about the mean.
    """
    rows = frame[[column, "item_id"]].dropna()
    if rows["item_id"].nunique() < 2:
        return None
    values = rows[column].astype(float)
    n, mean = len(values), float(values.mean())
    per = values.groupby(rows["item_id"].to_numpy()).agg(["sum", "size"])
    pivot = mean if center is None else center
    k = len(per)
    resid2 = float(((per["sum"] - pivot * per["size"]) ** 2).sum())
    return mean, math.sqrt(resid2 / n ** 2 * k / (k - 1))


def _fresh_ctr_gap(fresh: pd.DataFrame, established: pd.DataFrame) -> dict[str, object]:
    """Fresh minus established CTR, with the uncertainty that says whether it is a difference."""
    a, b = _clustered_mean(fresh, "clicked"), _clustered_mean(established, "clicked")
    se = math.hypot(a[1], b[1]) if a and b else 0.0
    if not se:
        return {"fresh_ctr_diff": None, "fresh_ctr_diff_se": None, "fresh_ctr_diff_z": None}
    diff = a[0] - b[0]
    return {"fresh_ctr_diff": _round(diff), "fresh_ctr_diff_se": _round(se), "fresh_ctr_diff_z": round(diff / se, 2)}
```

and in `row`, after `"established_reward_coverage"`: `**_fresh_ctr_gap(fresh, established),`.

- [ ] **Step 4: Suite** — `647 passed, 2 skipped`. **Step 5: Commit** `feat(freshness): the fresh-vs-established CTR gap with an item-clustered z`

### Task 3: Supply and age bands

**Files:** same. **Interfaces:** Consumes `_clustered_mean`; produces `AGE_BANDS`, `_fresh_supply(obs, fresh_share) -> dict`, `_age_bands(obs) -> list[dict]`; row fields `fresh_item_share`, `fresh_exposure_lift`; envelope key `age_bands` (rows `band, exposures, exposure_share, items, item_share, ctr, z`).

- [ ] **Step 1: Failing tests**

```python
def test_freshness_compares_exposure_with_supply():
    row = compute_freshness(_fresh_samples(), datetime(2026, 8, 1, tzinfo=timezone.utc))["rows"][0]

    # Two of four movies are fresh, and they take half the exposures: lift 1.0.
    assert (row["fresh_item_share"], row["fresh_exposure_lift"]) == (0.5, 1.0)


def test_freshness_age_bands_put_edges_inside_and_keep_empty_bands():
    bands = compute_freshness(_fresh_samples(), datetime(2026, 8, 1, tzinfo=timezone.utc))["age_bands"]

    assert [(b["band"], b["exposures"], b["items"]) for b in bands] == [
        ("0-7 d", 2, 1), ("8-30 d", 2, 1), ("31-90 d", 2, 1), ("91-365 d", 0, 0), ("> 1 y", 2, 1)]
    assert bands[3]["ctr"] is None and bands[0]["z"] is None  # empty band; one movie is untestable
    assert compute_freshness(pd.DataFrame([{"new_release": True}]), datetime(2026, 8, 1, tzinfo=timezone.utc))["age_bands"] == []


def test_freshness_age_band_z_is_item_clustered_against_overall_ctr():
    rows = []
    for item, published, clicks in (("a", "2026-07-27", (1, 1)), ("b", "2026-07-26", (1, 0)),
                                    ("c", "2025-06-01", (0, 0)), ("d", "2025-03-01", (0, 1))):
        rows += [{"item_id": item, "published_at": f"{published}T00:00:00Z", "clicked": c, "impression_ts": _T0}
                 for c in clicks]
    bands = {b["band"]: b for b in compute_freshness(pd.DataFrame(rows), datetime(2026, 8, 1, tzinfo=timezone.utc))["age_bands"]}

    # p0 = 4/8. Residuals about p0: a 2 - 1, b 1 - 1 -> se 0.3536, z (0.75 - 0.5) / se.
    assert bands["0-7 d"]["z"] == 0.71 and bands["> 1 y"]["z"] == -0.71
```

- [ ] **Step 2: Run — expect FAIL** `KeyError: 'fresh_item_share'` / `'age_bands'`.
- [ ] **Step 3: Implement**

Module constant near `SERIES_BUCKETS`:

```python
# Content-age bands; the 8-30 d edge matches the default 30-day freshness window.
AGE_BANDS = (("0-7 d", 0.0, 7.0), ("8-30 d", 7.0, 30.0), ("31-90 d", 30.0, 90.0),
             ("91-365 d", 90.0, 365.0), ("> 1 y", 365.0, math.inf))
```

In `row`, after the gap: `**_fresh_supply(obs, _ratio(len(fresh), len(obs))),`. Replace the final `return available(...)` with:

```python
    result = available("Fresh-item exposure", [row], total, _ratio(len(obs), total) or 0.0)
    result["age_bands"] = _age_bands(obs)
    return result
```

Add after `_fresh_ctr_gap`:

```python
def _fresh_supply(obs: pd.DataFrame, fresh_share: float | None) -> dict[str, object]:
    """Fresh share of the distinct movies served, and how much more exposure fresh ones got.

    Age only grows, so a movie fresh at any exposure was fresh at its first.
    """
    identified = obs.dropna(subset=["item_id"])
    if identified.empty:
        return {"fresh_item_share": None, "fresh_exposure_lift": None}
    fresh_movies = identified.groupby("item_id")["fresh"].any()
    supply = _ratio(int(fresh_movies.sum()), len(fresh_movies))
    return {"fresh_item_share": supply,
            "fresh_exposure_lift": _round(fresh_share / supply) if fresh_share is not None and supply else None}


def _age_bands(obs: pd.DataFrame) -> list[dict[str, object]]:
    """Exposure, supply and CTR per content-age band; z against overall CTR, movies as clusters."""
    aged = obs[obs["age"].notna().to_numpy()]
    if aged.empty:
        return []
    clicks = [float(v) for v in aged["clicked"] if v is not None and pd.notna(v)]
    p0 = sum(clicks) / len(clicks) if clicks else None
    movies = aged["item_id"].dropna().nunique()
    rows = []
    for label, low, high in AGE_BANDS:
        inside = ((aged["age"] >= low) if low == 0 else (aged["age"] > low)) & (aged["age"] <= high)
        band = aged[inside.to_numpy()]
        stats = _clustered_mean(band, "clicked", center=p0) if p0 is not None else None
        items = int(band["item_id"].dropna().nunique())
        rows.append({
            "band": label,
            "exposures": len(band),
            "exposure_share": _ratio(len(band), len(aged)),
            "items": items,
            "item_share": _ratio(items, movies) if movies else None,
            "ctr": _mean(float(v) for v in band["clicked"] if v is not None and pd.notna(v)),
            "z": round((stats[0] - p0) / stats[1], 2) if stats and stats[1] else None,
        })
    return rows
```

- [ ] **Step 4: Suite** — `650 passed, 2 skipped`. **Step 5: Commit** `feat(freshness): fresh supply, exposure lift and age bands`

### Task 4: `FreshnessSection`

**Files:** Modify `recsys-pipeline/frontend/components/measurements.jsx` (`FreshnessSection`, replaced entirely).

- [ ] **Step 1: Replace the component**

```jsx
export function FreshnessSection({ data }) {
  const bands = data?.age_bands ?? [];
  return (
    <MeasurementSection
      title="Freshness"
      data={data}
      columns={[
        "scope", "freshness_source", "fresh_share", "fresh_item_share", "fresh_exposure_lift",
        "freshness_coverage", "age_at_exposure_coverage", "mean_content_age_days",
        "median_content_age_days", "fresh_ctr", "established_ctr", "fresh_ctr_diff",
        "fresh_ctr_diff_se", "fresh_ctr_diff_z", "fresh_mean_reward", "established_mean_reward",
        "exposures",
      ]}
      kpis={(rows) => {
        const row = rows[0] || {};
        const gap = row.fresh_ctr_diff;
        return [
          { label: "fresh share", value: share(row.fresh_share),
            detail: row.fresh_item_share == null ? "of exposures"
              : `of exposures · ${share(row.fresh_item_share)} of served movies` },
          { label: "exposure lift", value: num(row.fresh_exposure_lift, 2), detail: "fresh exposure ÷ fresh supply" },
          { label: "median age at exposure",
            value: row.median_content_age_days == null ? "N/A" : `${num(row.median_content_age_days, 0)} d` },
          { label: "fresh − established CTR",
            value: gap == null ? "N/A" : `${gap >= 0 ? "+" : ""}${(gap * 100).toFixed(2)} pp`,
            detail: row.fresh_ctr_diff_z == null ? "no item ids to test" : `z = ${num(row.fresh_ctr_diff_z, 2)}` },
        ];
      }}
      description="How much of what was shown is recent, against how much recent content was served, and whether recency tracks engagement."
      chart={(rows) => {
        const row = rows[0] || {};
        const rewardObserved = (row.fresh_reward_coverage ?? 0) > 0 || (row.established_reward_coverage ?? 0) > 0;
        return (
          <ChartGrid>
            {bands.length ? (
              <>
                <GroupedBarChart title="Exposure vs supply by content age" percentage
                  labels={bands.map((b) => b.band)}
                  series={[{ name: "share of exposures", values: bands.map((b) => b.exposure_share) },
                           { name: "share of movies", values: bands.map((b) => b.item_share) }]} />
                <BarChart title="CTR by content age" percentage
                  labels={bands.map((b) => b.band)} values={bands.map((b) => b.ctr)} />
              </>
            ) : null}
            <BarChart title="CTR: fresh vs established" percentage
              labels={["fresh", "established"]}
              values={[row.fresh_ctr, row.established_ctr]} />
            {rewardObserved ? (
              <BarChart title="Mean reward by content age"
                labels={["fresh", "established"]}
                values={[row.fresh_mean_reward, row.established_mean_reward]} />
            ) : null}
          </ChartGrid>
        );
      }}
    >
      {bands.length ? (
        <DataTable rows={bands} compact
          columns={["band", "exposures", "exposure_share", "items", "item_share", "ctr", "z"]}
          formatters={{ exposures: count, exposure_share: share, items: count, item_share: share,
                        ctr: share, z: (v) => num(v, 2) }} />
      ) : null}
      <p className="fine-print">
        Content age is measured when the item was shown (impression_ts − published_at), so the
        figures do not change with the export date. Supply is the distinct movies served; a lift
        near 1 means recent movies got their share of exposure. A CTR gap or band z within ±2
        standard errors — movies as the unit — is noise.
      </p>
    </MeasurementSection>
  );
}
```

- [ ] **Step 2: Contract tests and suite** — `python3 -m pytest -q integration-tests/python_modeling/test_dashboard_measurement_contract.py && python3 -m pytest -q` → `650 passed, 2 skipped`.
- [ ] **Step 3: Build on the current snapshot** (no new keys): `npm run build`; in `.next/server/app/serving/freshness.html` expect 4 `class="metric-card"`, no "Exposure vs supply", no "Mean reward by content age".
- [ ] **Step 4: Commit** `feat(dashboard): freshness shows supply, age bands and an honest CTR gap`

### Task 5: Re-export twice and record the evidence

Exporter only against `/tmp/spark-recsys/movie-category-sim` with Redis up; stop and record if either is gone.

- [ ] **Step 1:** Export to the snapshot, then again to `/tmp/claude-fresh-2.json`; assert the two `freshness` sections are identical; diff against the committed snapshot (expect only `freshness` to move).
- [ ] **Step 2:** Record the row (fresh share, item share, lift, median age, gap/z, coverage) and `age_bands`.
- [ ] **Step 3:** Build; expect the band chart, band table and no reward card; screenshot.
- [ ] **Step 4:** Commit the snapshot alone; fill Verification; tick boxes; commit the evidence.

## Verification

- Suite: _(record)_
- Build on the pre-change snapshot: _(record)_
- Double export: _(record)_
- Real-data build: _(record)_
