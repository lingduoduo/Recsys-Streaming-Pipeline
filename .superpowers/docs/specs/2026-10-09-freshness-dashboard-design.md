# Freshness dashboard: age at exposure, an honest CTR gap, supply, age bands — Design

## The gap

The Freshness section is one row: `fresh_share` 0.1378, mean / median content age 414.6 / 414.9
days, fresh CTR 14.98% vs established 15.05%, two always-empty reward columns. Measured on the
surviving run (83,630 samples, 400 movies):

1. **Content age is measured at export time, not exposure time.** `_timestamp_observations` ages
   every sample against `now` (`datetime.now()` in the exporter). The run was exported 1.7 h after
   it ran, so today the numbers are close — but exporting the same run 30 days later drops
   `fresh_share` from 0.138 to **0.000**: every item "ages out" though it was fresh when shown. It
   is also why the two age fields drift on every re-export
   (`project_dashboard_snapshot_regeneration`).
2. **The fresh-vs-established CTR comparison is noise presented as two bars.** The difference is
   −0.07 pp; with movies as the unit (a cohort is a few dozen movies seen many times — the lesson
   of #280) its z is **−0.14**, and the fresh side rests on 54 movies.
3. **No supply reference.** Fresh items take 13.78% of exposures and are 13.50% of served movies —
   a lift of 1.02, i.e. serving is not recency-biased. Nothing on the page says so.
4. **One 30-day cut hides the age profile.** By band: 0–7 d 3.6% of exposures (14 movies), 8–30 d
   10.2% (40), 31–90 d 5.4% (21), 91–365 d 26.5% (106), > 1 y 54.4% (219).
5. **The reward chart is always empty** (reward coverage 0 on the sim) and renders as a blank card.
6. `compute_freshness` takes **1.7 s** on 83k rows (`iterrows`).

## Design

### 1. Age at exposure

`_timestamp_observations` is rewritten vectorised and returns a frame
(`fresh, age, clicked, reward, item_id, at_exposure`) instead of a tuple list:

- `age = impression_ts − published_at` in days, clipped at 0, where `impression_ts` (epoch
  seconds) is finite; otherwise `now − published_at` as today.
- A new row field `age_at_exposure_coverage` = share of observations aged at exposure (`None` for
  the boolean fallback). The fine print states the reference.

With every sample carrying `impression_ts`, freshness no longer depends on `now`: two exports of
the same run must produce identical freshness rows.

`_boolean_freshness_observations` returns the same frame shape (`age` NaN, `at_exposure` False)
and keeps `_boolean_value` per value, so the documented-encodings contract is unchanged.

### 2. Fresh − established CTR with its uncertainty

Row fields `fresh_ctr_diff`, `fresh_ctr_diff_se`, `fresh_ctr_diff_z`. Each cohort's mean CTR gets an
item-clustered (sandwich) SE, `Σ_i (clicks_i − p̄·n_i)² / n² · k/(k−1)` over its `k` movies; the
difference's SE is `sqrt(se_f² + se_e²)`. All three are `None` without `item_id` or with fewer than
two movies in either cohort.

### 3. Exposure vs supply

- `fresh_item_share` — share of distinct served movies that were fresh at their first exposure.
  Age only grows, so "fresh at first exposure" is "fresh at any exposure":
  `groupby(item_id).fresh.any()`.
- `fresh_exposure_lift` — `fresh_share / fresh_item_share`; `None` when the item share is 0 or
  unknown.

The population is served movies (samples carry no catalog list); the fine print says so.

### 4. Age bands

Envelope key `age_bands`, one row per band — `0-7 d` [0, 7], `8-30 d` (7, 30], `31-90 d`
(30, 90], `91-365 d` (90, 365], `> 1 y` (365, ∞) — matching `fresh = age ≤ 30`:

```json
{"band": "8-30 d", "exposures": 8508, "exposure_share": 0.1017, "items": 40,
 "ctr": 0.1492, "z": -0.31}
```

`z` is the band CTR against the overall CTR `p0`, item-clustered with residuals about `p0` (the
heatmap's convention). Empty bands are kept with `ctr` / `z` `None`. `age_bands` is `[]` for the
boolean fallback.

### 5. UI — `FreshnessSection`

- **KPIs**: fresh share (detail "of exposures · X% of served movies"), exposure lift, median age
  at exposure (days), fresh − established CTR (detail "z = …", or "no item ids").
- **Charts**: exposure share vs movie share by band (`GroupedBarChart`); CTR by band (`BarChart`);
  the existing CTR-by-cohort chart; the reward chart only when either cohort's reward coverage is
  above 0.
- **Tables**: the summary row (new columns added) and the band rows (`DataTable`).
- **Fine print**: ages are at exposure; supply is served movies; a CTR gap within two standard
  errors is noise.

## Non-goals

- No time series: freshness moves over days, the sim spans a minute.
- No catalog-wide supply (never-served movies have no `published_at` in samples).
- No change to the live-service freshness row.

## Testing

Python first (`test_quality_measurements.py`):

- Age taken from `impression_ts` when present; export-time fallback when absent; two calls with
  `now` 30 days apart give identical rows when every sample has `impression_ts`.
- `age_at_exposure_coverage` 1.0 / mixed / `None` for boolean.
- Clustered diff on a hand-computed frame; `None` without `item_id`.
- `fresh_item_share` and lift; band edges at exactly 7 and 30 days; empty band kept.
- Every existing freshness test unchanged and green (they carry no `impression_ts`, so they
  exercise the fallback).

Frontend: build on the current snapshot (no new keys) renders without the band charts and without
the reward card; after re-export, the band charts render and the reward card stays hidden.

## Snapshot

Re-export only, **twice**, against the surviving run: the two freshness sections must be
identical (this retires the known drift). Expected moves vs the committed snapshot: freshness only.

## Acceptance

1. Content age is measured at exposure; freshness rows are identical across exports.
2. The CTR gap publishes its clustered SE and z; the KPI shows z.
3. `fresh_item_share`, `fresh_exposure_lift` and `age_bands` publish as defined.
4. The reward chart renders only with observed reward.
5. Full suite green; no new npm dependency.
