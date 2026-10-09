# Diversity dashboard: an honest long tail, catalog-level spread, distributions, trends — Design

## The gap

The Diversity section shows four per-slate averages, one bar chart and a table of the aggregate row
plus ten slate rows. On the shipped run (16,726 five-item slates, 83,630 exposures):

| KPI | value |
|---|---|
| normalized genre entropy | 0.9725 |
| unique genres per slate | 7.95 |
| intra-list genre distance | 0.9294 |
| long-tail exposure share | 0.7958 |

Two defects make the section unable to catch the failure it exists for — a recommender that
narrows what people see:

1. **The long-tail KPI is circular.** `compute_diversity` takes the cutoff as the 80th percentile
   of popularity *over exposures* (`quality_measurements.py:216`) and then reports the share of
   exposures below it (`:359`). That is ≈ 0.80 by construction: a recommender serving only
   blockbusters scores the same. On this run 0.796, against 0.760 with the cutoff taken over
   distinct items.
2. **Every KPI is a per-slate average.** A recommender that showed every user the same five
   genre-diverse films would score perfectly on all four. Nothing measures spread across the
   catalog or across a user's history.

And the ten slate rows are an arbitrary sample of 16,726 — they read as data and say nothing.

## What the sim will show — stated up front

The sim's serving is near-uniform: all 400 items are served, the exposure Gini over items is 0.038,
the top 10% of items take 11.2% of exposures, per-slate entropy p5/p50/p95 is 0.94/0.97/1.00, and
the median user saw 261 distinct items. On sim data every figure here sits near its ideal. The point
is a section that *would* move for a biased recommender on real data; the page says so.

## Design

### 1. Long tail over distinct items

`long_tail_popularity_cutoff` becomes the `long_tail_percentile` quantile of popularity over
**distinct served items**, each counted once (first-seen popularity per `item_id`).
`long_tail_exposure_share` keeps its meaning: the share of exposures whose item popularity is below
the cutoff, per slate and in aggregate.

- The population is *served* items: popularity (`global:item_popularity`) is only known for items
  in it. The fine print says so.
- An item without an `item_id` counts as its own distinct item, so slates in tests that carry no
  ids behave as before (the existing two-item test still yields 0.5).

### 2. Catalog-level fields on the aggregate row

| field | definition |
|---|---|
| `items_served` | distinct `item_id` across all slates |
| `catalog_size` | catalog size if known, else `None` |
| `catalog_coverage` | `items_served / catalog_size`; `None` when the size is unknown |
| `exposure_gini` | Gini of per-item exposure counts over served items (0 = even) |
| `top_decile_exposure_share` | share of exposures going to the top `max(1, n // 10)` items |
| `median_items_per_user` | median over users of distinct items served to them |
| `user_repeat_rate` | `1 − Σ distinct-per-user / Σ exposures-per-user` (0 = never repeated) |

Each is `None` when its input is absent (no `item_id`s, no `user_id`s), never 0.

**Amended after the final review:**
- `exposure_gini` and `top_decile_exposure_share` run over the **whole catalog** when
  `catalog_size` is known, an unserved item counting as zero exposures. Over served items alone, a
  recommender serving the same five items to everyone read Gini 0.0 — "perfectly even".
- A new `user_repeat_rate_uniform` gives the repeat rate uniform serving would produce,
  `1 − Σ_u C·(1 − (1 − 1/C)^{E_u}) / Σ_u E_u` (`None` without `catalog_size`). Random serving
  repeats too: on the sim, 0.3789 measured against 0.3818 uniform. The KPI shows it as its detail.
- The spread fields come from item ids alone, so they are excluded from the "missing genre and
  popularity diversity signals" guard, which they had made dead; a histogram with no observed value
  is `[]`, not ten zero bins.

`catalog_size` arrives as a new optional argument, `compute_diversity(slates, long_tail_percentile,
catalog_size=None)`. `load_slates` already reads the full catalog (`fetch_movie_meta`) to attach
genres; it records `slates.attrs["catalog_size"] = len(genres)` (or leaves it unset when Redis gave
nothing), and `build_measurement_dashboard` passes `slates.attrs.get("catalog_size")`.

### 3. Distributions instead of samples

`compute_diversity` keeps returning `[aggregate, *slate_rows]` — per-slate rows are a tested caller
contract. Two envelope keys are added:

- **`distributions`** — `{"normalized_genre_entropy": [...], "intra_list_genre_distance": [...]}`,
  each 10 bins over [0, 1] as `{"bin_start": 0.0, "count": n}`, a value of exactly 1.0 in the last
  bin, slates with a `None` value skipped.
- **`genre_exposure`** — per genre, `exposure_share` (its share of all genre mentions across
  exposures) and `served_share` (its share of genre mentions across distinct served items), sorted
  by `exposure_share` descending. Serving that over-weights a genre relative to what it serves shows
  as the two bars diverging.

The **exporter** publishes only the aggregate row: the distributions cover every slate, so the
sampled slate rows and their "showing 10 of N" warning go. `_bounded_slate_rows` and
`SLATE_ROW_LIMIT` are replaced by `_aggregate_diversity_row`, which keeps `rows[0]`.

### 4. Diversity over time

The bucketing in `_satisfaction_series` is extracted into
`_time_buckets(stamps: pd.Series) -> tuple[pd.Series, list[float], float] | None` — bucket index per
row, bucket starts, width — with the whole-second rule unchanged (width `ceil((span + 1) / 24)` for
integer stamps, else `span / 24`; at most 24 buckets). Satisfaction calls it; its tests are the
guard that behaviour did not move.

Diversity adds **`series`** and **`series_bucket_seconds`**, bucketing slates by `request_ts`
(epoch seconds; the min impression time of the slate). Each bucket:

```json
{"bucket_start": 1791506959.0, "slates": 780, "normalized_genre_entropy": 0.972,
 "intra_list_genre_distance": 0.93, "long_tail_exposure_share": 0.76, "items_served": 398}
```

Means over the bucket's slates; the long-tail share uses the run-wide cutoff so buckets are
comparable; `None` rates for an empty bucket; `[]` when `request_ts` is absent or has no span.

### 5. UI — `DiversitySection`

- **KPIs** (aggregate row): genre entropy, intra-list distance, long-tail share, catalog coverage,
  exposure Gini, user repeat rate.
- **Charts**: entropy histogram and intra-list-distance histogram (`BarChart`, labels "0.0–0.1" …);
  genre exposure vs served share (`GroupedBarChart`, two series); over time — entropy and
  intra-list distance together, then long-tail share (`LineChart`, caption "N × w buckets over
  span"); when `series` is empty, one line "Time series unavailable (no request_ts span)".
- **Table**: the aggregate row with the new columns; no slate rows.
- **Fine print**: the long tail is over distinct served items; the sim serves near-uniform slates,
  so every figure sits near its ideal.

## Non-goals

- No catalog-wide popularity for never-served items.
- No per-segment diversity — Fairness slices by segment.
- No embedding-based (semantic) diversity.
- No change to the per-slate metrics' definitions other than the long-tail cutoff.

## Testing

Python first (`test_quality_measurements.py`, `test_analysis_dashboard.py`):

- **Circularity**: a slate set where one popular item is served many times. The exposure-quantile
  cutoff and the distinct-item cutoff differ, and the published share follows the distinct-item
  one.
- Gini and top-decile share on known exposure counts; `items_served`, `catalog_coverage` with and
  without `catalog_size`; `median_items_per_user` and `user_repeat_rate` on a known frame; `None`
  without `item_id` / `user_id`.
- Histogram bins (1.0 in the last bin, `None` skipped); genre exposure vs served shares.
- `_time_buckets` extraction: the satisfaction series tests unchanged and green; diversity series
  buckets, run-wide cutoff, empty series without `request_ts`.
- Exporter: `_aggregate_diversity_row` keeps only the aggregate; `load_slates` records
  `catalog_size`.
- Existing: `test_dashboard_columns_match_the_published_measurement_keys` (every requested column
  published), the scorecard headline test (`normalized_genre_entropy` on `rows[0]`).

Frontend: `npm run build`, then in the built Diversity page count 6 KPI cards, 2 histogram cards,
1 grouped chart, 2 line charts.

## Snapshot

Re-export only, against the surviving run's Parquet and Redis — no re-simulation. Expected moves:
the diversity section, plus freshness's two content-age fields (wall-clock). Anything else moving is
a regression.

## Acceptance

1. The long-tail cutoff is taken over distinct served items; the circularity test passes.
2. The aggregate row publishes the seven catalog-level fields with the semantics above.
3. `distributions`, `genre_exposure`, `series`, `series_bucket_seconds` are published; satisfaction
   output is unchanged by the `_time_buckets` extraction.
4. The snapshot carries the aggregate row only for diversity; the Diversity page shows 6 KPIs, two
   histograms, the genre chart and two time charts.
5. Full suite green; no new npm dependency.
