# Satisfaction dashboard: population, time series, rating shape — Design

## The gap

The Satisfaction section is one row of whole-run averages from `compute_satisfaction`
(`quality_measurements.py:78`), shown as four KPIs, two bar charts and a table. On the shipped
snapshot that row is: CTR 15.1%, order rate 3.4%, mean rating 4.17, mean dwell 69,938.8.

Three things a reader needs are missing:

1. **Who and what the averages cover.** No user count and no item count. "CTR 15.1%" over 200
   users and 400 items is a different claim than over 20 users and 5 items, and nothing on the
   page says which.
2. **Whether the averages are stable.** One number per metric over the whole run hides any drift.
   There is no time dimension anywhere in the section.
3. **How much rating evidence there is.** `mean_rating` 4.17 sits beside `rating_coverage` 3.4%,
   but the count of ratings behind it, and their spread, are not shown.

One defect rides along: mean dwell is rendered as raw milliseconds (`num(row.mean_dwell_millis, 0)`
→ "69939"), which reads as a count, not a duration.

## What the data supports — and two caveats that shape the design

Training samples carry `user_id`, `item_id`, `impression_ts`, `clicked`, `ordered`, `rating`.
`impression_ts` is **epoch seconds** (`OnlineJoinerStreamingJob.scala:150`).

**Caveat 1 — the sim's clock spans minutes, not days.** `make_slate` stamps every event with
`time.time()` at generation (`movie_segment_producer.py:245`), so a whole run's samples fall inside
however long the producer ran. A per-day series would be a single point. The series therefore
uses **adaptive buckets**: the observed `[min, max]` of `impression_ts` split into 24 equal-width
buckets. On sim data it shows drift across the run; on real data spanning weeks the same code
shows weekly drift. The page states the span and bucket width rather than implying a calendar.

**Caveat 2 — sim ratings are thin and derived.** Only orders carry a rating (3.4% of samples), and
the producer sets `rating = round(min(5, 3 + 2·completion), 1)`. A rating trend in the sim
re-plots completion among orders, and a bucket holds roughly 1/24 of ~3,000 ratings. The page says
so, and every rating point carries its count.

## Design

### Python — `compute_satisfaction`

The summary row gains three fields:

| field | definition |
|---|---|
| `users` | distinct non-null `user_id` |
| `items` | distinct non-null `item_id` (items *served*, not catalog size) |
| `rated_samples` | count of samples with a numeric `rating` — the n behind `mean_rating` |

Each is `None` when its column is absent, never 0: 0 users would be a false measurement.

The envelope gains two top-level keys beside `rows` (`_merge_live_row` spreads `**offline`, so
they survive the live-row merge untouched):

**`series`** — a list of 24 buckets, each:

```json
{"bucket_start": 1760000000, "impressions": 3688, "users": 191,
 "ctr": 0.149, "order_rate": 0.034, "mean_rating": 4.18, "ratings": 125}
```

- Bucket width `w = (max_ts − min_ts) / 24`; sample in bucket `min(floor((ts − min_ts) / w), 23)`,
  so the maximum timestamp lands in the last bucket rather than a 25th.
- **Amended after the first real run:** when every stamp is a whole second (what the joiner
  publishes), `w = ceil((span + 1) / 24)` whole seconds and the bucket count is
  `ceil((span + 1) / w)`, at most 24. A fractional width gave each bucket 2 or 3 distinct seconds,
  a sawtooth in every count from the width alone (impressions 1,265–4,790 per bucket on a 66 s
  run). Only the last bucket can be partial, and the page says so.
- `ctr` and `order_rate` over that bucket's samples carrying the signal; `mean_rating` over its
  rated samples only, `None` when it has none — never 0, which would draw a cliff.
- Empty buckets are kept (`impressions: 0`, rates `None`) so the x-axis stays evenly spaced.
- `series` is `[]` when `impression_ts` is absent or every sample shares one timestamp
  (zero span). The rest of the section is unaffected.

Plus `series_bucket_seconds` (`w`, rounded to 0.1) so the UI can state the bucket width.

**`rating_distribution`** — counts in half-point bins `[1.0, 1.5) … [4.5, 5.0]`, as
`[{"rating": 1.0, "count": n}, …]`. Bins run from the lowest observed rating's bin through the
4.5 bin, zero-count bins included, so a gap shows as a gap; 5.0 falls in the 4.5 bin.
`[]` when there are no ratings.

### Frontend

**`MeasurementSection`** passes the envelope to the chart callback: `chart(rows, data)`. The other
six sections ignore the second argument.

**KPIs** (offline row, `rows[0]` as today): users, items, CTR, order rate, mean rating with
`detail` "n = 2,983", mean dwell in **seconds** (`(ms / 1000).toFixed(1) + " s"`).

**`LineChart`** — new, in `ui.jsx`, plain SVG, no dependency (the frontend has none beyond Next and
React and should stay that way). Props: `title`, `labels` (x labels, as `BarChart` names them), `series`
(1–2 of `{name, values, notes?}`, where `notes[i]` is appended to point i's tooltip),
`percentage`, `valueFormatter`, `caption`. Draws a polyline per series broken at `null`s (a gap,
not a zero), a dot per point with a `<title>` tooltip, y-axis min/max labels, and the caption under
the plot. Series colours reuse the `GroupedBarChart` index-fixed palette; two series maximum.

**Charts**, in order:

1. CTR and order rate over time (2 series, percentage).
2. Mean rating over time; each point's tooltip includes its rating count.
3. Active users per bucket.
4. Rating distribution (existing `BarChart`).
5. Optional signal coverage (existing; kept, including its `const fields = [...]` list, which
   `test_satisfaction_coverage_chart_omits_the_series_that_cannot_show_coverage` parses).
6. Engagement rates (existing; kept).

The time charts share one caption: "24 × {w} buckets over {span}". When `series` is empty they are
replaced by one line: "No impression timestamps — time series unavailable."

**Fine print** adds the two caveats in one sentence each.

The table gains `users`, `items`, `rated_samples` columns.

## Non-goals

- No change to the sim's clock. Spreading events over simulated days would move freshness, the
  CTR train/holdout split and OPE; that is its own change if wanted.
- No catalog-size denominator for item coverage — it needs Redis and is not what "items" asks.
- No per-segment breakdowns; Fairness already slices CTR by segment.
- No chart library.

## Testing

Python, written first (`test_quality_measurements.py`):

- `users`/`items`/`rated_samples` on a known frame; `None` when the column is absent.
- Bucketing: 24 buckets; equal width; the max timestamp lands in bucket 23; an empty middle bucket
  is present with `impressions: 0` and `None` rates.
- `mean_rating` averages rated rows only; `None` for a bucket with no ratings.
- `series == []` for a missing `impression_ts` and for a zero span.
- `rating_distribution` bin edges, including 5.0 in the last bin and zero-count bins.
- The live-row merge keeps `series` (`test_dashboard_measurement_contract.py`).

Frontend: `npm run build`, then count in the built Satisfaction page: the 3 line charts (`<polyline`
inside `.line-chart`), the 6 KPI cards, and that dwell renders with " s".

## Snapshot

The Parquet behind the current snapshot is gone (`/tmp` was cleared), so refreshing it means
re-running `scripts/run-movie-category-sim.sh` and then the exporter. That moves every number in
the snapshot, as any re-simulation does. It lands as its own commit so the code diff stays
reviewable, and the PR records the observed span, bucket width, user/item counts and rating count.

## Acceptance

1. `compute_satisfaction` publishes `users`, `items`, `rated_samples`, `series`,
   `series_bucket_seconds`, `rating_distribution` with the semantics above; full suite passes.
2. The live-row merge preserves them.
3. The Satisfaction page shows 6 KPIs (dwell in seconds), 3 time charts with a span caption, the
   rating distribution and the coverage chart; an empty series degrades to one line.
4. No new npm dependency.
5. The snapshot is regenerated from a fresh sim run, in a separate commit.
