# Spec: Movie-Category Engagement Simulation

> Item-side parallel of the user-segment sim
> ([2026-06-27-movielens-segment-simulation.md](2026-06-27-movielens-segment-simulation.md)).
> That one breaks engagement down by *user* segments; this one by *movie* category.

## Objective

Compare engagement across a 3-level movie category, where movie metadata flows through the
real context path (`movielens_context` → `MovieLensContextCollectorStreamingJob` → Redis
`movie:{id}:features`) and engagement through the behavior path (`recsys_events` →
`OnlineJoinerStreamingJob` → Parquet). The report **joins** the two by `item_id`. No
topic/schema/Redis-key change.

## Category taxonomy (l1/l2/l3)

Derived from a movie's `genres` + `releaseYear` (see `movie_categories.py`):
- **l1** = genre family (Action&Adventure, SciFi&Fantasy, Drama&Romance, Comedy, Crime&Thriller, Other)
- **l2** = primary (first) genre
- **l3** = primary genre × release decade (e.g. `Sci-Fi·2010s`)

## Data flow

```
movie_segment_producer
  ├─ per movie: {item_id, title, genres, release_year, timestamp}  (MovieUpdated-shaped)
  │     → movielens_context → MovieLensContextCollectorStreamingJob → Redis movie:{id}:features
  └─ behavior slates; each impressed item clicked INDEPENDENTLY with a category-modulated prob
        → recsys_events → OnlineJoinerStreamingJob → training_samples Parquet

movie_category_report.py (PySpark)
  per-item engagement (Parquet, group by item_id) ⨝ Redis movie:{id}:features → derive l1/l2/l3
  → CTR / order_rate / clicks_per_item + CTR lift, per level
```

## Scope

- **In:** `movie_categories.py`, `movie_segment_producer.py`, `movie_category_report.py`,
  `run-movie-category-sim.sh`.
- **Out:** job-logic/schema changes (`writeMovieUpdates` already has no Jedis-pipeline bug, unlike
  the user path); significance tests / cross-tabs (future).

## Metrics & ground truth

Per category value: `impressions`, `ctr`, `order_rate`, `clicks_per_item`, `ctr_lift_pct`.
Injected (per-item click prob): family SciFi&Fantasy > Action&Adventure > Crime&Thriller >
Comedy > Drama&Romance > Other; newer decades higher.

## Success criteria (testable)

- [ ] Movie metadata emitted as MovieUpdated-shaped records to `movielens_context`.
- [ ] Collector populates Redis `movie:{id}:features` (run with `KAFKA_STARTING_OFFSETS=earliest`,
      drain until all `NUM_ITEMS` keys written).
- [ ] Report joins Parquet engagement with Redis categories; emits per-l1/l2/l3 metrics + lift.
- [ ] Run recovers the injected family + recency orderings.

## Notes

- CTR here is per-impression with an **independent per-item** click model (so per-item CTR
  reflects the item's category) — higher absolute CTR than the slate-based sims.
- Run the report via `"$SPARK_HOME/bin/spark-submit"` (pinned Spark 3.5.1 vs pip pyspark 4.1.1).
