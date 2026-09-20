# Fixing L3 and adding decade as a dimension — Design

## The defect

`l3(genres, year)` is `f"{primary_genre(genres)}·{decade(year)}"`, and every value it has ever
produced ends in `·unknown`. Not for want of data:

- the producer emits `release_year` on every `MovieEvent` (1980–2024)
- it reaches Redis as `movie:{id}:features → releaseYear`; all 400 movies in the last run carried
  one, spread 1980s–2020s (89 / 87 / 74 / 105 / 45)
- `fetch_movie_meta` already returns it, as `release_year`, at `feature_derivations.py:126`

Then `load_samples` discards it:

```python
meta = {m["item_id"]: m["genres"] for m in fetch_movie_meta(host, port)}
```

Only `genres` survives, so the frame never gains a `release_year` column, so `compute_keyword`'s
`year = df["release_year"] if "release_year" in df.columns else [None] * len(df)` yields all `None`,
so `decade(None)` returns `"unknown"`. One projection, three levels of consequence: L3 has been a
relabelled copy of L2 for its whole existence.

## Change 1 — carry the year

`load_samples` hydrates `release_year` from the same fetch that already hydrates genres. One Redis
scan, not two:

```python
need_genres = missing_genres.any()
need_year = "release_year" not in df.columns
if need_genres or need_year:
    meta = {str(m["item_id"]): m for m in fetch_movie_meta(host, port)}
    ...
```

Three properties this must preserve:

1. **L1 and L2 are untouched.** Both derive from `genres` alone. `GENRE_FAMILY`, `primary_genre`,
   `family_of` and the `dist()`/`tops` shapes do not change.
2. **Redis being unreachable stays graceful.** `fetch_movie_meta` returns `[]` on any exception, so
   `release_year` becomes all-`None` and L3 falls back to `·unknown` exactly as it does today. The
   fix adds a capability; it does not add a dependency.
3. **The year is read with `.get()`, never `[...]`.** The existing test monkeypatches
   `fetch_movie_meta` with dicts carrying only `item_id` and `genres`. Indexing would raise
   `KeyError` and break a passing test for no reason.

One consequence worth stating: `test_load_samples_normalizes_columns` calls `load_samples` with
default host/port and no monkeypatch, so it will now attempt a Redis scan where it previously did
not. `fetch_movie_meta` swallows connection failures, so the test passes with or without a server —
but it does become environment-dependent in the sense that the scan is attempted. That is the cost
of hydrating a column the frame never carries, and there is no way to learn the year without asking.

## Change 2 — decade as its own dimension

Using the existing `decade()` derivation, unchanged. Two additions to `compute_keyword`:

| key | shape | built from |
|---|---|---|
| `by_decade` | one row per decade, with the same metric set `by_keyword` carries | `dist("decade")` |
| `decade_grid` | decade × keyword cross-tab | `cross_tab("dec", "decade")` |

`by_decade` is what makes decade *standalone* rather than a component of L3: it reuses `dist()`, so
it reports impressions, clicks, orders, mean score, exposure and click shares, divergence, CTR and
CVR per decade — the same columns, directly comparable with the keyword breakdown.

`decade_grid` is bounded at 5 × 18 = 90 cells, so it ships in the snapshot like the other two grids.

**Why decade is the more useful axis.** It is the only dimension here not derived from the genre
string. L1, L2, L3 and both existing grids all trace back to `genres[0]`, so their cross-tabs carry
structure by construction — the topic grid's forced diagonal is the visible case. Decade comes from
`releaseYear`, an independent field, so a decade × keyword cell is a real joint observation and the
grid has no forced cells at all.

**Scope decision, stated because it is a judgement call.** `by_decade` and `decade_grid` live inside
the existing `keyword` section rather than becoming a fourteenth dashboard section. A new section
would need catalogue, registry, route, scorecard and measurement-schema changes for a dimension that
answers the same question the keyword section already asks — what the catalog offers and what gets
clicked. If it should be its own page, that is a separate, larger change.

## Change 3 — the colour domain pools three grids

`heatDomain([data.grid, data.topic_grid, data.decade_grid])`. The shared domain exists so a shade
means the same rate in every figure in the section; leaving the new grid out would break exactly that
property. The rendered colours shift slightly because the pooled percentiles move, and the legend
already prints the range it spans.

## Artifacts

`/tmp/spark-recsys/movie-category-sim/training-samples` no longer exists, so the snapshot cannot be
re-exported and a full `run-movie-category-sim.sh` is required. Every number moves
([[project_dashboard_snapshot_regeneration]]).

The comparison the change is judged by, captured before and after:

- L3's distinct values — currently 18, all `<genre>·unknown`; expected ~90 spanning five decades
- `keyword × decade` — currently absent; expected up to 90 populated cells with no forced diagonal
- `by_decade` — five rows; whether exposure and clicks concentrate in any era

## Acceptance

1. `load_samples` returns a `release_year` column when metadata is available.
2. L3 values carry a real decade rather than `unknown` when metadata is available.
3. L3 still falls back to `·unknown` when Redis is unreachable.
4. L1 and L2 values are unchanged by the fix.
5. `decade()` is reused, not reimplemented.
6. Decade grouping is independent of genre: two items sharing a decade group together regardless of
   their genres, and an item's decade does not change when its genres do.
7. `by_decade` carries the same metric columns as `by_keyword`.
8. `decade_grid` has no forced diagonal, unlike `topic_grid`.
9. Snapshot regenerated; the before/after L3 and decade distributions reported.
10. Full suite green; `validate:data` passes; build clean.
