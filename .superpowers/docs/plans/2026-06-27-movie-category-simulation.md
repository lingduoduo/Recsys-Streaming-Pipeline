# Plan: Movie-Category Engagement Simulation

Implementation plan for [2026-06-27-movie-category-simulation.md](../specs/2026-06-27-movie-category-simulation.md).
Shipped in PR #96 (branch `feat/movie-category-sim`).

**Status:** ✅ shipped. Verified e2e (400 movies, 100k impressions): collector wrote all 400
movie keys; l1/l2/l3 recover the injected ground truth. Tests: Python 77 passed / 1 skipped.

## Decisions

- Taxonomy: l1 = genre family, l2 = primary genre, l3 = genre × decade.
- Per-item independent click model (each impressed item clicked with its own category-modulated
  prob), so per-item CTR reflects the item's category.
- Reuses the hardened sim pattern from the user-segment sim (unique per-run topics, Redis flush,
  drain collector until all NUM_ITEMS keys written).

## Tasks

### 1 — movie_categories.py → verify: derivation unit tests
- Pure `l1`/`l2`/`l3`/`decade`/`primary_genre`/`family_of`; accept genres as list or comma-string.

### 2 — movie_segment_producer.py → verify: model + shape unit tests
- `assign_movies` (genres+releaseYear), `item_click_prob`/`order_prob` (family + recency ground
  truth), `movie_event` (MovieUpdated shape). Emits metadata → movielens_context, behavior →
  recsys_events with independent per-item clicks.

### 3 — movie_category_report.py (PySpark) → verify: fetch parsing unit test + e2e
- `fetch_movie_features` (Redis → derived l1/l2/l3; `[]` if down), `per_item_engagement`,
  `category_metrics`. Joins per-item engagement with movie categories; one table/CSV per level.

### 4 — run-movie-category-sim.sh → verify: e2e run
- docker → producer → collector (→ Redis movie:*:features, drain to NUM_ITEMS) →
  OnlineJoiner (→ Parquet) → report.

## Verification (real run)

| Level | result | matches injected? |
|---|---|---|
| l1 | SciFi&Fantasy 0.230 > Action 0.204 > Crime 0.184 > Comedy 0.175 > Drama 0.167 > Other 0.132 | ✓ |
| l3 | top cells SciFi/Fantasy/Animation in recent decades | ✓ (family + recency) |

## Future scope

- Significance tests and 2-D cross-tabs (e.g. l1 × decade); secondary-genre handling in l2.
