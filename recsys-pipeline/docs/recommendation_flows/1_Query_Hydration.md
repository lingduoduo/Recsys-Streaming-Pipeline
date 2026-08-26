# Retrieval Pipeline

**Flow:** **Current: Query hydration** · [Next](2_Fetch_Popular_Stuff.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

For the complete local startup sequence, follow the [root quick start](../../../README.md#recsys-pipeline).

Before candidate retrieval and scoring, the service enriches each request with Redis-backed user
state.

### Query Hydration

Spring injects the available `QueryHydrator<ScoredMoviesQuery>` beans into
`HybridRecommendationService`, which applies them sequentially to the request.

## Required state

- `MovieLensContextCollectorStreamingJob` is the writer for the legacy
  `user:{id}:features` hash (demographics, rating aggregates, and recent/action sequences), the
  `movie:{id}:features` hash (title, genres, and release year), and the columnar rating sequence
  hashes `seq:{id}:rating:{day}`. The serving path reads the user hash; it reads the sequence hashes
  when `recsys.sequence.mode=on` or compares them in `shadow` mode. The movie hash is not read by
  query hydration or by the current serving path.
- The in-repo serving side-effect writer maintains `user:{id}:served_history` and
  `user:{id}:impressions` after a request selects at least one item.
- The retrieval service reads `user:{id}:recent` and `user:{id}:rated` lists plus
  `user:{id}:request_history`, `user:{id}:bloom_filter`, and `user:{id}:cached_movies` hashes, but
  this repository contains no writer for those keys. They must be externally populated or
  pre-seeded. The context collector also does not write optional `favoriteGenres` or
  `inferredGenres` fields in `user:{id}:features`; those require an external/pre-seeded value when
  used.

When a hash or sequence key is absent, its client returns an empty/default value. Hydration still
completes, but downstream filtering and scoring lose that history, demographic, or preference
signal. In `on` sequence mode, a sequence-store read error falls back to the legacy
`user:{id}:features` sequence.

| Hydrator | Field(s) hydrated | Source |
|---|---|---|
| `UserDemographicsQueryHydrator` | `demographics` | `MovieLensFeatureClient` (`user:{id}:features`) |
| `UserMovieFeaturesQueryHydrator` | Base `MovieLensUserFeatures` fields | `MovieLensFeatureClient` (`user:{id}:features`) |
| `RatingSequencesQueryHydrator` | Action (50), retrieval (100), and scoring (20) sequence views | `recentlyRatedMovieIds` in `user:{id}:features`, or `seq:{id}:rating:{day}` according to `recsys.sequence.mode` |
| `BehaviorSequencesQueryHydrator` | `watchedMovieIds`, with recent engagement (`detail_view`, `click`) merged in front; runs last | `seq:{id}:behavior:{day}` according to `recsys.sequence.mode`; `off` reads nothing |
| `ServedHistoryQueryHydrator` | `servedMovieIds` | `ServedHistoryClient` (`user:{id}:served_history`) |
| `PastRequestTimestampsQueryHydrator` | `pastRequestTimestamps` | `PastRequestTimestampsClient` (`user:{id}:request_history`) |
| `CachedMoviesQueryHydrator` | `cachedMovieIds`, `hasCachedMovies` (true at 100+ IDs) | `CachedMoviesClient` (`user:{id}:cached_movies`) |
| `InferredGenresQueryHydrator` | `inferredGenres` | `MovieLensFeatureClient` (`user:{id}:features`) |
| `ImpressedMoviesQueryHydrator` | `impressedMovieIds` | `ImpressedMoviesClient` (`user:{id}:impressions`) |
| `ImpressionBloomFilterQueryHydrator` | `impressionBloomFilter` | `ImpressionBloomFilterClient` (`user:{id}:bloom_filter`) |
| `MovieLensUserHistoryQueryHydrator` | `watchedMovieIds`, `ratedMovieIds` | `UserMovieHistoryClient` (`user:{id}:recent`, `user:{id}:rated`) |

These are the query hydrators present in
`com.demo.retrieval.service.query_hydrators`; the service has no creator-relationship, location,
subscription, or collection hydrators.
