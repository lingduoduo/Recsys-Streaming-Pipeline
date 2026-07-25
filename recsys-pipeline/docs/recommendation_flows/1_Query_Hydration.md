# Retrieval Pipeline

**Flow:** **Current: Query hydration** · [Next](2_Fetch_Popular_Stuff.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

For the complete local startup sequence, follow the [root quick start](../../../README.md#recsys-pipeline).

Before scoring, each request is enriched through two sequential pipelines.

### Query Hydration

`QueryHydrator<ScoredMoviesQuery>` implementations populate per-user context fields on the incoming request. Each hydrator reads one concern and writes one field group; hydrators run independently and can be parallelized.

## Required state

- `MovieLensContextCollectorStreamingJob` is the writer for the legacy
  `user:{id}:features` hash (demographics, rating aggregates, and recent/action sequences), the
  `movie:{id}:features` hash (title, genres, and release year), and the columnar rating sequence
  hashes `seq:{id}:rating:{day}`. The serving path reads the user hash; it reads the sequence hashes
  when `recsys.sequence.mode=on` or compares them in `shadow` mode. The movie hashes are pipeline
  context for enrichment and derived datasets, but are not currently read directly by the serving
  hydrators.
- Serving side effects and other feature writers maintain the dedicated user keys used by the
  remaining hydrators, including `user:{id}:recent`, `user:{id}:rated`,
  `user:{id}:served_history`, `user:{id}:impressions`, `user:{id}:request_history`,
  `user:{id}:bloom_filter`, and `user:{id}:cached_movies`.

When a hash or sequence key is absent, its client returns an empty/default value. Hydration still
completes, but downstream filtering and scoring lose that history, demographic, or preference
signal. In `on` sequence mode, a sequence-store read error falls back to the legacy
`user:{id}:features` sequence.

| Hydrator | Field(s) hydrated | Source |
|---|---|---|
| `UserDemographicsQueryHydrator` | `demographics` | `MovieLensFeatureClient` (`user:{id}:features`) |
| `UserInferredGenderQueryHydrator` | `inferredGender`, `inferredGenderScore` | `MovieLensFeatureClient`; falls back to `demographics.gender` for new users (ratingCount == 0) |
| `UserMovieFeaturesQueryHydrator` | rating-based features | `MovieLensFeatureClient` |
| `UserActionSequenceQueryHydrator` | `actionSequenceMovieIds` (dedup + truncate to 50) | `MovieLensFeatureClient` (field: `recentlyRatedMovieIds`) |
| `RetrievalSequenceQueryHydrator` | `retrievalSequenceMovieIds` (dedup + truncate to 100) | `UserActionAggregationClient` (`user:{id}:features` via dedup pipeline) |
| `ScoringSequenceQueryHydrator` | `scoringSequenceMovieIds` (dedup + truncate to 20) | `UserActionAggregationClient` |
| `ServedHistoryQueryHydrator` | `servedMovieIds` | `ServedHistoryClient` (`user:{id}:served_history`) |
| `IpQueryHydrator` | `ipLocation` (ZIP code proxy) | `GeoLocationClient` (`user:{id}:features`) |
| `PastRequestTimestampsQueryHydrator` | `pastRequestTimestamps` | `PastRequestTimestampsClient` (`user:{id}:request_history`) |
| `MutualFollowQueryHydrator` | `mutualFollowMinhash` | `SimilarityMinHashClient` (`user:{id}:minhash`) |
| `CachedMoviesQueryHydrator` | `cachedMovieIds`, `hasCachedMovies` | `CachedMoviesClient` (`user:{id}:cached_movies`) |
| `InferredGenresQueryHydrator` | `inferredGenres` (genre preference signal) | `MovieLensFeatureClient` |
| `FollowedGenresQueryHydrator` | `followedGenres` (followed genre IDs) | `MovieLensFeatureClient` |
| `SubscribedUserIdsQueryHydrator` | `subscribedUserIds` | `SocialGraphClient` (`user:{id}:social`) |
| `BlockedUserIdsQueryHydrator` | `blockedUserIds` | `SocialGraphClient` |
| `MutedUserIdsQueryHydrator` | `mutedUserIds` | `SocialGraphClient` |
| `FollowedUserIdsQueryHydrator` | `followedUserIds` | `SocialGraphClient` |
| `ImpressedMoviesQueryHydrator` | `impressedMovieIds` | `ImpressedMoviesClient` (`user:{id}:impressions`) |
| `ImpressionBloomFilterQueryHydrator` | `impressionBloomFilter` | `ImpressionBloomFilterClient` (`user:{id}:bloom_filter`) |
| `FollowedCollectionsQueryHydrator` | `followedCollections` | `FollowedStarterPacksClient` (`user:{id}:starter_packs`) |
| `MovieLensUserHistoryQueryHydrator` | `watchedMovieIds`, `ratedMovieIds` | `UserMovieHistoryClient` (`user:{id}:history`) |

All client classes live under `com.demo.retrieval.service.clients`. `MovieLensFeatureClient` covers the general rating-and-demographics feature store; the remaining clients own a dedicated Redis key namespace.
