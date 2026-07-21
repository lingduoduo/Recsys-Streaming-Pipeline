# Retrieval Pipeline

Before scoring, each request is enriched through two sequential pipelines.

### Query Hydration

`QueryHydrator<ScoredMoviesQuery>` implementations populate per-user context fields on the incoming request. Each hydrator reads one concern and writes one field group; hydrators run independently and can be parallelized.

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

All client classes live under `com.demo.retrieval.service.clients`. `MovieLensFeatureClient` covers the general rating-and-demographics feature store. `SocialGraphClient` covers block/mute/follow/subscribe relationships (different write path); all other clients own a dedicated Redis key namespace.