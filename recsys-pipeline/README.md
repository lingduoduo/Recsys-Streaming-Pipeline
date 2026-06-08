# Streaming Recsys Platform

`recsys-pipeline` is a recommendation-system playground with three complementary paths:

- A real-time Kafka -> Spark Structured Streaming -> Redis pipeline that keeps recent user history and global popularity fresh.
- A training-data pipeline that joins behavior logs into feature+label samples, writes them to Kafka and HDFS-compatible storage, then rebuilds request-level slates for online learning.
- An offline Spark training flow that builds Item2Vec, user, and ALS embeddings from historical ratings.

The retrieval layer is a Spring Boot service that combines three scoring model types — an offline ONNX deep-learning model, an online learning reward model updated in real time from the feedback stream, and a UCB/Thompson bandit RL policy — with content-based retrieval, a Redis replay buffer, cold-start candidate generation, and feedback tracking.

## Service Layout

All independently runnable application code lives under `services/`:

| Service | Build tool | Responsibility |
|---|---|---|
| `services/spark-streaming-job` | sbt | Streaming ingestion, feature joins, offline embeddings, and candidate generation |
| `services/java-retrieval-service` | Maven | Spring Boot recommendation and feedback API |
| `services/python-modeling` | pip / pytest | Synthetic event producer and MovieLens modeling pipeline |

Infrastructure, shared sample data, and orchestration scripts remain at the `recsys-pipeline` root.

## Architecture

```text
services/python-modeling/producer.py ──(user_id key)──► Kafka: user_events ──► UserEventStreamingJob ──► Redis
                                                                        |── user:{id}:recent  (TTL 7d)
                                                                        └── global:item_popularity

services/python-modeling/producer.py ──(request_id key)► Kafka: behavior_logs ──► OnlineJoinerStreamingJob
                                                    |──► Kafka: training_samples
                                                    └──► Parquet: training-samples/date=YYYY-MM-DD/

Kafka: training_samples ──► ExperienceCollectorStreamingJob ──► Kafka: training_experiences
Kafka: training_experiences ──► RecommendationResponseStatsJob ──► Kafka: recommendation_metrics
Kafka: movielens_context ──► MovieLensContextCollectorStreamingJob ──► Redis user/movie context

ratings.csv ──► ItemSequencePreprocessingJob ──► Item2VecTrainingJob ──► embedding.txt
                                                                     └──► Redis i2vEmb:{item}

ratings.csv + embedding.txt ──► UserEmbeddingTrainingJob ──► user_embedding.txt
ratings.csv ──► AlsEmbeddingTrainingJob ──► als/userFactors + als/itemFactors

user_embedding.txt + item_embedding.txt ──► EmbeddingCandidateGenerationJob
                                                    |──► Redis user:{id}:candidates (top-K by cosine similarity)
                                                    └──► Parquet candidate-generation/

services/python-modeling/movielens_pipeline.py ──► two-tower retrieval + transformer ranking ──► sampledata/*.onnx

services/java-retrieval-service
  ├── Offline: ONNX model file and lookup tables
  ├── Redis: embeddings, bandit counters, user history, reward stats
  └── In-memory: FeatureCache (Caffeine)
        │
        ▼
      /recommend/{user}
      /feedback
      /metrics
      /embedding/{item}
```

## Spark Job Package Structure

The Spark module follows a pipeline-oriented layout inspired by common recommendation feature platforms:

| Package | Responsibility | Examples |
|---|---|---|
| `com.demo.process` | Transform, join, label, and prepare stream or batch data | `OnlineJoinerStreamingJob`, `ExperienceCollectorStreamingJob`, `RecommendationResponseStatsJob`, `ItemSequencePreprocessingJob` |
| `com.demo.task` | Runnable entry points for streaming ingestion and offline training tasks | `UserEventStreamingJob`, `Item2VecTrainingJob`, `UserEmbeddingTrainingJob`, `AlsEmbeddingTrainingJob` |
| `com.demo.recommend` | Candidate generation and recommendation-specific retrieval logic | `EmbeddingCandidateGenerationJob` |
| `com.demo.sink` | External writes and persistence helpers | `RedisWriter` |
| `com.demo.util` | Shared runtime and Spark helpers | `Env`, `SparkSessions` |

## Scoring Model Architecture

Candidate items are scored from three model types, each with a different learning paradigm:

| Model type | Class | Signal | When it learns |
|---|---|---|---|
| **Offline** | `DeepLearningPredictionService` | ONNX MLP score for a (user, item) pair; loaded from `mlp_embedding_model.onnx` at startup | At training time; static at serve time |
| **Online learning** | `OnlineLearningService` | Weighted mean reward per item, genre, tag, and globally; stored as Redis hashes | After every `/feedback` call |
| **RL** | `HybridRecommendationService` | UCB, Thompson Sampling, or tabular Q-learning score using replay state/action/reward events | After impressions and `/feedback` rewards |

The final per-candidate score is computed as:

```
offlineScore  = relevanceWeight × cosine(userEmb, itemEmb)
              + contentWeight   × genreTagOverlap
              + popularityWeight × normalizedPopularity
              + deepLearningWeight × onnxScore

learnedPrior  = offlineScore × (1 − onlineWeight) + onlineScore × onlineWeight

banditScore   = BetaPosteriorMean(learnedPrior, clicks, impressions)
              + explorationBonus(UCB | Thompson)
```

## Storage Architecture

Feature data is split across three tiers by access pattern and update frequency.

| Tier | Technology | What lives there | Update frequency |
|------|-----------|-----------------|-----------------|
| **Offline** | Filesystem | ONNX model (`mlp_embedding_model.onnx`), ID lookup tables (`mlp_embedding_lookups.json`), Parquet training samples partitioned by date | Training jobs; hot-swap without redeploy via env vars |
| **Redis** | Redis | User recent-click lists, global item popularity, item/user embeddings, bandit counters, reward model stats, replay buffer | Streaming jobs (every micro-batch) and `/feedback` calls |
| **In-memory** | Caffeine | Item vectors (`i2vEmb:*`), reward model statistics (`reward-model:*`) | Populated from Redis on first request; TTL-expired and invalidated on write |

The in-memory cache (`FeatureCache`) eliminates O(N × features) Redis round-trips per recommendation request. Before the scoring loop a single `MGET` warms all candidate and recent-item vectors; reward model estimates are cached per key for `rewardTtlSeconds` and invalidated immediately when `/feedback` updates them.

**Offline model hot-swap** — set `ONNX_MODEL_PATH` and `ONNX_LOOKUPS_PATH` to replace the model artifacts on the filesystem without rebuilding the JAR. Falls back to the classpath resources when the env vars are unset (development and test default).

## What The Retrieval Service Does

For each recommendation request, the service:

1. Runs the query hydration pipeline — 21 `QueryHydrator` implementations populate the `ScoredMoviesQuery` with per-user context (watch history, rating sequences, social graph, served history, minhash, cached candidates, bloom filter, geo, demographics, inferred signals).
2. Pulls popular items from `global:item_popularity`.
3. Generates extra cold-start candidates from the configured catalog.
4. Runs candidate filters (`CandidateFilter`) to drop seen, blocked, muted, or ineligible candidates.
5. Runs candidate hydrators (`CandidateHydrator`) to enrich surviving candidates with engagement counts, in-network signals, MinHash Jaccard similarity, and visibility flags.
6. Scores candidates by combining all three model types: offline ONNX scores, online reward-model estimates, and the bandit arm score (see above).
7. Randomizes the top scoring pool slightly to avoid deterministic repetition.
8. Stores pending recommendation context for replay-buffer training.
9. Tracks impressions, clicks, regret-style metrics, novelty, and catalog coverage.

The default catalog and ranking weights live in `services/java-retrieval-service/src/main/resources/application.yml`.

## Quick Start

Prerequisites:

- Java 17
- Apache Spark 3.5.x with Scala 2.12
- `sbt`
- Maven 3.8+
- Docker / Docker Compose
- Python 3

Build the Spark job jar:

```bash
cd services/spark-streaming-job
sbt assembly
cd ../..
```

Start Kafka, Zookeeper, and Redis:

```bash
docker compose up -d
```

Start the clickstream producer:

```bash
python -m pip install -r services/python-modeling/requirements.txt
python services/python-modeling/producer.py
```

Run the streaming job:

```bash
./run-streaming-job.sh
```

Or run it directly with `spark-submit`:

```bash
spark-submit \
  --class com.demo.task.UserEventStreamingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

Run the offline embedding pipeline (Item2Vec → user embeddings → candidate pre-computation):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv ./run-offline-pipeline.sh
```

Or run the Python two-stage pipeline (two-tower retrieval + transformer ranking, no Spark required):

```bash
pip install torch onnx onnxruntime numpy
python services/python-modeling/movielens_pipeline.py
```

The first run trains and exports the three ONNX models. Later runs reuse them. You can
select users, control the retrieval candidate count, or retrain into a separate model
directory:

```bash
python services/python-modeling/movielens_pipeline.py --user alice --top-k 5
python services/python-modeling/movielens_pipeline.py --user alice --user bob
python services/python-modeling/movielens_pipeline.py --force-train --model-dir /tmp/movielens-models
```

Run the focused Python tests with:

```bash
pytest -q services/python-modeling
```

Start the retrieval service:

```bash
cd services/java-retrieval-service
mvn spring-boot:run
```

Query the API:

```bash
curl http://localhost:8080/recommend/user_1
curl http://localhost:8080/recommend/user_1?limit=10
curl http://localhost:8080/metrics
```

## API

### `GET /recommend/{user}?limit=6`

Returns recent interactions, selected recommendations, per-item diagnostics, and request-level metrics.

Example response:

```json
{
  "user": "user_1",
  "recent": ["item_7", "item_2"],
  "recommendations": ["item_5", "item_4", "item_1"],
  "diagnostics": [
    {
      "item": "item_5",
      "estimatedReward": 0.71,
      "relevanceScore": 0.62,
      "contentScore": 0.67,
      "dlScore": 0.74,
      "rewardModelScore": 0.58,
      "explorationBonus": 0.19,
      "banditScore": 0.78,
      "coldStart": true,
      "impressions": 2,
      "clicks": 1
    }
  ],
  "metrics": {
    "algorithm": "ucb",
    "eligibleCandidateCount": 8,
    "randomizationPool": 5,
    "pseudoRegret": 0.04,
    "avgEstimatedReward": 0.68,
    "avgExplorationBonus": 0.12,
    "coldStartShare": 0.5,
    "catalogCoverage": 0.57
  }
}
```

Notes:

- `limit` defaults to `6`.
- `limit` is clamped to `1..50`.
- User and item IDs must match `[a-zA-Z0-9_:-]{1,64}`.

### `GET /predict/{user}/{item}`

Scores a single (user, item) pair using the offline ONNX model. Returns an error if either ID is not in the model's lookup table.

```bash
curl http://localhost:8080/predict/user_1/item_5
```

```json
{"model":"mlp_embedding","user":"user_1","item":"item_5","userId":0,"itemId":4,"score":0.742}
```

### `GET /predict/id?userId=0&itemId=4`

Same as above but accepts raw integer lookup IDs directly.

### `GET /predict/metadata`

Returns model name, lookup table sizes, and ONNX input/output names for the loaded offline model.

### `POST /feedback`

Records user feedback for an exposed item. All Redis writes are batched in a single `executePipelined` call (one network round-trip instead of ~22). The three phases on each call:

1. **Read** — fetch the pending replay context written at serve time (`GET replay:pending:{user}:{item}`). Must happen before the pipeline because reads cannot be issued inside a write pipeline.
2. **Write (pipelined)** — batch all writes in one flush:
   - Increment bandit click counter and per-algorithm metrics hashes.
   - Increment `OnlineLearningService` reward stats for the item, its genres, its tags, and the global prior (`HINCRBY` on `reward-model:*` hashes).
   - Push the rewarded event to the replay buffer (`RPUSH` + `LTRIM`).
3. **Invalidate** — purge affected `reward-model:*` keys from the Caffeine in-memory cache so the next `/recommend` request reads fresh stats.

Example:

```bash
curl -X POST http://localhost:8080/feedback \
  -H 'Content-Type: application/json' \
  -d '{"user":"user_1","item":"item_5","clicked":true,"reward":1.0}'
```

### `GET /metrics`

Returns aggregate online metrics for the currently configured algorithm, plus a built-in per-algorithm comparison view.

- `requests`
- `recommendationsServed`
- `clicks`
- `ctr`
- `avgObservedReward`
- `avgEstimatedReward`
- `avgPseudoRegret`
- `cumulativePseudoRegret`
- `avgNoveltyScore`
- `coldStartImpressions`
- `exploratoryImpressions`
- `catalogCoverage`
- `allAlgorithms.ucb`
- `allAlgorithms.thompson`
- `global`

Metrics are stored in Redis under:

- `bandit:metrics` for all traffic combined
- `bandit:metrics:ucb`
- `bandit:metrics:thompson`

### `GET /embedding/{item}`

Returns an item embedding from Redis using key `i2vEmb:{item}`.

## Real-Time Path

### `services/python-modeling/producer.py`

Publishes synthetic events to Kafka. In `clickstream` mode it writes simple click events keyed by `user_id`. In `behavior` mode it writes full impression/click/order slates keyed by `request_id`, which co-partitions all events in the same slate for the `OnlineJoinerStreamingJob` join. Uses lz4 compression; the event loop accounts for send latency so the configured rate is maintained accurately at high throughput.

Environment variables:

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `KAFKA_TOPIC` | `user_events` | Topic to publish to |
| `PRODUCER_MODE` | `clickstream` | `clickstream` emits simple clicks keyed by `user_id`; `behavior` emits full slates keyed by `request_id` |
| `EVENTS_PER_SECOND` | `1` | Target event rate (loop corrects for send latency) |
| `NUM_USERS` | `5` | Size of synthetic user pool |
| `NUM_ITEMS` | `10` | Size of synthetic item pool |
| `SLATE_SIZE` | `5` | Items per recommendation request in behavior mode |
| `LOG_EVERY` | `100` | Print a log line every N sent events (not every event) |
| `MAX_EVENTS` | `0` | Stop after N events; `0` runs continuously |

Event schema:

```json
{"user_id":"user_1","item_id":"item_3","event_type":"click","timestamp":1713600001}
```

Behavior workflow schema:

```json
{
  "request_id": "req_abc123",
  "user_id": "user_1",
  "item_id": "item_3",
  "event_type": "impression",
  "timestamp": 1713600001,
  "position": 0,
  "user_features": {"tier": "vip"},
  "item_features": {"bucket": "b1"},
  "context_features": {"device": "ios", "country": "US"}
}
```

### `UserEventStreamingJob`

Consumes click events from Kafka and writes Redis state used by the retrieval service. Redis connections are managed through a per-executor `JedisPool` (one pool per JVM, reused across micro-batches) rather than opening a new TCP connection per partition.

For each micro-batch it:

1. Aggregates clicked items per user in a single pass.
2. Writes one `LPUSH` + `LTRIM` + `EXPIRE` per user to `user:{id}:recent` (not one write per event).
3. Aggregates per-item click counts.
4. Writes one `ZINCRBY` per unique item to `global:item_popularity`.

Redis keys written:

| Key | Type | Contents | TTL |
|---|---|---|---|
| `user:{id}:recent` | list | Most recent clicked items, newest first | `RECENT_ITEMS_TTL_SECONDS` (default 7 days) |
| `global:item_popularity` | sorted set | Global click counts | none |

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `UserEventStreamingJob` |
| `SPARK_MASTER` | `local[*]` |
| `SPARK_SQL_SHUFFLE_PARTITIONS` | `4` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `user_events` |
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `RECENT_ITEMS_LIMIT` | `20` |
| `RECENT_ITEMS_TTL_SECONDS` | `604800` (7 days) |
| `REDIS_PIPELINE_SIZE` | `500` |
| `REDIS_POOL_MAX_TOTAL` | `8` |
| `MAX_OFFSETS_PER_TRIGGER` | `5000` |
| `TRIGGER_INTERVAL` | `5 seconds` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/user-event-streaming-job` |

### `OnlineJoinerStreamingJob`

Consumes Kafka behavior logs and turns recommendation impressions plus later feedback into training samples with `features + label`.

```bash
PRODUCER_MODE=behavior KAFKA_TOPIC=behavior_logs python services/python-modeling/producer.py

SPARK_MAIN_CLASS=com.demo.process.OnlineJoinerStreamingJob \
ONLINE_JOINER_INPUT_TOPIC=behavior_logs \
ONLINE_JOINER_OUTPUT_TOPIC=training_samples \
ONLINE_JOINER_HDFS_OUTPUT_PATH=/tmp/spark-recsys/training-samples \
./run-streaming-job.sh
```

For each micro-batch it:

1. Runs a **single-pass conditional `groupBy`** over `(request_id, user_id, item_id)` that replaces the previous double-filter + join pattern: impression/exposure rows contribute position, timestamp, and feature fields; click/order/purchase rows contribute feedback signals — one shuffle instead of two (groupBy + join), one scan instead of two.
2. Drops groups with no impression in this batch (`impression_ts IS NULL`) — pure late-feedback events with no matching slate exposure.
3. Produces one sample per exposed item with `clicked`, `ordered`, and numeric `label` (`0.0` = not clicked, `1.0` = clicked, `2.0` = ordered).
4. Persists the joined samples (`MEMORY_AND_DISK_SER`) and writes them to both sinks inside a `try/finally` that always unpersists.
5. Writes samples to Kafka for online model updates.
6. Writes samples to Parquet **partitioned by date** (`date=YYYY-MM-DD/`) for efficient incremental reads by downstream training jobs.

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `OnlineJoinerStreamingJob` |
| `ONLINE_JOINER_INPUT_TOPIC` | `behavior_logs` |
| `ONLINE_JOINER_OUTPUT_TOPIC` | `training_samples` |
| `ONLINE_JOINER_HDFS_OUTPUT_PATH` | `/tmp/spark-recsys/training-samples` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/online-joiner` |

### `ExperienceCollectorStreamingJob`

Consumes item-level samples from Kafka and rebuilds each recommendation request as a list-level experience, also called an impression list or slate.

```bash
SPARK_MAIN_CLASS=com.demo.process.ExperienceCollectorStreamingJob \
EXPERIENCE_COLLECTOR_INPUT_TOPIC=training_samples \
EXPERIENCE_COLLECTOR_OUTPUT_TOPIC=training_experiences \
./run-streaming-job.sh
```

For each micro-batch it groups samples by `request_id + user_id`, sorts items by `position`, and emits a slate JSON containing request context, item features, item labels, slate size, and aggregate slate reward.

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `ExperienceCollectorStreamingJob` |
| `EXPERIENCE_COLLECTOR_INPUT_TOPIC` | `training_samples` |
| `EXPERIENCE_COLLECTOR_OUTPUT_TOPIC` | `training_experiences` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/experience-collector` |

### `RecommendationResponseStatsJob`

Consumes request-level slates from `training_experiences` and emits global response metric events to Kafka. The job borrows the shape of a For You feed response stats collector: one response produces a total counter, a country-bucketed total counter, selected item/ad counts, and guardrail checks for empty or sufficiently populated responses.

```bash
SPARK_MAIN_CLASS=com.demo.process.RecommendationResponseStatsJob \
RESPONSE_STATS_INPUT_TOPIC=training_experiences \
RESPONSE_STATS_OUTPUT_TOPIC=recommendation_metrics \
./run-streaming-job.sh
```

Metric payloads use:

- `metric_name`: `RecommendationFeed.response`
- `tags`: `type`, `subscription`, optional `country`, optional `blender`, and optional `stage`
- `value`: count for that response/stat

Item/ad splitting reads `item_features.type` or `item_features.product_type`; values `ad`, `ads`, and `sponsored` count as ads, and every other selected candidate counts as an item. `subscription` comes from `user_features.subscription_level` or `user_features.subscription`; `country` comes from context/user country fields and is bucketed; `blender` comes from `context_features.AdsBlenderType` or `context_features.ads_blender_type`.

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `RecommendationResponseStatsJob` |
| `RESPONSE_STATS_INPUT_TOPIC` | `training_experiences` |
| `RESPONSE_STATS_OUTPUT_TOPIC` | `recommendation_metrics` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/response-stats` |

### `MovieLensContextCollectorStreamingJob`

Consumes MovieLens user, movie, and rating context updates from Kafka and writes the Redis hashes used by the retrieval service query hydrators. This is the MovieLens analogue of the source/context collection ideas in the added Rust source references: context events are normalized once in the streaming layer, then the online service reads compact per-user and per-movie feature state at request time.

```bash
SPARK_MAIN_CLASS=com.demo.process.MovieLensContextCollectorStreamingJob \
MOVIELENS_CONTEXT_INPUT_TOPIC=movielens_context \
./run-streaming-job.sh
```

For each micro-batch it:

1. Classifies mixed JSON records as `user_update`, `movie_update`, or `rating`.
2. Merges user demographic fields and rating aggregates into `user:{id}:features`.
3. Maintains `avgRating`, `ratingCount`, `recentlyRatedMovieIds`, and `actionSequenceMovieIds` for query hydration.
4. Stores movie title, genres, and release year under `movie:{id}:features`.

Redis keys written:

| Key | Type | Contents | TTL |
|---|---|---|---|
| `user:{id}:features` | hash | MovieLens user demographics and rating context | `MOVIELENS_CONTEXT_TTL_SECONDS` (default 30 days) |
| `movie:{id}:features` | hash | Movie title, genres, release year | `MOVIELENS_CONTEXT_TTL_SECONDS` (default 30 days) |

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `MovieLensContextCollectorStreamingJob` |
| `MOVIELENS_CONTEXT_INPUT_TOPIC` | `movielens_context` |
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `MOVIELENS_CONTEXT_TTL_SECONDS` | `2592000` (30 days) |
| `MOVIELENS_RECENT_RATINGS_LIMIT` | `50` |
| `REDIS_PIPELINE_SIZE` | `500` |
| `REDIS_POOL_MAX_TOTAL` | `8` |
| `MAX_OFFSETS_PER_TRIGGER` | `5000` |
| `TRIGGER_INTERVAL` | `10 seconds` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/movielens-context-collector` |

## Retrieval Pipeline

Before scoring, each request is enriched through two sequential pipelines.

### Query Hydration

`QueryHydrator<ScoredMoviesQuery>` implementations populate per-user context fields on the incoming request. Each hydrator reads one concern and writes one field group; hydrators run independently and can be parallelised.

| Hydrator | Field(s) hydrated | Source |
|---|---|---|
| `UserDemographicsQueryHydrator` | `demographics` | `MovieLensFeatureClient` (`user:{id}:features`) |
| `UserInferredGenderQueryHydrator` | `inferredGender`, `inferredGenderScore` | `MovieLensFeatureClient`; falls back to `demographics.gender` for new users (ratingCount == 0) |
| `UserMovieFeaturesQueryHydrator` | rating-based features | `MovieLensFeatureClient` |
| `UserActionSequenceQueryHydrator` | `actionSequenceMovieIds` (dedup + truncate to 50) | `MovieLensFeatureClient` — `recentlyRatedMovieIds` |
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

All client classes live under `com.demo.retrieval.service.clients`. `MovieLensFeatureClient` covers the general rating-and-demographics feature store. `SocialGraphClient` covers block/mute/follow/subscribe relationships (different write path). All other clients each own a dedicated Redis key namespace.

### Candidate Filters and Hydrators

After initial candidate generation, candidates pass through two more pipelines.

**Filters** (`CandidateFilter`) drop ineligible candidates:

| Filter | Removes |
|---|---|
| `PreviouslySeenMoviesFilter` | Movies the user has already watched |
| `PreviouslySeenMoviesBackupFilter` | Same as above using `impressedMovieIds`; fallback when bloom filter is unavailable |
| `PreviouslyServedMoviesFilter` | Movies served in recent requests (`servedMovieIds`) |
| `SelfMovieFilter` | Movies where the requesting user is the creator (`userId == ownerId`) |
| `CreatorBlocklistFilter` | Movies from blocked creators |
| `MutedKeywordFilter` | Movies whose title/tags match muted keywords |
| `AgeFilter` | Movies outside the user's age-appropriate range |
| `VideoFilter` | Non-video content (configurable) |
| `ReshareDeduplicationFilter` | Duplicate reshares of the same source movie |
| `GenreIdsFilter` | Candidates not matching requested genre IDs |
| `NewUserGenreFilter` | Topic-restricted candidates for new users |

**Candidate hydrators** (`CandidateHydrator`) enrich surviving candidates with additional signals:

| Hydrator | Adds |
|---|---|
| `CoreDataCandidateHydrator` | Title, genres, release year from movie feature store |
| `InNetworkCandidateHydrator` | Whether the candidate is from a followed creator |
| `MutualFollowJaccardCandidateHydrator` | Jaccard similarity score via MinHash |
| `EngagementCountsCandidateHydrator` | Global rating count and average rating |
| `GenreMatchCandidateHydrator` | Genre match signal |
| `SubscriptionCandidateHydrator` | Subscription-gated content flag |
| `LanguageCodeCandidateHydrator` | Language metadata |
| `HasMediaCandidateHydrator` | Media type flags |
| `BlockedByCandidateHydrator` | Whether the viewer is blocked by the candidate creator |
| `VisibilityFilteringCandidateHydrator` | Visibility policy check |
| `FollowingRepliedUsersCandidateHydrator` | Social proximity signal |
| `QuoteCandidateHydrator` | Quote/reference metadata |
| `GizmoduckCandidateHydrator` | External content safety signal |

## Retrieval Service Configuration

`services/java-retrieval-service/src/main/resources/application.yml` defines Redis connectivity, in-memory cache settings, and recommendation parameters under `recsys`.

### Offline model paths

Set these to load model artifacts from the filesystem instead of the bundled classpath resources. Unset (default) → use classpath (development and test).

| Env var | Default | Description |
|---|---|---|
| `ONNX_MODEL_PATH` | *(classpath)* | Absolute path to `mlp_embedding_model.onnx` on the filesystem |
| `ONNX_LOOKUPS_PATH` | *(classpath)* | Absolute path to `mlp_embedding_lookups.json` on the filesystem |

### In-memory cache (FeatureCache)

| Property | Env var | Default | Description |
|---|---|---|---|
| `recsys.cache.item-vector-max-size` | `RECSYS_ITEM_VECTOR_CACHE_SIZE` | `10000` | Maximum cached item vectors |
| `recsys.cache.item-vector-ttl-seconds` | `RECSYS_ITEM_VECTOR_TTL` | `300` | Item vector TTL (seconds); matches training job cadence |
| `recsys.cache.reward-max-size` | `RECSYS_REWARD_CACHE_SIZE` | `50000` | Maximum cached reward model stat entries |
| `recsys.cache.reward-ttl-seconds` | `RECSYS_REWARD_TTL` | `30` | Reward model stat TTL; invalidated immediately on `/feedback` writes |

### Recommendation properties

| Property | Default |
|---|---|
| `recsys.embeddings.item-prefix` | `i2vEmb` |
| `recsys.embeddings.user-prefix` | `uEmb` |
| `recsys.candidate-generation.popularity-fetch-multiplier` | `5` |
| `recsys.candidate-generation.cold-start-pool-size` | `25` |
| `recsys.candidate-generation.top-n-randomization-pool` | `5` |
| `recsys.filtering.enabled` | `true` |
| `recsys.filtering.blocked-users` | *(empty)* |
| `recsys.filtering.muted-product-types` | *(empty)* |
| `recsys.filtering.muted-genres` | *(empty)* |
| `recsys.filtering.muted-keywords` | *(empty)* |
| `recsys.bandit.algorithm` | `ucb` |
| `recsys.bandit.exploration-alpha` | `0.75` |
| `recsys.bandit.max-exploration-bonus` | `0.25` |
| `recsys.bandit.cold-start-exposure-threshold` | `5` |
| `recsys.bandit.cold-start-boost` | `1.35` |
| `recsys.bandit.relevance-weight` | `0.6` |
| `recsys.bandit.content-weight` | `0.25` |
| `recsys.bandit.popularity-weight` | `0.15` |
| `recsys.bandit.deep-learning-weight` | `0.0` |
| `recsys.bandit.q-learning-alpha` | `0.1` |
| `recsys.bandit.q-learning-gamma` | `0.9` |
| `recsys.bandit.q-learning-epsilon` | `0.1` |
| `recsys.replay-buffer.max-size` | `10000` |
| `recsys.replay-buffer.candidate-snapshot-size` | `20` |
| `recsys.reward-model.weight` | `0.25` |
| `recsys.reward-model.global-weight` | `0.15` |
| `recsys.reward-model.item-weight` | `0.45` |
| `recsys.reward-model.genre-weight` | `0.25` |
| `recsys.reward-model.tag-weight` | `0.15` |
| `recsys.reward-model.min-feature-count` | `3` |

Runtime overrides:

| Env var | Default |
|---|---|
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `SERVER_PORT` | `8080` |
| `ITEM_EMBEDDING_PREFIX` | `i2vEmb` |
| `USER_EMBEDDING_PREFIX` | `uEmb` |
| `RECSYS_POPULARITY_FETCH_MULTIPLIER` | `5` |
| `RECSYS_COLD_START_POOL_SIZE` | `25` |
| `RECSYS_RANDOMIZATION_POOL` | `5` |
| `RECSYS_BANDIT_ALGORITHM` | `ucb` |
| `RECSYS_EXPLORATION_ALPHA` | `0.75` |
| `RECSYS_MAX_EXPLORATION_BONUS` | `0.25` |
| `RECSYS_COLD_START_THRESHOLD` | `5` |
| `RECSYS_COLD_START_BOOST` | `1.35` |
| `RECSYS_RELEVANCE_WEIGHT` | `0.6` |
| `RECSYS_CONTENT_WEIGHT` | `0.25` |
| `RECSYS_POPULARITY_WEIGHT` | `0.15` |
| `RECSYS_DEEP_LEARNING_WEIGHT` | `0.0` |
| `RECSYS_REPLAY_BUFFER_MAX_SIZE` | `10000` |
| `RECSYS_REPLAY_CANDIDATE_SNAPSHOT_SIZE` | `20` |
| `RECSYS_REWARD_MODEL_WEIGHT` | `0.25` |
| `RECSYS_REWARD_GLOBAL_WEIGHT` | `0.15` |
| `RECSYS_REWARD_ITEM_WEIGHT` | `0.45` |
| `RECSYS_REWARD_GENRE_WEIGHT` | `0.25` |
| `RECSYS_REWARD_TAG_WEIGHT` | `0.15` |
| `RECSYS_REWARD_MIN_FEATURE_COUNT` | `3` |
| `RECSYS_ITEM_VECTOR_CACHE_SIZE` | `10000` |
| `RECSYS_ITEM_VECTOR_TTL` | `300` |
| `RECSYS_REWARD_CACHE_SIZE` | `50000` |
| `RECSYS_REWARD_TTL` | `30` |
| `ONNX_MODEL_PATH` | *(classpath fallback)* |
| `ONNX_LOOKUPS_PATH` | *(classpath fallback)* |

Bandit algorithm notes:

- `ucb` builds a Beta-smoothed posterior mean for each item, then adds a confidence term proportional to `sqrt(log(total_impressions) / pulls)`.
- `thompson` builds the same posterior and ranks items by a Beta posterior sample, which gives a concrete stochastic arm draw per request.
- `q-learning` stores tabular Q-values in Redis under `q-learning:q:{stateKey}` and updates them from feedback with `Q(s,a) += alpha * (reward + gamma * max_a Q(s',a) - Q(s,a))`.
- `sarsa` stores tabular Q-values under `sarsa:q:{stateKey}` and updates with the on-policy target `reward + gamma * Q(s', a')`, where `a'` is selected by the same epsilon-greedy policy used for serving.
- The `learnedPrior` fed to the bandit is a blend of `offlineScore` (static signals + ONNX) and `onlineScore` (real-time reward model), so bandit updates refine rather than replace the base ranker.
- Set `RECSYS_DEEP_LEARNING_WEIGHT` to a value between `0.0` and `1.0` to enable the offline ONNX model's contribution to `offlineScore`. The weights do not need to sum to `1.0`; scores are clamped to `[0, 1]` at each stage.
- Switch algorithms with `RECSYS_BANDIT_ALGORITHM=ucb`, `RECSYS_BANDIT_ALGORITHM=thompson`, `RECSYS_BANDIT_ALGORITHM=q-learning`, or `RECSYS_BANDIT_ALGORITHM=sarsa`.

### Realtime training write path

`/feedback` triggers online learning by updating reward statistics in Redis for the item, its genres and tags, and a global prior. Before pipelining, this was ~22 individual round-trips per feedback call. The current implementation collapses all writes into one:

```
GET  replay:pending:{user}:{item}           ← phase 1: read (before pipeline)
─────────────────────────────── pipeline ───────────────────────────────────
HINCRBY bandit:metrics clicks 1             ← if clicked
HINCRBY bandit:metrics:{algo} clicks 1
INCR    bandit:item:{item}:clicks
HINCRBY bandit:metrics reward_total {r}
HINCRBY bandit:metrics:{algo} reward_total {r}
HINCRBY reward-model:global count 1         ← online learning update
HINCRBY reward-model:global reward_total {r}
HINCRBY reward-model:item:{item} count 1
HINCRBY reward-model:item:{item} reward_total {r}
HINCRBY reward-model:genre:{g} ...          ← one pair per genre
HINCRBY reward-model:tag:{t} ...            ← one pair per tag
RPUSH   replay:recommendations {payload}    ← replay buffer
LTRIM   replay:recommendations -{max} -1
─────────────────────────────── flush ──────────────────────────────────────
featureCache.invalidateRewardStats(...)     ← phase 3: local cache purge
```

Replay and reward-model Redis keys:

| Key | Type | Contents |
|---|---|---|
| `replay:pending:{user}:{item}` | string | Last recommendation context for a served user-item pair |
| `replay:recommendations` | list | Rewarded replay events from `/feedback` |
| `reward-model:global` | hash | Global count and reward total |
| `reward-model:item:{item}` | hash | Per-item count and reward total |
| `reward-model:genre:{genre}` | hash | Per-genre count and reward total |
| `reward-model:tag:{tag}` | hash | Per-tag count and reward total |

## Offline Path

### `ItemSequencePreprocessingJob`

Builds time-ordered item sequences from ratings with `rating >= 3.5`.

```bash
spark-submit \
  --class com.demo.process.ItemSequencePreprocessingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  /tmp/spark-recsys/item-sequences
```

Environment variables:

| Env var | Description |
|---|---|
| `RATINGS_INPUT_PATH` | Ratings CSV input |
| `ITEM_SEQUENCES_OUTPUT_PATH` | Optional output path |

### `Item2VecTrainingJob`

Trains Spark MLlib `Word2Vec` on item sequences and writes item embeddings to a text file. It can also publish embeddings into Redis for the retrieval service.

```bash
spark-submit \
  --class com.demo.task.Item2VecTrainingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/embedding.txt \
  item_1
```

Key environment variables:

| Env var | Default |
|---|---|
| `RATINGS_INPUT_PATH` | required if arg omitted |
| `ITEM2VEC_EMBEDDING_PATH` | `recsys-pipeline/sampledata/embedding.txt` |
| `ITEM2VEC_QUERY_ITEM` | `592` |
| `ITEM2VEC_REDIS_KEY_PREFIX` | `i2vEmb` |
| `ITEM2VEC_REDIS_TTL_SECONDS` | `86400` |
| `ITEM2VEC_MIN_COUNT` | `1` |
| `ITEM2VEC_SAVE_TO_REDIS` | `false` |

Example: train and write embeddings to Redis:

```bash
ITEM2VEC_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
REDIS_PORT=6379 \
spark-submit \
  --class com.demo.task.Item2VecTrainingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/embedding.txt \
  item_1
```

Output format:

```text
item_1:0.0123 -0.4567 0.8910 ...
```

### `UserEmbeddingTrainingJob`

Builds a user embedding by averaging the vectors of positively rated items.

```bash
spark-submit \
  --class com.demo.task.UserEmbeddingTrainingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/item_embedding_sample.txt \
  sampledata/user_embedding.txt
```

Key environment variables:

| Env var | Default |
|---|---|
| `RATINGS_INPUT_PATH` | required if arg omitted |
| `ITEM2VEC_EMBEDDING_PATH` | required if arg omitted |
| `USER_EMBEDDING_OUTPUT_PATH` | `recsys-pipeline/sampledata/user_embedding.txt` |
| `USER_EMBEDDING_MIN_RATING` | `3.5` |

Output format:

```text
user_1:0.9 0.1 0.0
```

### `AlsEmbeddingTrainingJob`

Trains Spark ML `ALS` directly on ratings and writes latent user and item factors.

```bash
spark-submit \
  --class com.demo.task.AlsEmbeddingTrainingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/als
```

Key environment variables:

| Env var | Default |
|---|---|
| `RATINGS_INPUT_PATH` | required if arg omitted |
| `ALS_EMBEDDING_OUTPUT_PATH` | `recsys-pipeline/sampledata/als` |
| `ALS_RANK` | `16` |
| `ALS_MAX_ITER` | `10` |
| `ALS_REG_PARAM` | `0.1` |

Output paths:

```text
sampledata/als/userFactors/part-...
sampledata/als/itemFactors/part-...
```

### `EmbeddingCandidateGenerationJob`

Batch embedding-based candidate pre-computation. Loads pre-trained user and item embeddings (output of `UserEmbeddingTrainingJob` or `AlsEmbeddingTrainingJob`), broadcasts the full item catalog to every executor, and computes cosine similarity locally per user partition with no cross-join or shuffle. Writes top-K candidates per user to Parquet and optionally to Redis.

```bash
spark-submit \
  --class com.demo.recommend.EmbeddingCandidateGenerationJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/user_embedding.txt \
  sampledata/embedding.txt \
  sampledata/candidates
```

Key environment variables:

| Env var | Default |
|---|---|
| `USER_EMBEDDING_PATH` | required if arg omitted |
| `ITEM_EMBEDDING_PATH` | required if arg omitted |
| `CANDIDATE_OUTPUT_PATH` | optional Parquet output path |
| `CANDIDATE_TOP_K` | `100` |
| `CANDIDATE_SAVE_TO_REDIS` | `false` |
| `CANDIDATE_REDIS_KEY_PREFIX` | `user` (writes `user:{id}:candidates`) |
| `CANDIDATE_REDIS_TTL_SECONDS` | `86400` |

## Cold-Start RL Extension Plan

The current retrieval service is intentionally the right starting point for a cold-start recommendation project: it already has content-based retrieval, item embeddings, online feedback, and a UCB/Thompson-style exploration layer. The next steps should extend that path gradually rather than jumping straight to a full DQN ranker.

Recommended build order:

1. ✅ Strengthen the existing baseline *(implemented)*:
   - content-based retrieval from genres, tags, popularity, and embeddings
   - offline ONNX MLP score (`DeepLearningPredictionService`) blended via `deep-learning-weight`
   - online reward model (`OnlineLearningService`) updated from the feedback stream
   - UCB/Thompson bandit exploration for low-exposure and cold-start items
   - replay buffer storing `(user, context, candidates, action, reward, timestamp)`
2. Add the RL framing:
   - model recommendation as contextual Q-learning where the state is the user/session context, the action is the recommended item, and the reward comes from `/feedback`
   - keep the UCB ranker as the online-safe policy while the Q-value model is trained offline or in shadow mode
3. Scale with DQN ranking:
   - replace tabular Q-values with a neural scorer over user, item, and content embeddings
   - rank candidate items by predicted Q-value rather than treating the full catalog as the action space
   - train from the replay buffer with negative samples from exposed-but-unclicked items
4. Stabilize with Double DQN:
   - use the online network to choose the next best item
   - use the target network to evaluate that item
   - periodically update the target network to reduce overestimation and ranking churn
5. Improve cold-start sample efficiency with Dyna-style planning:
   - train a lightweight environment or reward model from observed feedback
   - generate simulated feedback for new or low-exposure items
   - mix real replay and simulated replay, with lower weight on simulated samples

Clean project story:

- Q-learning gives the recommendation loop an RL framing.
- DQN makes the ranking policy scalable with user and item embeddings.
- Double DQN makes the learned ranker more stable.
- Dyna-Q improves cold-start sample efficiency by learning from simulated feedback before enough real impressions arrive.

## Notes

- `docker-compose.yml` is explicitly for local development and runs Kafka and Redis without authentication.
- `run-streaming-job.sh` expects `services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar` to exist.
- The retrieval service can serve recommendations without embeddings, but the relevance component will be zero until vectors are loaded.
