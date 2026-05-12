# Spark Recsys

`spark-recsys` is a small recommendation-system playground with three complementary paths:

- A real-time Kafka -> Spark Structured Streaming -> Redis pipeline that keeps recent user history and global popularity fresh.
- A training-data pipeline that joins behavior logs into feature+label samples, writes them to Kafka and HDFS-compatible storage, then rebuilds request-level slates for online learning.
- An offline Spark training flow that builds Item2Vec, user, and ALS embeddings from historical ratings.

The retrieval layer is a Spring Boot service that combines those signals with content-based retrieval, a lightweight UCB/Thompson bandit policy, a Redis replay buffer, a simple reward model, cold-start candidate generation, and feedback tracking.

## Architecture

```text
producer.py -> Kafka topic: user_events -> UserEventStreamingJob -> Redis
                                                          |-> user:{id}:recent
                                                          |-> global:item_popularity

producer.py -> Kafka topic: behavior_logs -> OnlineJoinerStreamingJob
                                      |-> Kafka topic: training_samples
                                      |-> Parquet path: /tmp/spark-recsys/training-samples

Kafka topic: training_samples -> ExperienceCollectorStreamingJob
                              -> Kafka topic: training_experiences

ratings.csv -> ItemSequencePreprocessingJob -> Item2VecTrainingJob -> embedding.txt
                                                                   -> Redis i2vEmb:{item}

ratings.csv + embedding.txt -> UserEmbeddingTrainingJob -> user_embedding.txt
ratings.csv -> AlsEmbeddingTrainingJob -> als/userFactors + als/itemFactors

Redis + catalog config -> retrieval-service -> /recommend/{user}
                                           -> /feedback
                                           -> /metrics
                                           -> /embedding/{item}
```

## What The Retrieval Service Does

For each recommendation request, the service:

1. Loads the user's recent click history from Redis.
2. Pulls popular items from `global:item_popularity`.
3. Generates extra cold-start candidates from the configured catalog.
4. Scores candidates with a weighted blend of:
   - embedding relevance
   - content similarity from genres and tags
   - popularity
   - learned reward estimates from item, genre, tag, and global feedback
   - exploration bonus for low-exposure items via `ucb` or `thompson`
5. Randomizes the top scoring pool slightly to avoid deterministic repetition.
6. Stores pending recommendation context for replay-buffer training.
7. Tracks impressions, clicks, regret-style metrics, novelty, and catalog coverage.

The default catalog and ranking weights live in `retrieval-service/src/main/resources/application.yml`.

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
cd spark-streaming-job
sbt assembly
cd ..
```

Start Kafka, Zookeeper, and Redis:

```bash
docker compose up -d
```

Start the clickstream producer:

```bash
pip install kafka-python
python producer.py
```

Run the streaming job:

```bash
./run-streaming-job.sh
```

Or run it directly with `spark-submit`:

```bash
spark-submit \
  --class com.demo.streaming.UserEventStreamingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

Start the retrieval service:

```bash
cd retrieval-service
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

### `POST /feedback`

Records user feedback for an exposed item and updates aggregate metrics.
It also appends a rewarded recommendation event to the Redis replay buffer and updates the online reward-model counters.

Example:

```bash
curl -X POST http://localhost:8080/feedback \
  -H 'Content-Type: application/json' \
  -d '{"item":"item_5","clicked":true,"reward":1.0}'
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

### `producer.py`

Publishes synthetic click events to Kafka topic `user_events`.

Environment variables:

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `KAFKA_TOPIC` | `user_events` | Topic to publish to |
| `PRODUCER_MODE` | `clickstream` | `clickstream` emits simple clicks; `behavior` emits impressions plus click/order feedback |
| `EVENTS_PER_SECOND` | `1` | Event rate |
| `NUM_USERS` | `5` | Size of synthetic user pool |
| `NUM_ITEMS` | `10` | Size of synthetic item pool |
| `SLATE_SIZE` | `5` | Items per recommendation request in behavior mode |

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

Consumes click events from Kafka and writes Redis state used by the retrieval service.

For each micro-batch it:

1. Aggregates clicked items per user.
2. Writes batched `LPUSH` plus `LTRIM` updates to `user:{id}:recent`.
3. Aggregates per-item click counts.
4. Writes `ZINCRBY` updates to `global:item_popularity`.

Redis keys written:

| Key | Type | Contents |
|---|---|---|
| `user:{id}:recent` | list | Most recent clicked items, newest first |
| `global:item_popularity` | sorted set | Global click counts |

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
| `REDIS_PIPELINE_SIZE` | `500` |
| `MAX_OFFSETS_PER_TRIGGER` | `5000` |
| `TRIGGER_INTERVAL` | `5 seconds` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/user-event-streaming-job` |

### `OnlineJoinerStreamingJob`

Consumes Kafka behavior logs and turns recommendation impressions plus later feedback into training samples with `features + label`.

```bash
PRODUCER_MODE=behavior KAFKA_TOPIC=behavior_logs python producer.py

SPARK_MAIN_CLASS=com.demo.streaming.OnlineJoinerStreamingJob \
ONLINE_JOINER_INPUT_TOPIC=behavior_logs \
ONLINE_JOINER_OUTPUT_TOPIC=training_samples \
ONLINE_JOINER_HDFS_OUTPUT_PATH=/tmp/spark-recsys/training-samples \
./run-streaming-job.sh
```

For each micro-batch it:

1. Keeps `impression` / `exposure` rows as candidate training examples.
2. Aggregates `click`, `order`, and `purchase` feedback by `request_id + user_id + item_id`.
3. Produces one sample per exposed item with `clicked`, `ordered`, and numeric `label` (`0.0`, `1.0`, `2.0`).
4. Writes samples to Kafka for online model updates and to Parquet for HDFS-style offline training storage.

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
SPARK_MAIN_CLASS=com.demo.streaming.ExperienceCollectorStreamingJob \
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

## Retrieval Service Configuration

`retrieval-service/src/main/resources/application.yml` defines Redis connectivity plus recommendation settings under `recsys`.

Important properties:

| Property | Default |
|---|---|
| `recsys.embeddings.item-prefix` | `i2vEmb` |
| `recsys.embeddings.user-prefix` | `uEmb` |
| `recsys.candidate-generation.popularity-fetch-multiplier` | `5` |
| `recsys.candidate-generation.cold-start-pool-size` | `25` |
| `recsys.candidate-generation.top-n-randomization-pool` | `5` |
| `recsys.bandit.algorithm` | `ucb` |
| `recsys.bandit.exploration-alpha` | `0.75` |
| `recsys.bandit.max-exploration-bonus` | `0.25` |
| `recsys.bandit.cold-start-exposure-threshold` | `5` |
| `recsys.bandit.cold-start-boost` | `1.35` |
| `recsys.bandit.relevance-weight` | `0.6` |
| `recsys.bandit.content-weight` | `0.25` |
| `recsys.bandit.popularity-weight` | `0.15` |
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
| `RECSYS_REPLAY_BUFFER_MAX_SIZE` | `10000` |
| `RECSYS_REPLAY_CANDIDATE_SNAPSHOT_SIZE` | `20` |
| `RECSYS_REWARD_MODEL_WEIGHT` | `0.25` |
| `RECSYS_REWARD_GLOBAL_WEIGHT` | `0.15` |
| `RECSYS_REWARD_ITEM_WEIGHT` | `0.45` |
| `RECSYS_REWARD_GENRE_WEIGHT` | `0.25` |
| `RECSYS_REWARD_TAG_WEIGHT` | `0.15` |
| `RECSYS_REWARD_MIN_FEATURE_COUNT` | `3` |

Bandit algorithm notes:

- `ucb` builds a Beta-smoothed posterior mean for each item, then adds a confidence term proportional to `sqrt(log(total_impressions) / pulls)`.
- `thompson` builds the same posterior and ranks items by a Beta posterior sample, which gives a concrete stochastic arm draw per request.
- The recommender's relevance, content, popularity, and learned reward-model blend acts as the prior mean for both algorithms, so bandit updates refine rather than replace the base ranker.
- Switch algorithms with `RECSYS_BANDIT_ALGORITHM=ucb` or `RECSYS_BANDIT_ALGORITHM=thompson`.

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
  --class com.demo.recsys.ItemSequencePreprocessingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
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
  --class com.demo.recsys.Item2VecTrainingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/embedding.txt \
  item_1
```

Key environment variables:

| Env var | Default |
|---|---|
| `RATINGS_INPUT_PATH` | required if arg omitted |
| `ITEM2VEC_EMBEDDING_PATH` | `spark-recsys/sampledata/embedding.txt` |
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
  --class com.demo.recsys.Item2VecTrainingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
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
  --class com.demo.recsys.UserEmbeddingTrainingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/item_embedding_sample.txt \
  sampledata/user_embedding.txt
```

Key environment variables:

| Env var | Default |
|---|---|
| `RATINGS_INPUT_PATH` | required if arg omitted |
| `ITEM2VEC_EMBEDDING_PATH` | required if arg omitted |
| `USER_EMBEDDING_OUTPUT_PATH` | `spark-recsys/sampledata/user_embedding.txt` |
| `USER_EMBEDDING_MIN_RATING` | `3.5` |

Output format:

```text
user_1:0.9 0.1 0.0
```

### `AlsEmbeddingTrainingJob`

Trains Spark ML `ALS` directly on ratings and writes latent user and item factors.

```bash
spark-submit \
  --class com.demo.recsys.AlsEmbeddingTrainingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/als
```

Key environment variables:

| Env var | Default |
|---|---|
| `RATINGS_INPUT_PATH` | required if arg omitted |
| `ALS_EMBEDDING_OUTPUT_PATH` | `spark-recsys/sampledata/als` |
| `ALS_RANK` | `16` |
| `ALS_MAX_ITER` | `10` |
| `ALS_REG_PARAM` | `0.1` |

Output paths:

```text
sampledata/als/userFactors/part-...
sampledata/als/itemFactors/part-...
```

## Cold-Start RL Extension Plan

The current retrieval service is intentionally the right starting point for a cold-start recommendation project: it already has content-based retrieval, item embeddings, online feedback, and a UCB/Thompson-style exploration layer. The next steps should extend that path gradually rather than jumping straight to a full DQN ranker.

Recommended build order:

1. Strengthen the existing baseline:
   - content-based retrieval from genres, tags, popularity, and embeddings
   - UCB bandit exploration for low-exposure and cold-start items
   - replay buffer storing `(user, context, candidates, action, reward, timestamp)`
   - reward model that predicts click or normalized reward from user, item, and context features
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
- `run-streaming-job.sh` expects `spark-streaming-job/target/scala-2.12/spark-recsys-job.jar` to exist.
- The retrieval service can serve recommendations without embeddings, but the relevance component will be zero until vectors are loaded.
