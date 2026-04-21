# Spark Recsys

A two-path recommendation system demo:

- **Real-time retrieval** — Kafka click events → Spark Structured Streaming → Redis → Spring Boot REST API.
- **Offline Item2Vec** — historical ratings → item sequences → Word2Vec embeddings.

```
ratings.csv ──► ItemSequencePreprocessingJob ──► Item2VecTrainingJob ──► embedding.txt

producer.py ──► Kafka (user_events)
                    │
                    ▼
          UserEventStreamingJob (Spark Streaming)
                    │
          ┌─────────┴──────────┐
          │ user:{id}:recent   │  global:item_popularity
          └─────────┬──────────┘
                    ▼
        GET /recommend/{user}  (Spring Boot)
```

## Quick Start

Start Kafka, Zookeeper, and Redis:

```bash
docker compose up -d
```

Send synthetic click events:

```bash
pip install kafka-python
python producer.py
```

Run the streaming job (separate terminal):

```bash
spark-submit \
  --class com.demo.streaming.UserEventStreamingJob \
  path/to/spark-recsys-job.jar
```

Start the retrieval service (separate terminal):

```bash
cd retrieval-service
mvn spring-boot:run
```

Query recommendations:

```bash
curl http://localhost:8080/recommend/user_1
curl http://localhost:8080/recommend/user_1?limit=10
```

## Real-Time Path

### producer.py

Publishes synthetic click events to the `user_events` Kafka topic.

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `user_events` | Topic to publish to |
| `EVENTS_PER_SECOND` | `1` | Publish rate |

Event schema:

```json
{"user_id": "user_1", "item_id": "item_3", "event_type": "click", "timestamp": 1713600001}
```

### UserEventStreamingJob

Spark Structured Streaming job that consumes `user_events` and writes to Redis.

For each micro-batch it:
1. Collects all items per user and increments per-item click counts.
2. Issues one `LPUSH` (multi-item) + one `LTRIM` per unique user.
3. Issues one `ZINCRBY` per unique item using the aggregated count.

Redis keys written:

| Key | Type | Contents |
|---|---|---|
| `user:{id}:recent` | list | Most-recent N clicked items (newest first) |
| `global:item_popularity` | sorted set | Cumulative click count per item |

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `KAFKA_TOPIC` | `user_events` | Topic to consume |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `RECENT_ITEMS_LIMIT` | `20` | Max items kept per user |
| `REDIS_PIPELINE_SIZE` | `500` | Commands per pipeline flush |
| `MAX_OFFSETS_PER_TRIGGER` | `5000` | Kafka offsets per trigger |
| `TRIGGER_INTERVAL` | `5 seconds` | Micro-batch interval |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/user-event-streaming-job` | Checkpoint path |

```bash
spark-submit \
  --class com.demo.streaming.UserEventStreamingJob \
  path/to/spark-recsys-job.jar
```

### Retrieval Service

Spring Boot service at port 8080.

**`GET /recommend/{user}?limit=6`**

Returns recent items the user clicked plus globally popular items (excluding recent ones).

```json
{
  "user": "user_1",
  "recent": ["item_7", "item_2"],
  "recommendations": ["item_5", "item_9", "item_1"]
}
```

| Parameter | Default | Max |
|---|---|---|
| `limit` | `6` | `50` |

Build and run:

```bash
cd retrieval-service
mvn spring-boot:run
```

| Env var | Default |
|---|---|
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `SERVER_PORT` | `8080` |

## Offline Path

### ItemSequencePreprocessingJob

Converts a ratings CSV into time-ordered item sequences per user.

Steps:
1. Load `ratings.csv`.
2. Keep positive interactions (`rating >= 3.5` by default).
3. Group by user and sort each user's items by `timestamp`.
4. Emit one space-separated sequence per user.

Example input → output:

```
userId,movieId,rating,timestamp        →   item_1 item_3
user_1,item_1,4.0,1713600001
user_1,item_3,5.0,1713600010
```

Output columns: `userId`, `movieIds`, `movieIdStr`

```bash
spark-submit \
  --class com.demo.recsys.ItemSequencePreprocessingJob \
  path/to/spark-recsys-job.jar \
  spark-recsys/sampledata/ratings.csv \
  /tmp/spark-recsys/item-sequences
```

Environment variable form:

```bash
RATINGS_INPUT_PATH=spark-recsys/sampledata/ratings.csv \
ITEM_SEQUENCES_OUTPUT_PATH=/tmp/spark-recsys/item-sequences \
spark-submit --class com.demo.recsys.ItemSequencePreprocessingJob path/to/spark-recsys-job.jar
```

### Item2VecTrainingJob

Trains Spark MLlib `Word2Vec` on sequences from `ItemSequencePreprocessingJob` and writes item embeddings to a text file.

Default hyperparameters:

| Parameter | Default |
|---|---|
| `vectorSize` | `10` |
| `windowSize` | `5` |
| `numIterations` | `10` |
| `numSynonyms` | `20` |

```bash
spark-submit \
  --class com.demo.recsys.Item2VecTrainingJob \
  path/to/spark-recsys-job.jar \
  spark-recsys/sampledata/ratings.csv \
  spark-recsys/sampledata/embedding.txt \
  item_1
```

Arguments: `<ratings-path> <embedding-output-path> <query-item>`

Environment variable form:

```bash
RATINGS_INPUT_PATH=spark-recsys/sampledata/ratings.csv \
ITEM2VEC_EMBEDDING_PATH=spark-recsys/sampledata/embedding.txt \
ITEM2VEC_QUERY_ITEM=item_1 \
spark-submit --class com.demo.recsys.Item2VecTrainingJob path/to/spark-recsys-job.jar
```

Embedding output — one item per line:

```text
item_1:0.0123 -0.4567 0.8910 ...
```
