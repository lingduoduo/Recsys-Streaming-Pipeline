# Spark Recsys

A two-path recommendation system demo:

- **Real-time retrieval** — Kafka click events → Spark Structured Streaming → Redis → Spring Boot REST API ([RecSys](https://github.com/lingduoduo/RecSys)).
- **Offline embeddings** — historical ratings → Item2Vec/item metadata/ALS embeddings → user and item vectors.

```
ratings.csv ──► ItemSequencePreprocessingJob ──► Item2VecTrainingJob ──► embedding.txt
      │                                                               │
      └────────────────────► UserEmbeddingTrainingJob ◄──────────────┘
                                      │
                                      ▼
                              user_embedding.txt

ratings.csv ──► AlsEmbeddingTrainingJob ──► als/userFactors + als/itemFactors

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

Prerequisites:

```bash
brew install sbt
```

Install Apache Spark 3.5.x and use Java 17. If Spark is installed outside `/Users/linghuang/opt/spark-3.5.1-bin-hadoop3`, set `SPARK_HOME`.

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
cd spark-streaming-job
sbt assembly
cd ..

spark-submit \
  --class com.demo.streaming.UserEventStreamingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

Or use the project launcher from `spark-recsys`:

```bash
./run-streaming-job.sh
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
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
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
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  spark-recsys/sampledata/ratings.csv \
  /tmp/spark-recsys/item-sequences
```

Environment variable form:

```bash
RATINGS_INPUT_PATH=spark-recsys/sampledata/ratings.csv \
ITEM_SEQUENCES_OUTPUT_PATH=/tmp/spark-recsys/item-sequences \
spark-submit --class com.demo.recsys.ItemSequencePreprocessingJob spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
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
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
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
spark-submit --class com.demo.recsys.Item2VecTrainingJob spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

Embedding output — one item per line:

```text
item_1:0.0123 -0.4567 0.8910 ...
```

### UserEmbeddingTrainingJob

Builds user embeddings by averaging the embeddings of items a user rated positively.

This keeps the user-vector path intentionally simple. Item embeddings can come from:

- Word2Vec / Item2Vec trained on watch or rating sequences.
- Content embeddings from title, genre, description, or other metadata.
- ALS or matrix factorization, which can directly learn both user and item latent vectors.

Steps:
1. Load `ratings.csv`.
2. Keep positive ratings (`rating >= 3.5` by default).
3. Load item embeddings in `movieId:v1 v2 ...` format.
4. Join ratings with item embeddings by `movieId`.
5. Group by `userId`.
6. Average the positive items' vectors to produce `userEmbedding`.

```bash
spark-submit \
  --class com.demo.recsys.UserEmbeddingTrainingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  spark-recsys/sampledata/ratings.csv \
  spark-recsys/sampledata/embedding.txt \
  spark-recsys/sampledata/user_embedding.txt
```

For a quick local demo without first running Item2Vec, use the sample item vectors:

```bash
spark-submit \
  --class com.demo.recsys.UserEmbeddingTrainingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  spark-recsys/sampledata/ratings.csv \
  spark-recsys/sampledata/item_embedding_sample.txt \
  spark-recsys/sampledata/user_embedding.txt
```

Environment variable form:

```bash
RATINGS_INPUT_PATH=spark-recsys/sampledata/ratings.csv \
ITEM2VEC_EMBEDDING_PATH=spark-recsys/sampledata/embedding.txt \
USER_EMBEDDING_OUTPUT_PATH=spark-recsys/sampledata/user_embedding.txt \
spark-submit --class com.demo.recsys.UserEmbeddingTrainingJob spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

User embedding output:

```text
user_1:0.9 0.1 0.0
```

### AlsEmbeddingTrainingJob

Trains Spark ML `ALS` directly on ratings and exports learned user and item latent factors.

ALS is the simplest path when you want Spark to learn both sides of the embedding space from collaborative behavior:

```scala
val als = new ALS()
  .setUserCol("userIdInt")
  .setItemCol("movieIdInt")
  .setRatingCol("rating")
  .setRank(16)
  .setMaxIter(10)
  .setRegParam(0.1)

val model = als.fit(ratingsDf)
val userFactors = model.userFactors
val itemFactors = model.itemFactors
```

The job maps string IDs like `user_1` and `item_1` to integer ALS IDs, trains the model, then maps factors back to the original IDs before writing output.

Default hyperparameters:

| Parameter | Default | Env var |
|---|---|---|
| `rank` | `16` | `ALS_RANK` |
| `maxIter` | `10` | `ALS_MAX_ITER` |
| `regParam` | `0.1` | `ALS_REG_PARAM` |

```bash
spark-submit \
  --class com.demo.recsys.AlsEmbeddingTrainingJob \
  spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  spark-recsys/sampledata/ratings.csv \
  spark-recsys/sampledata/als
```

Environment variable form:

```bash
RATINGS_INPUT_PATH=spark-recsys/sampledata/ratings.csv \
ALS_EMBEDDING_OUTPUT_PATH=spark-recsys/sampledata/als \
ALS_RANK=16 \
ALS_MAX_ITER=10 \
ALS_REG_PARAM=0.1 \
spark-submit --class com.demo.recsys.AlsEmbeddingTrainingJob spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

Output:

```text
spark-recsys/sampledata/als/userFactors/part-...
spark-recsys/sampledata/als/itemFactors/part-...
```

Each line uses the same text embedding shape as the other jobs:

```text
user_1:0.0123 -0.4567 0.8910 ...
item_1:0.1111 0.2222 0.3333 ...
```
