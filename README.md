# Spark with Scala

A collection of Apache Spark projects in Scala demonstrating data analysis, machine learning pipelines, and real-time recommendation systems.

## Repository Structure

```
Spark-with-Scala/
├── spark-analysis/          # Spark fundamentals, user analysis, and ML pipelines
└── spark-recsys/            # Real-time recommendation system (streaming + retrieval)
```

---

## spark-analysis

Learning and production-grade Spark code covering core APIs, user behavior analysis, and binary classification.

### Files

| File | Object | Description |
|------|--------|-------------|
| `spark_report.scala` | `ScalaBasics` | Scala and Spark RDD fundamentals: collections, pair RDDs, `reduceByKey`, `groupByKey` |
| `spark_report.scala` | `SparkDataFrameBasics` | DataFrame and Dataset APIs: Spark SQL, joins, window functions, Hive integration |
| `spark_report.scala` | `UserSuspensionReport` | User suspension analysis: groupBy/pivot breakdowns by geo, email domain, device type |
| `spark_report.scala` | `ActiveUserSuspensionModel` | Logistic regression pipeline (StringIndexer → OneHotEncoder → VectorAssembler → LR) |
| `spark_report.scala` | `ContentClassificationReport` | Content classification metrics: confidence bucketing, appeal and post breakdowns |
| `spark_encoder.scala` | `ActiveUsersJob` | Active user feature engineering and binary classification per device type |
| `spark_model.scala` | `ActiveUsersJob` | Extended version of spark_encoder with additional search-activity features |
| `retention_label.scala` | `AdjustUserRetentionDataJob` | BigQuery → Spark retention labeling: D1, D2, L7, WAU flags from Adjust acquisition data |

### Key Techniques

- **Catalyst-transparent transforms**: `isin`/`when`/`otherwise` instead of UDFs for geo and email domain bucketing
- **DataFrame caching**: `cache()`/`unpersist()` for multi-scan reuse across device types and retention windows
- **ML Pipeline**: end-to-end `Pipeline` with `StringIndexer`, `OneHotEncoder`, `VectorAssembler`, and `LogisticRegression`
- **BigQuery integration**: `spark-bigquery` connector with `WRITE_EMPTY` disposition and `CREATE_IF_NEEDED`
- **Structured Streaming concepts**: documented in `README.md` (Kafka, Spark Streaming, Flink, Druid)

### Running

```bash
# Submit any job via spark-submit
spark-submit --class com.demo.analysis.ActiveUsersJob \
  --master yarn target/spark-analysis.jar

# AdjustUserRetentionDataJob requires --date and --outputBq
spark-submit --class com.demo.analysis.AdjustUserRetentionDataJob \
  target/spark-analysis.jar \
  --date 20240101 \
  --outputBq myproject.dataset.retention_labels
```

---

## spark-recsys

A three-path recommendation system: a real-time Kafka→Spark Streaming→Redis pipeline for live user history, a Kafka→Spark online joiner and slate collector for training data, and offline embedding trainers with a Spring Boot retrieval service that combines an offline ONNX model, a real-time online learning reward model, and a UCB/Thompson bandit RL policy.

### Architecture

```
Real-time path
──────────────
producer.py  ──Kafka──►  UserEventStreamingJob  ──Redis──►  retrieval-service
(user events)            (Structured Streaming)              (Spring Boot)

Training-data path
──────────────────
behavior logs ──Kafka──► OnlineJoinerStreamingJob ──Kafka/HDFS──► training samples
training samples ──Kafka──► ExperienceCollectorStreamingJob ──Kafka──► slates

Offline path
────────────
ratings CSV  ──►  ItemSequencePreprocessingJob  ──►  Item2VecTrainingJob
                  (item sequences per user)           (Word2Vec embeddings → Redis / file)
```

### Components

| Component | Language | Description |
|-----------|----------|-------------|
| `spark-streaming-job` | Scala / Spark | Consumes Kafka events; writes user histories and item popularity to Redis |
| `spark-streaming-job` | Scala / Spark | Joins behavior logs into feature+label samples and reconstructs request-level slates |
| `spark-streaming-job` | Scala / Spark | Trains Item2Vec embeddings from rating sequences; stores to Redis with TTL |
| `retrieval-service` | Java / Spring Boot | REST API serving recommendations and Item2Vec embeddings from Redis |
| `producer.py` | Python | Kafka producer that generates synthetic user–item events |

### Quick Start

```bash
# 1. Start infrastructure
docker-compose up -d          # Kafka + Redis

# 2. Run producer
python spark-recsys/producer/producer.py

# 3. Submit streaming job
spark-submit \
  --class com.demo.streaming.UserEventStreamingJob \
  spark-recsys/spark-streaming-job/target/spark-streaming-job.jar

# 4. Train Item2Vec embeddings
spark-submit \
  --class com.demo.recsys.Item2VecTrainingJob \
  spark-recsys/spark-streaming-job/target/spark-streaming-job.jar \
  spark-recsys/sampledata/ratings.csv

# 5. Start retrieval service
cd spark-recsys/retrieval-service && mvn spring-boot:run
```

### Key Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/recommend/{user}?limit=6` | Ranked recommendations with per-item diagnostics and request metrics |
| `GET` | `/predict/{user}/{item}` | Offline ONNX model score for a (user, item) pair |
| `GET` | `/predict/id?userId=&itemId=` | Same as above using raw integer lookup IDs |
| `GET` | `/predict/metadata` | Loaded model name, lookup table sizes, and ONNX input/output names |
| `POST` | `/feedback` | Record a click/reward signal; triggers online learning and bandit updates |
| `GET` | `/metrics` | Aggregate bandit metrics (CTR, regret, novelty, coverage) per algorithm |
| `GET` | `/embedding/{item}` | Item2Vec embedding vector for an item |

See [spark-recsys/README.md](spark-recsys/README.md) for full configuration, environment variable reference, and architecture details.

---

## Requirements

- Java 11+, Scala 2.12
- Apache Spark 3.x
- Apache Kafka 3.x
- Redis 6+
- Maven 3.8+ (retrieval service)

> **Note:** This repository is for learning and demonstration purposes. See individual sub-project READMEs for production configuration guidance.
