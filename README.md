# Recsys Streaming Pipeline

A recommendation-system playground that combines streaming data pipelines, offline embedding jobs, online learning, and bandit-style RL ranking. The repo includes Spark-based production paths, Spark/Flink learning notes, Kafka/Redis infrastructure, and a Spring Boot retrieval service.

## Repository Structure

```
Recsys-Streaming-Pipeline/
├── README.md                          # This file
├── SPEC.md                            # Training-consolidation spec (offline/online unification)
├── docs/
│   └── superpowers/plans/             # Phased implementation plans
├── spark-analysis/                    # Spark/Flink concepts, user analysis, and ML pipelines
│   ├── README.md                      # spark-analysis overview
│   ├── spark_report.scala             # RDD, DataFrame, window functions, logistic regression pipeline
│   ├── spark_encoder.scala            # Active user feature engineering and binary classification
│   ├── spark_model.scala              # Extended encoder with search-activity features
│   ├── retention_label.scala          # BigQuery → Spark retention labeling (D1, D2, L7, WAU)
│   └── Algebird.md                    # Algebird sketch library notes
└── recsys-pipeline/                   # Streaming recommendation platform
    ├── README.md                      # recsys-pipeline overview
    ├── pom.xml                        # Maven build for the Scala/Java services
    ├── pytest.ini                     # Pytest config for integration tests
    ├── services/
    │   ├── spark-streaming-job/       # Scala/Spark: streaming ingestion, feature joins, offline embeddings
    │   ├── java-retrieval-service/    # Java/Spring Boot: ONNX scoring, bandit RL, REST API
    │   └── python-modeling/           # Python: event producer, two-tower training, ONNX export
    ├── integration-tests/             # Cross-service integration tests (pytest + shell)
    ├── scripts/
    │   └── install-cron.sh            # Installs scheduled retraining cron job
    ├── sampledata/                    # ratings.csv and sample embeddings
    ├── run-streaming-job.sh           # Submit Spark streaming job
    ├── run-offline-pipeline.sh        # Train Item2Vec embeddings
    ├── run-user-embedding-pipeline.sh # Train user embeddings
    ├── run-als-pipeline.sh            # Train ALS collaborative-filtering embeddings
    ├── run-retrain.sh                 # Full retrain: replay export → ALS → user emb → two-tower → hot-reload
    ├── recsys-streaming-pipeline.png  # Architecture diagram
    ├── recsys-streaming-pipeline.html # Interactive architecture diagram
    └── docker-compose.yml             # Local Kafka + Redis
```

---

## spark-analysis

Learning and production-grade stream-processing notes and Spark code covering core APIs, Flink comparisons, user behavior analysis, and binary classification.

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

## recsys-pipeline

A streaming recommendation platform: a real-time Kafka→Spark Streaming→Redis path for live user history, a Kafka→Spark online joiner and slate collector for training data, offline embedding trainers, and a Spring Boot retrieval service that combines an offline ONNX model, a real-time online learning reward model, and a UCB/Thompson bandit RL policy. Feature storage uses a three-tier design: offline files (ONNX model + Parquet training samples), Redis (real-time embeddings, counters, user history), and a Caffeine in-memory cache that collapses per-request Redis round-trips from O(N×features) to O(1).

### Architecture

```
Real-time path
──────────────
services/python-modeling/producer.py  ──Kafka──►  UserEventStreamingJob  ──Redis──►  services/java-retrieval-service
(user events)            (Structured Streaming)              (Spring Boot)

Training-data path
──────────────────
recsys_events    ──Kafka──► OnlineJoinerStreamingJob       ──Kafka/HDFS──► training_samples
training_samples ──Kafka──► ExperienceCollectorStreamingJob ──Kafka──►    training_experiences (slates)

Offline path
────────────
ratings CSV  ──►  ItemSequencePreprocessingJob  ──►  Item2VecTrainingJob
                  (item sequences per user)           (Word2Vec embeddings → Redis / file)
```

### Components

| Component | Language | Description |
|-----------|----------|-------------|
| `services/spark-streaming-job` | Scala / Spark | Consumes Kafka events; writes user histories and item popularity to Redis |
| `services/spark-streaming-job` | Scala / Spark | Joins the `recsys_events` stream into feature+label samples and reconstructs request-level slates |
| `services/spark-streaming-job` | Scala / Spark | Trains Item2Vec embeddings from rating sequences; stores to Redis with TTL |
| `services/java-retrieval-service` | Java / Spring Boot | REST API serving hybrid recommendations with offline, online, and RL signals |
| `services/python-modeling/producer.py` | Python | Kafka producer that generates synthetic user–item events |

### Quick Start

```bash
# 1. Start infrastructure (requires a running Docker daemon: Docker Desktop, or `colima start`)
cd recsys-pipeline
docker compose up -d          # Kafka + Redis (+ Zookeeper)
docker compose ps             # wait until kafka/redis are healthy

# 2. Run producer (install deps first)
python -m pip install -r services/python-modeling/requirements.txt
python services/python-modeling/producer.py

# 3. Build the Spark job jar, then submit it
(cd services/spark-streaming-job && sbt assembly)
spark-submit \
  --class com.demo.task.UserEventStreamingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar

# 4. Train Item2Vec embeddings
spark-submit \
  --class com.demo.task.Item2VecTrainingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv

# 5. Start retrieval service
cd services/java-retrieval-service && mvn spring-boot:run
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

See [recsys-pipeline/README.md](recsys-pipeline/README.md) for full configuration, environment variable reference, and architecture details.

---

## Requirements

- Java 17, Scala 2.12
- Apache Spark 3.5.x
- Apache Kafka 3.x
- Redis 7+
- Maven 3.8+ (retrieval service)
- Docker / Docker Compose (local infrastructure)

> **Note:** This repository is for learning and demonstration purposes. See individual sub-project READMEs for production configuration guidance.
