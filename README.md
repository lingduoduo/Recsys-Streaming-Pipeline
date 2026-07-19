# Recsys Streaming Pipeline

A recommendation-system playground that combines streaming data pipelines, offline embedding jobs, online learning, and bandit-style RL ranking. The repo includes Spark-based production paths, Spark/Flink learning notes, Kafka/Redis infrastructure, and a Spring Boot retrieval service.

## Repository Structure

```
Recsys-Streaming-Pipeline/
├── README.md                          # This file
├── docs/                              # Dev docs — specs, plans, notes (see docs/README.md)
│   ├── specs/                         # Training-consolidation + data-pipeline specs
│   ├── plans/                         # Phased implementation plans
│   └── notes/                         # Research findings
├── spark-analysis/                    # Spark/Flink concepts, user analysis, and ML pipelines
│   ├── README.md                      # spark-analysis overview
│   ├── spark_report.scala             # RDD, DataFrame, window functions, logistic regression pipeline
│   ├── spark_encoder.scala            # Active user feature engineering and binary classification
│   ├── spark_model.scala              # Extended encoder with search-activity features
│   ├── retention_label.scala          # BigQuery → Spark retention labeling (D1, D2, L7, WAU)
│   └── Algebird.md                    # Algebird sketch library notes
├── recsys-pipeline/                   # Streaming recommendation platform
    ├── README.md                      # recsys-pipeline overview
    ├── pom.xml                        # Maven build for the Scala/Java services
    ├── pytest.ini                     # Pytest config for integration tests
    ├── services/
    │   ├── spark-streaming-job/       # Scala/Spark: streaming ingestion, feature joins, offline embeddings
    │   ├── java-retrieval-service/    # Java/Spring Boot: ONNX scoring, bandit RL, REST API, offline MDP policy eval
    │   └── python-modeling/           # Python: event producer, two-tower training, ONNX export, off-policy eval
    ├── integration-tests/             # Cross-service integration tests (pytest + shell)
    ├── scripts/
    │   └── install-cron.sh            # Installs scheduled retraining cron job
    ├── sampledata/                     # ratings.csv, catalog.json, sample embeddings
    ├── run-streaming-job.sh            # Submit a single Spark streaming job (SPARK_MAIN_CLASS)
    ├── run-data-pipeline.sh            # Launch all core streaming jobs together
    ├── run-offline-pipeline.sh         # Train Item2Vec embeddings
    ├── run-user-embedding-pipeline.sh  # Train user embeddings
    ├── run-als-pipeline.sh             # Train ALS collaborative-filtering embeddings
    ├── run-retrain.sh                  # Full retrain: replay export → ALS → user emb → two-tower → hot-reload
    ├── run-engagement-sim.sh           # E2E sim: engagement CTR time-series (+ report)
    ├── run-movielens-segment-sim.sh    # E2E sim: engagement by user segment (+ report)
    ├── run-movie-category-sim.sh       # E2E sim: engagement by movie category l1/l2/l3 (+ report)
    ├── recsys-streaming-pipeline.png   # Architecture diagram
    ├── recsys-streaming-pipeline.html  # Interactive architecture diagram
    └── docker-compose.yml              # Local Kafka + Redis
└── frontend/                          # Next.js app: React rendering of the analysis dashboard
    ├── app/ · components/             # App-router pages + dashboard section components
    ├── data/dashboard.json            # Committed snapshot (regenerate via export_dashboard_json.py)
    └── export_dashboard_json.py       # Dumps compute_* output → dashboard.json
```

---

## spark-analysis

Stream-processing notes and Spark code — from learning fundamentals to production-grade jobs — covering core APIs, Flink comparisons, user-behavior analysis, and binary classification.

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
Serving path
────────────
Producer (Python)                      producer.py → synthetic user–item events
    │
    ▼
Kafka: recsys_events                   one stream, two consumers ↓ (serving) + ↓ (training, below)
    │
    ▼
UserEventStreamingJob (Spark)
    │  click events → Redis
    │    user:{id}:recent         (LPUSH + LTRIM)
    │    global:item_popularity   (ZINCRBY)
    ▼
Redis :6379                            three-tier feature store — also holds offline embeddings + reward stats
    │  history · popularity · embeddings · reward stats
    ▼
Java Retrieval Service (Spring Boot :8080)
    GET /recommend/{user}   ·   POST /feedback → reward update (→ Redis)
    │
    ├── HybridRecommendationService    embedding cosine + content overlap + popularity + bandit
    ├── OnlineLearningService          Redis reward stats · UCB / Thompson / Q-learning / SARSA
    ├── DeepLearningPredictionService  mlp_embedding_model.onnx (classpath; blend weight 0.0 by default, opt-in)
    └── TwoTowerPredictionService      movielens_*_tower.onnx (opt-in: set ONNX_*_TOWER_PATH)

Offline modeling (Spark — manually triggered)          writes embeddings → Redis (read by the serving path above)
──────────────────────────────────────────────
ratings.csv
    ├── Item2VecTrainingJob        → Redis i2vEmb:{movieId} (optional) + sampledata/item_embedding.txt
    ├── AlsEmbeddingTrainingJob    → Redis alsItemEmb:{movieId}, alsUserEmb:{userId}
    └── UserEmbeddingTrainingJob   → Redis uEmb:{userId}   (needs item_embedding.txt)

ratings.csv (+ replay CSV)
    └── movielens_pipeline.py      train_two_tower() + train_ranking()
            → sampledata/movielens_{user,item}_tower.onnx, movielens_ranking.onnx
            → Redis embeddings (--save-embeddings-to-redis)

Training-data pipeline (Spark streaming)               second consumer of recsys_events (above)
────────────────────────────────────────
Kafka: recsys_events
    └── OnlineJoinerStreamingJob
            → Kafka: training_samples                     (feature+label rows)
            → HDFS: /tmp/spark-recsys/training-samples/   (parquet archive)

Kafka: training_samples
    ├── ExperienceCollectorStreamingJob → Kafka: training_experiences (request slates)
    ├── RecallSampleStreamingJob        → Kafka: recall_samples
    ├── RankingSampleStreamingJob       → Kafka: ranking_samples
    └── RelevanceSampleStreamingJob     → Kafka: relevance_samples
```

### Components

| Component | Language | Pipeline | Description |
|-----------|----------|----------|-------------|
| `services/python-modeling/producer.py` | Python | Data | Kafka producer emitting synthetic user–item events to `recsys_events` |
| `spark-streaming-job` · `UserEventStreamingJob` | Scala / Spark | Data | Consumes `recsys_events`; writes user history + item popularity to Redis |
| `spark-streaming-job` · `OnlineJoinerStreamingJob` | Scala / Spark | Data | Joins `recsys_events` into feature+label training samples |
| `spark-streaming-job` · `ExperienceCollectorStreamingJob` | Scala / Spark | Data | Reconstructs request-level slates (`training_experiences`) |
| `spark-streaming-job` · Item2Vec / ALS / UserEmbedding jobs | Scala / Spark | Modeling | Train item/user embeddings from rating sequences |
| `services/python-modeling/movielens_pipeline.py` | Python | Modeling | Two-tower training + ONNX export |
| `services/java-retrieval-service` | Java / Spring Boot | Experiment | REST API serving hybrid recommendations + bandit RL |

The platform runs as **three pipelines**. On first setup, run them top to bottom; each section below is titled with the **service and port** it owns. Unless noted, commands run from `recsys-pipeline/`.

---

### Infrastructure — Kafka `:9092` · Zookeeper `:2181` · Redis `:6379`

Requires a running Docker daemon (Docker Desktop, or `colima start`).

```bash
cd recsys-pipeline
docker compose up -d     # Kafka (:9092, :29092), Zookeeper (:2181), Redis (:6379)
docker compose ps        # wait until kafka/redis report "healthy"
```

Topics (`recsys_events`, `training_samples`, `training_experiences`, and the derived
`recall_samples` / `ranking_samples` / `relevance_samples`) auto-create on first use.

---

### 1. Data Pipeline — Kafka `:9092` → Redis `:6379`

Generates events and turns them into training data. No HTTP surface.

```bash
# One-time setup: Python deps + build the Spark fat jar
python -m pip install -r services/python-modeling/requirements.txt
(cd services/spark-streaming-job && sbt assembly)

# Emit synthetic user–item events to the recsys_events topic (:9092)
python services/python-modeling/producer.py

# Ingest events → user history + item popularity in Redis (:6379)
./run-streaming-job.sh                                                       # UserEventStreamingJob (default)

# Join events → feature+label training samples (Kafka + HDFS/parquet)
SPARK_MAIN_CLASS=com.demo.process.OnlineJoinerStreamingJob ./run-streaming-job.sh

# Collect samples → request-level slates (training_experiences topic)
SPARK_MAIN_CLASS=com.demo.process.ExperienceCollectorStreamingJob ./run-streaming-job.sh

# Derive ML datasets from training_samples → new *_samples topics
# (ranking/relevance also read Redis embeddings / movie features)
SPARK_MAIN_CLASS=com.demo.process.RecallSampleStreamingJob    ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.RankingSampleStreamingJob   ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.RelevanceSampleStreamingJob ./run-streaming-job.sh

# Or launch the core streaming jobs together
./run-data-pipeline.sh
```

> End-to-end **simulation harnesses** drive this whole pipeline and emit analysis reports:
> `./run-engagement-sim.sh`, `./run-movielens-segment-sim.sh`, `./run-movie-category-sim.sh`
> (see the recsys-pipeline README → *Simulation Harnesses*).

**Analytics dashboard** — turn any run's `training_samples` Parquet (from a sim or the joiner) into
one self-contained HTML page covering keyword / query / relevance / recall / ranking plus the two
offline policy evaluations below (off-policy + MDP):

```bash
IN=/tmp/spark-recsys/movie-category-sim/training-samples   # any run's training_samples
REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py --input "$IN" \
  --mdp-csv "$IN/../mdp_eval.csv"          # optional: render the MDP card from the Java CLI's CSV
# → writes $IN/../report-dashboard/index.html   (open in a browser)
```

The recall/ranking sections need Redis embeddings (`movie:*:features`, `i2vEmb` / `uEmb`); without
them they render an explicit N/A card. The `run-movie-category-sim.sh` harness now generates those
embeddings + item popularity itself, so its dashboard renders real recall/ranking numbers. The
**off-policy card** is computed live from the Redis replay buffer (same Direct-Method logic as
`ope_eval_report.py`); the **MDP card** renders from a `mdp_eval.csv` if one is present (default
`<input>/../mdp_eval.csv`, or `--mdp-csv`). Each falls back to an N/A card when its data is absent.
See the recsys-pipeline README → *Analysis Reports* for the five individual per-report scripts, and
*Offline policy evaluation* above for the two evaluators.

---

### 2. Modeling Pipeline — Spark embeddings + Python two-tower → ONNX

Trains embeddings offline and produces the ONNX model the retrieval service serves.

```bash
# Item2Vec item embeddings (writes sampledata/item_embedding.txt + Redis)
RATINGS_INPUT_PATH=sampledata/ratings.csv ./run-offline-pipeline.sh

# ALS collaborative-filtering embeddings
RATINGS_INPUT_PATH=sampledata/ratings.csv ./run-als-pipeline.sh

# User embeddings (needs the item embedding file from the offline step)
RATINGS_INPUT_PATH=sampledata/ratings.csv \
  ITEM2VEC_EMBEDDING_PATH=sampledata/item_embedding.txt \
  ./run-user-embedding-pipeline.sh

# Export the Redis replay buffer back to a ratings CSV (for retraining)
python services/python-modeling/replay_export.py

# Full retrain: replay export → ALS → user emb → two-tower → hot-reload
./run-retrain.sh                       # flags: --skip-spark --skip-python --skip-reload; DRY_RUN=1
```

The retrain's final step hot-reloads the ONNX model in the running service (`:8080`):

```bash
curl -X POST http://localhost:8080/actuator/model-reload
```

---

### 3. Experiment Pipeline — Retrieval service `:8080`

Serves recommendations and runs online learning + UCB/Thompson bandit RL.

```bash
# Start the service (binds :8080; connects to Redis :6379)
cd services/java-retrieval-service && mvn spring-boot:run
```

```bash
# Ranked recommendations with per-item diagnostics + request metrics
curl 'http://localhost:8080/recommend/u_1?limit=6'

# Offline ONNX score for a (user, item) pair
curl 'http://localhost:8080/predict/u_1/movie_42'

# Same prediction using raw integer lookup IDs
curl 'http://localhost:8080/predict/id?userId=1&itemId=42'

# Loaded model name, lookup-table sizes, ONNX input/output names
curl http://localhost:8080/predict/metadata

# Item2Vec embedding vector for an item
curl http://localhost:8080/embedding/movie_42

# Record a click/reward → triggers online learning + bandit update
curl -X POST http://localhost:8080/feedback \
  -H 'Content-Type: application/json' \
  -d '{"user":"u_1","item":"movie_42","clicked":true,"reward":1.0}'

# Aggregate bandit metrics (CTR, regret, novelty, coverage) per algorithm
curl http://localhost:8080/metrics
```

---

### Offline policy evaluation

Two standalone, honest offline evaluators that compare policies *without* touching the serving
path — replacing the service's self-referential vanity metrics.

```bash
# Bandit off-policy evaluation (Direct Method) from the rl_experience replay buffer.
# Fits a numpy logistic reward model on logged (taken-action features → observed reward),
# then re-picks each event's slate under logging / popularity / ctr / model:* / random
# policies. Reports value, lift-vs-logging, and 95% bootstrap CIs, plus estimator AUC/MSE.
REDIS_HOST=localhost python services/python-modeling/ope_eval_report.py --output ope_eval.csv

# Offline MovieLens MDP: uniform-random vs greedy leave-one-user-out movie-score policy over
# seeded finite-horizon episodes. Reports mean discounted return, mean steps, standard error,
# and reproducible 95% bootstrap CIs. Isolated from live Redis/bandit state.
# Needs a real MovieLens ratings.csv — the default --min-user-ratings/--min-movie-ratings
# filters wipe out the tiny bundled sampledata/ratings.csv.
cd services/java-retrieval-service
mvn -q compile exec:java \
  -Dexec.mainClass=com.demo.retrieval.evaluation.MovieLensPolicyEvaluation \
  -Dexec.args="--ratings /path/to/movielens/ratings.csv --output mdp_eval.csv"
```

Both intervals quantify *episode/event-sampling* uncertainty only — the OPE CIs are conditional
on the fitted reward model, and the MDP CIs on the fixed dataset. Neither is a claim of A/B lift.

---

### Ports

| Port | Service | Bound by | Notes |
|------|---------|----------|-------|
| `8080` | Retrieval service (Spring Boot) | `java-retrieval-service` (`SERVER_PORT`) | REST API: `/recommend`, `/predict`, `/feedback`, `/metrics` |
| `9092` | Kafka — host listener | `docker-compose.yml` | Producer and Spark jobs connect here (`localhost:9092`) |
| `29092` | Kafka — internal listener | `docker-compose.yml` | Inter-container only (`kafka:29092`) |
| `2181` | Zookeeper | `docker-compose.yml` | Kafka coordination |
| `6379` | Redis | `docker-compose.yml` (`REDIS_PORT`) | Embeddings, counters, user history |

> The Spark driver UI also binds `4040` (incrementing if taken) while a streaming/training job runs.

See [recsys-pipeline/README.md](recsys-pipeline/README.md) for full configuration, environment variable reference, and architecture details.

---

## frontend

A Next.js (app-router) rendering of the analysis dashboard — the same engagement / keyword /
query / recall / ranking / off-policy / MDP sections as the Python `analysis_dashboard_report.py`,
as React components. It reads a committed JSON snapshot, so it runs without Redis or Spark.

```bash
cd frontend
npm install
npm run dev            # http://localhost:3000

# refresh the snapshot from a real run (Redis up; a run's training_samples Parquet):
REDIS_HOST=localhost python export_dashboard_json.py --input /tmp/spark-recsys/training-samples
```

[`export_dashboard_json.py`](frontend/export_dashboard_json.py) reuses the pure `compute_*`
functions from the Python dashboard, so the React UI shows exactly what the HTML dashboard would;
sections with unavailable inputs render an explicit N/A card. See [frontend/README.md](frontend/README.md).

---

## Requirements

- Java 17, Scala 2.12
- Apache Spark 3.5.x
- Apache Kafka 3.x
- Redis 7+
- Maven 3.8+ (retrieval service)
- Docker / Docker Compose (local infrastructure)
- Node.js 18+ / npm (frontend dashboard, optional)

> **Note:** This repository is for learning and demonstration purposes. See individual sub-project READMEs for production configuration guidance.
