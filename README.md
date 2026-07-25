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

From the repository root:

```bash
cd spark-analysis
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

The platform runs as **three pipelines**. The workflow immediately below is the one canonical
local path from a clean checkout to a populated React dashboard. Later command sequences are
explicitly optional references, not alternative quick starts.

---

### Canonical finite local workflow

#### 1. Verify prerequisites

From the repository root, verify the tools used by this workflow. Java 17, Spark 3.5 with
`spark-submit` on `PATH`, sbt, Python, Docker Compose, Node.js 18+, and npm must be available.

```bash
java -version
spark-submit --version
sbt --version
python --version
docker compose version
node --version
npm --version
```

Each command must print a version and exit successfully before continuing.

#### 2. Check host-port conflicts

From the repository root, inspect the local ports before starting infrastructure:

```bash
lsof -nP -iTCP:6379 -sTCP:LISTEN
lsof -nP -iTCP:9092 -sTCP:LISTEN
lsof -nP -iTCP:2181 -sTCP:LISTEN
lsof -nP -iTCP:3000 -sTCP:LISTEN
```

No output means the port is free. If a command shows an owner, identify it and stop it only when
you know it belongs to this project; do not stop unrelated containers or services.

#### 3. Start Kafka and Redis

From the repository root:

```bash
cd recsys-pipeline
docker compose up -d zookeeper kafka redis
docker compose ps
```

Do not continue until Kafka and Redis report `healthy`. Ports `9092`, `2181`, and `6379` are the
host listeners. A bind or readiness failure here is an infrastructure problem, not a Spark
failure.

#### 4. Install dependencies and build the Spark artifact

From the repository root:

```bash
cd recsys-pipeline
python -m pip install -r services/python-modeling/requirements.txt
python -m pip install pandas pyarrow numpy redis
(cd services/spark-streaming-job && sbt assembly)
test -s services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

The final command exits successfully when the required fat jar exists.

#### 5. Run the finite movie-category simulation

From the repository root:

```bash
cd recsys-pipeline
./run-movie-category-sim.sh
```

The harness is finite but can take several minutes. It resets this Compose project's volumes and
Redis state, generates movie metadata and engagement samples, runs the Spark jobs, creates
embeddings, and writes the standalone HTML report. Its primary Parquet output is
`/tmp/spark-recsys/movie-category-sim/training-samples`.

#### 6. Wait for the completion signal

Do not export a snapshot while the harness is still draining Kafka or writing Parquet. Continue
only after the simulation prints the literal terminal line:

```text
==> done. CSVs under /tmp/spark-recsys/movie-category-sim/report-categories ; dashboard at /tmp/spark-recsys/movie-category-sim/report-dashboard/index.html
```

Keep Redis running after this line; the exporter still needs `movie:*:features`, `i2vEmb:*`, and
`uEmb:*`.

#### 7. Export and validate the React snapshot

From the repository root:

```bash
REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input /tmp/spark-recsys/movie-category-sim/training-samples \
  --output frontend/data/dashboard.json

python - <<'PY'
import json

expected = "/tmp/spark-recsys/movie-category-sim/training-samples"
with open("frontend/data/dashboard.json") as handle:
    data = json.load(handle)
assert data["input"] == expected, data["input"]
assert data["rows"] > 0, data["rows"]
assert data["keyword"]["tops"]["l1"], "empty Keyword Gap L1 table"
print("snapshot valid:", data["rows"], "rows;", len(data["keyword"]["tops"]["l1"]), "L1 entries")
PY
```

Success is a `snapshot valid:` line with positive row and L1 counts. `frontend/data/dashboard.json`
is the output consumed by the React app.

#### 8. Launch and refresh the React dashboard

From the repository root:

```bash
cd frontend
npm install
npm run dev
```

This development server is intentionally long-running. Open `http://localhost:3000`. After
regenerating `frontend/data/dashboard.json`, hard-refresh the browser; if it still shows the old
row count, stop the server with `Ctrl-C` and run `npm run dev` again.

#### 9. Stop processes and infrastructure

Stop the dashboard with `Ctrl-C` in its terminal. Manual producers and Spark jobs from the optional
reference below are also long-running and should be stopped with `Ctrl-C` in their own terminals.
If a terminal was lost, inspect the exact process first, then enter only a PID owned by this
project:

```bash
ps -Ao pid=,command= | grep -E '[p]roducer.py|[r]un-streaming-job.sh|[s]park-submit|[n]ext dev'
printf 'Confirmed project PID to stop: '
read -r RECSYS_PROCESS_PID
kill -TERM "$RECSYS_PROCESS_PID"
```

Finally, from the repository root, stop only this repository's Compose project:

```bash
cd recsys-pipeline && docker compose down
```

### Optional reference: manual streaming data pipeline

This path is useful for inspecting individual Kafka/Spark stages, but it is not the canonical
dashboard workflow. Run each producer or Spark command from the repository root in a separate
terminal; all Spark consumers are long-running until `Ctrl-C`.

```bash
cd recsys-pipeline

# Finite producer check: exits after 100 records.
EVENTS_PER_SECOND=20 LOG_EVERY=10 MAX_EVENTS=100 \
  python services/python-modeling/producer.py

# Individual long-running consumers:
./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.OnlineJoinerStreamingJob ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.ExperienceCollectorStreamingJob ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.RecallSampleStreamingJob    ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.RankingSampleStreamingJob   ./run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.RelevanceSampleStreamingJob ./run-streaming-job.sh

# Or launch the core long-running consumers together:
./run-data-pipeline.sh
```

Spark streaming jobs wait when `recsys_events` has no records. Zero Kafka offsets mean a producer
has not emitted to that topic; they do not prove a consumer crash. Topic names must match across
each producer and consumer.

### Optional reference: standalone HTML dashboard

The simulation already writes a self-contained report at
`/tmp/spark-recsys/movie-category-sim/report-dashboard/index.html`. To regenerate that separate
artifact after the canonical simulation has printed `==> done`, keep Redis running and run the
following from the repository root:

```bash
cd recsys-pipeline
IN=/tmp/spark-recsys/movie-category-sim/training-samples
REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py \
  --input "$IN" \
  --mdp-csv "$IN/../mdp_eval.csv"
```

The report writes to `$IN/../report-dashboard/index.html`. Recall/ranking require Redis movie
metadata and embeddings; off-policy and MDP cards render N/A when their replay-buffer or
`mdp_eval.csv` inputs are absent. See
[Analysis Reports](recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md) for focused
report commands.

### Troubleshooting the local workflow

Run Docker Compose diagnostics below from the repository root after entering `recsys-pipeline/`;
the remaining diagnostics use the paths shown.

| Symptom | Diagnostic | Interpretation | Remedy |
|---|---|---|---|
| `Bind for 0.0.0.0:6379 failed` | `lsof -nP -iTCP:6379 -sTCP:LISTEN` and `docker ps --filter publish=6379` | another process/container owns the port | identify the owner; stop it only if safe, then rerun |
| `UnknownTopicOrPartitionException` | `docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic recsys_events` | missing/wrong topic | use the launcher bootstrap or create/produce to the configured topic |
| Producer appears stuck | inspect `rate=...` and `LOG_EVERY` banner | normal long-running producer | use `LOG_EVERY=1` or `MAX_EVENTS` |
| Dashboard exporter says no samples | `find /tmp/spark-recsys/movie-category-sim/training-samples -name '*.parquet'` | simulation not finished | wait for `==> done` |
| Keyword Gap is `unknown`; L1/L2/L3 empty | `docker compose exec -T redis redis-cli --scan --pattern 'movie:*:features' \| wc -l` | snapshot exported without movie metadata | keep Redis running and export from the movie-category path |
| Dashboard still shows old row count | inspect `input` and `rows` in `frontend/data/dashboard.json` | stale static snapshot/browser | rerun exporter, hard-refresh, or restart `npm run dev` |
| ONNX Gather index error | `GET /predict/metadata` | raw numeric ID exceeds lookup size | use string IDs or indices within metadata bounds |

---

### Optional reference: modeling pipeline — Spark embeddings + Python two-tower → ONNX

This optional reference trains embeddings offline and produces the ONNX model the retrieval
service serves. Run the block from the repository root.

```bash
cd recsys-pipeline
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

<a id="3-experiment-pipeline--retrieval-service-8080"></a>

### Optional reference: experiment pipeline — retrieval service `:8080`

This optional reference serves recommendations and runs online learning + UCB/Thompson bandit RL.
Run the service block from the repository root.

```bash
# Start the service (binds :8080; connects to Redis :6379)
cd recsys-pipeline/services/java-retrieval-service
mvn spring-boot:run
```

```bash
# Ranked recommendations with per-item diagnostics + request metrics
curl 'http://localhost:8080/recommend/u_1?limit=6'

# Offline ONNX score for a string ID pair in the default lookup
curl 'http://localhost:8080/predict/user_employee_01/action_benefits'

# Loaded model name, lookup-table sizes, ONNX input/output names
curl http://localhost:8080/predict/metadata

# Raw zero-based lookup indices; inspect metadata first.
# For the default model, users are 0..31 and items are 0..11.
curl 'http://localhost:8080/predict/id?userId=0&itemId=4'

# Item2Vec embedding vector for an item
curl http://localhost:8080/embedding/movie_42

# Record a click/reward → triggers online learning + bandit update
curl -X POST http://localhost:8080/feedback \
  -H 'Content-Type: application/json' \
  -d '{"user":"u_1","item":"movie_42","clicked":true,"reward":1.0}'

# Aggregate bandit metrics (CTR, regret, novelty, coverage) per algorithm
curl http://localhost:8080/metrics
```

The default MLP string lookup contains four user families:
`user_employee_01..08`, `user_manager_01..08`, `user_new_hire_01..08`, and
`user_payroll_admin_01..08`. Its item lookup uses twelve `action_*` IDs, including
`action_benefits`, `action_learning`, `action_onboarding`, and `action_payroll`. Use those string
IDs with `/predict/{user}/{item}`. Numeric `/predict/id` values are internal indices, not external
movie IDs, and must satisfy `0 <= userId < users` and `0 <= itemId < items` from
`/predict/metadata`.

---

### Offline policy evaluation

This optional reference covers two standalone offline evaluators that compare policies *without*
touching the serving path. Run the block from the repository root.

```bash
cd recsys-pipeline
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
as React components. This is an optional way to view the committed snapshot without Redis or
Spark. Run it from the repository root:

```bash
cd frontend
npm install
npm run dev            # http://localhost:3000
```

[`export_dashboard_json.py`](frontend/export_dashboard_json.py) reuses the pure `compute_*`
functions from the Python dashboard, so the React UI shows exactly what the HTML dashboard would;
sections with unavailable inputs render an explicit N/A card. Refresh the snapshot only through
step 7 of the [canonical finite workflow](#canonical-finite-local-workflow), which uses the
movie-category input required for populated Keyword Gap tables. See
[frontend/README.md](frontend/README.md).

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
