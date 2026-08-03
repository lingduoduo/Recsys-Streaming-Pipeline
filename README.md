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
    ├── scripts/                        # All runnable scripts; each cd's up to recsys-pipeline/
    │   ├── run-streaming-job.sh        # Submit a single Spark streaming job (SPARK_MAIN_CLASS)
    │   ├── run-data-pipeline.sh        # Launch all core streaming jobs together
    │   ├── run-offline-pipeline.sh     # Train Item2Vec embeddings
    │   ├── run-user-embedding-pipeline.sh  # Train user embeddings
    │   ├── run-als-pipeline.sh         # Train ALS collaborative-filtering embeddings
    │   ├── run-retrain.sh              # Full retrain: replay export → ALS → user emb → two-tower → hot-reload
    │   ├── run-engagement-sim.sh       # E2E sim: engagement CTR time-series (+ report)
    │   ├── run-movielens-segment-sim.sh    # E2E sim: engagement by user segment (+ report)
    │   ├── run-movie-category-sim.sh   # E2E sim: engagement by movie category l1/l2/l3 (+ report)
    │   └── install-cron.sh             # Installs scheduled retraining cron job
    ├── sampledata/                     # ratings.csv, catalog.json, sample embeddings
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

A streaming recommendation platform: a real-time Kafka → Spark Streaming → Redis path for live
user history, a Kafka → Spark online joiner and slate collector for training data, offline
embedding trainers, and a Spring Boot retrieval service that combines an offline ONNX model, a
real-time online-learning reward model, and a UCB/Thompson bandit RL policy. Feature storage uses
a three-tier design: offline files (ONNX model + Parquet training samples), Redis (real-time
embeddings, counters, user history), and a Caffeine in-memory cache that collapses per-request
Redis round-trips from O(N×features) to O(1).

**Full docs → [recsys-pipeline/README.md](recsys-pipeline/README.md)** — architecture, service
layout, scoring and storage design, configuration reference, simulation harnesses, and
measurements.

**Getting it running → [Local Workflow Reference](recsys-pipeline/README.md#local-workflow-reference)**
— the one canonical path from a clean checkout to a populated React dashboard, plus
troubleshooting, port assignments, and the optional manual-pipeline, modeling, experiment, and
offline-policy-evaluation sequences.

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
step 7 of the [canonical local workflow](recsys-pipeline/README.md#local-workflow-reference), which uses the
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
