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

## Sub-projects

Each sub-project owns its own documentation. This file is the index.

### [spark-analysis/](spark-analysis/README.md)

Spark/Flink concepts and production-grade Scala jobs: RDD and DataFrame fundamentals, window
functions, user-behaviour analysis, binary-classification ML pipelines, and BigQuery retention
labelling. Start here for the streaming concepts rather than the running platform.

**Full docs → [spark-analysis/README.md](spark-analysis/README.md)** — file-by-file reference,
key techniques, and how to submit each job.

### [recsys-pipeline/](recsys-pipeline/README.md)

The streaming recommendation platform: a Kafka → Spark → Redis path for live user history, an
online joiner and slate collector for training data, offline embedding trainers, and a Spring
Boot retrieval service combining an ONNX model, an online-learning reward model, and a
UCB/Thompson bandit policy.

**Full docs → [recsys-pipeline/README.md](recsys-pipeline/README.md)** — architecture, service
layout, configuration reference, simulation harnesses, and measurements.

**Getting it running → [Local Workflow Reference](recsys-pipeline/README.md#local-workflow-reference)**
— the one canonical path from a clean checkout to a populated dashboard, plus troubleshooting,
port assignments, and the optional modeling / experiment / offline-policy-evaluation sequences.

### [frontend/](frontend/README.md)

A Next.js (app-router) rendering of the analysis dashboard — the same engagement, keyword, query,
recall, ranking, off-policy, and MDP sections as the Python report, as React components. An
optional way to view the committed snapshot without Redis or Spark.

```bash
cd frontend
npm install
npm run dev            # http://localhost:3000
```

**Full docs → [frontend/README.md](frontend/README.md)**

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
