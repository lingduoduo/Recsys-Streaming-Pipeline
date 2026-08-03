# Recsys Streaming Pipeline

A recommendation-system playground combining streaming data pipelines, offline embedding jobs,
online learning, and bandit-style RL ranking — Spark-based production paths, Spark/Flink learning
notes, Kafka/Redis infrastructure, and a Spring Boot retrieval service.

## Start here

| I want to… | Go to |
|---|---|
| **Run the whole thing locally** — clean checkout to a populated dashboard | [Local Workflow Reference](recsys-pipeline/README.md#local-workflow-reference) |
| Understand the architecture, services, and storage design | [recsys-pipeline/README.md](recsys-pipeline/README.md#architecture) |
| Configure or operate the retrieval service | [Retrieval Service Configuration](recsys-pipeline/README.md#retrieval-service-configuration) |
| Reproduce a measurement run end to end | [Simulation Harnesses](recsys-pipeline/README.md#simulation-harnesses) |
| View the analysis dashboard without Redis or Spark | [frontend/README.md](frontend/README.md) |
| Read the Spark/Flink notes and Scala job reference | [spark-analysis/README.md](spark-analysis/README.md) |

Something broken during setup? Start with
[Troubleshooting](recsys-pipeline/README.md#troubleshooting-the-local-workflow).

## Sub-projects

Each sub-project owns its own documentation; this file is the index.

| Directory | What it is | Docs |
|---|---|---|
| [`recsys-pipeline/`](recsys-pipeline/) | The streaming recommendation platform: Kafka → Spark → Redis for live user history, an online joiner and slate collector for training data, offline embedding trainers, and a Spring Boot retrieval service combining an ONNX model, an online-learning reward model, and a UCB/Thompson bandit policy. | [README](recsys-pipeline/README.md) |
| [`frontend/`](frontend/) | Next.js (app-router) rendering of the analysis dashboard — the measurement and diagnostic sections as React components, served from a committed JSON snapshot. | [README](frontend/README.md) |
| [`spark-analysis/`](spark-analysis/) | Standalone Spark/Flink material: streaming concepts plus production-grade Scala jobs for user-behaviour analysis, classification, and BigQuery retention labelling. Not part of the running platform. | [README](spark-analysis/README.md) |

Feature storage in `recsys-pipeline` is three-tier: offline files (ONNX model + Parquet training
samples), Redis (real-time embeddings, counters, user history), and a Caffeine in-memory cache
that collapses per-request Redis round-trips from O(N×features) to O(1).

## Repository Structure

```
Recsys-Streaming-Pipeline/
├── recsys-pipeline/     # The streaming recommendation platform
│   ├── services/            # spark-streaming-job (Scala) · java-retrieval-service · python-modeling
│   ├── scripts/             # All runnable scripts; each cd's up to recsys-pipeline/
│   ├── integration-tests/   # Cross-service tests (pytest + shell)
│   ├── docs/                # Architecture and recommendation-flow docs
│   ├── sampledata/          # ratings.csv, catalog.json, sample embeddings
│   └── docker-compose.yml   # Local Kafka, ZooKeeper, Redis
├── frontend/            # Next.js dashboard + export_dashboard_json.py
└── spark-analysis/      # Spark/Flink notes and standalone Scala jobs
```

## Requirements

- Java 17, Scala 2.12
- Apache Spark 3.5.x
- Apache Kafka 3.x
- Redis 7+
- Maven 3.8+ (retrieval service)
- Docker / Docker Compose (local infrastructure)
- Node.js 18+ / npm (frontend dashboard, optional)

> **Note:** This repository is for learning and demonstration purposes. See individual sub-project
> READMEs for production configuration guidance.
