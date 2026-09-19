# Recsys Streaming Pipeline

A recommendation-system playground combining streaming data pipelines, offline embedding jobs,
online learning, and bandit-style RL ranking — Spark-based production paths, Spark/Flink learning
notes, and Kafka/Redis infrastructure. Recommendations are served by a separate retrieval service,
[lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service).

## Repository boundary

This project spans two repositories, and the split is the single most useful thing to know before
reading any other document here.

| | This repository | [lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service) |
|---|---|---|
| **Owns** | The offline and streaming path: Spark jobs, Python modeling and post-training, simulations, and the analysis dashboard | The serving path: the retrieval service, its ONNX model, online-learning reward model and bandit policy |
| **Builds with** | sbt, Spark, Python, npm | Maven / Spring Boot |
| **Talks to the other by** | HTTP, when a simulation measures a running service | Kafka and Redis, which this repository's jobs consume |

The service exposes its retrieval endpoints under the versioned prefix **`/api/v1/retrieval`** —
`/recommend/{user}`, `/feedback` and `/metrics`. `run-movie-category-sim.sh` composes that URL from
two overridable variables:

- `SERVICE_URL` — the origin, default `http://localhost:8080`
- `RETRIEVAL_BASE` — the prefix, default `/api/v1/retrieval`

If the service moves its routes again, the simulation says so by name rather than reporting the
service as absent: it falls back to probing `/health/live`, which belongs to the health controller
and survives a retrieval-route reorganisation.

Maven is a prerequisite of that repository, not of this checkout. Nothing here has a `pom.xml`.

## Start here

| I want to… | Go to |
|---|---|
| **Run the whole thing locally** — clean checkout to a populated dashboard | [Local Workflow Reference](recsys-pipeline/README.md#local-workflow-reference) |
| Understand the architecture, services, and storage design | [recsys-pipeline/README.md](recsys-pipeline/README.md#architecture) |
| Configure or operate the retrieval service | [Retrieval Service Configuration](recsys-pipeline/README.md#retrieval-service-configuration) |
| Reproduce a measurement run end to end | [Simulation Harnesses](recsys-pipeline/README.md#simulation-harnesses) |
| View the analysis dashboard without Redis or Spark | [recsys-pipeline/frontend/README.md](recsys-pipeline/frontend/README.md) |
| Read the Spark/Flink notes and Scala job reference | [spark-analysis/README.md](spark-analysis/README.md) |

Something broken during setup? Start with
[Troubleshooting](recsys-pipeline/README.md#troubleshooting-the-local-workflow).

## Sub-projects

Each sub-project owns its own documentation; this file is the index.

| Directory | What it is | Docs |
|---|---|---|
| [`recsys-pipeline/`](recsys-pipeline/) | The streaming recommendation platform: Kafka → Spark → Redis for live user history, an online joiner and slate collector for training data, and offline embedding trainers. Recommendations are served by a separate retrieval service, [lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service), combining an ONNX model, an online-learning reward model, and a UCB/Thompson bandit policy. | [README](recsys-pipeline/README.md) |
| [`recsys-pipeline/frontend/`](recsys-pipeline/frontend/) | Next.js (app-router) rendering of the analysis dashboard — the measurement and diagnostic sections as React components, served from a committed JSON snapshot. | [README](recsys-pipeline/frontend/README.md) |
| [`spark-analysis/`](spark-analysis/) | Standalone Spark/Flink material: streaming concepts plus production-grade Scala jobs for user-behaviour analysis, classification, and BigQuery retention labelling. Not part of the running platform. | [README](spark-analysis/README.md) |

Feature storage in `recsys-pipeline` is three-tier: offline files (ONNX model + Parquet training
samples), Redis (real-time embeddings, counters, user history), and a Caffeine in-memory cache
that collapses per-request Redis round-trips from O(N×features) to O(1).

## Repository Structure

```
Recsys-Streaming-Pipeline/
├── recsys-pipeline/     # The streaming recommendation platform
│   ├── services/            # spark-streaming-job (Scala) · python-modeling (retrieval service moved to lingduoduo/Recsys-Backend-Service)
│   ├── scripts/             # All runnable scripts; each cd's up to recsys-pipeline/
│   ├── integration-tests/   # Cross-service tests (pytest + shell)
│   ├── docs/                # Architecture and recommendation-flow docs
│   ├── frontend/            # Next.js dashboard + export_dashboard_json.py
│   ├── sampledata/          # ratings.csv, catalog.json, sample embeddings
│   └── docker-compose.yml   # Local Kafka, ZooKeeper, Redis
└── spark-analysis/      # Spark/Flink notes and standalone Scala jobs
```

## Requirements

- Java 17, Scala 2.12
- Apache Spark 3.5.x
- Apache Kafka 3.x
- Redis 7+
- Docker / Docker Compose (local infrastructure)
- Node.js 18+ / npm (frontend dashboard, optional)

> **Note:** This repository is for learning and demonstration purposes. See individual sub-project
> READMEs for production configuration guidance.
