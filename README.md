# Recsys Streaming Pipeline

A recommendation-system playground combining streaming data pipelines, offline embedding jobs,
online learning, and bandit-style RL ranking — Spark-based production paths, Spark/Flink learning
notes, and Kafka/Redis infrastructure.

## Repository boundary

This project spans two repositories. This one owns the data path; the backend serves the retrieval
endpoints. Knowing which side you are on makes every other document here easier to read.

| | This repository | [lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service) |
|---|---|---|
| **Owns** | The offline and streaming path: Spark jobs, Python modeling and post-training, simulations, and the analysis dashboard | The serving path: the retrieval endpoints, the ONNX model they score with, the online-learning reward model, the bandit policy, and the in-process cache in front of Redis |
| **Builds with** | sbt, Spark, Python, npm | Maven / Spring Boot |
| **Talks to the other by** | HTTP, when a simulation measures a running backend | Kafka and Redis, which this repository's jobs consume |

The backend serves retrieval under the versioned prefix **`/api/v1/retrieval`** — `/recommend/{user}`,
`/feedback` and `/metrics` among them. The full endpoint reference is
[API.md](recsys-pipeline/docs/recommendation_architecture/API.md).

`run-movie-category-sim.sh` is the only script here that calls the backend. It composes the URL from
three overridable variables:

- `RETRIEVAL_SERVICE_PORT` — the port the default origin uses, default `8080`
- `SERVICE_URL` — the origin, default `http://localhost:$RETRIEVAL_SERVICE_PORT`
- `RETRIEVAL_BASE` — the prefix, default `/api/v1/retrieval`

If the backend re-mounts those routes, the simulation names the prefix rather than reporting the
backend as absent: it falls back to probing `/health/live`, which belongs to the health controller
and survives a retrieval-route reorganisation.

## Start here

| I want to… | Go to |
|---|---|
| **Run the whole thing locally** — clean checkout to a populated dashboard | [Local Workflow Reference](recsys-pipeline/README.md#local-workflow-reference) |
| Understand the architecture, services, and storage design | [recsys-pipeline/README.md](recsys-pipeline/README.md#architecture) |
| Configure the backend's retrieval service — Redis, cache, model paths | [Retrieval Service Configuration](recsys-pipeline/README.md#retrieval-service-configuration) |
| Reproduce a measurement run end to end | [Simulation Harnesses](recsys-pipeline/README.md#simulation-harnesses) |
| View the analysis dashboard without Redis or Spark | [recsys-pipeline/frontend/README.md](recsys-pipeline/frontend/README.md) |
| Read the Spark/Flink notes and Scala job reference | [spark-analysis/README.md](spark-analysis/README.md) |

Something broken during setup? Start with
[Troubleshooting](recsys-pipeline/README.md#troubleshooting-the-local-workflow).

## Sub-projects

Each sub-project owns its own documentation; this file is the index.

| Directory | What it is | Docs |
|---|---|---|
| [`recsys-pipeline/`](recsys-pipeline/) | The streaming recommendation platform: Kafka → Spark → Redis for live user history, an online joiner and slate collector that turn served slates into training data, offline embedding trainers, and the post-training suite. | [README](recsys-pipeline/README.md) |
| [`recsys-pipeline/frontend/`](recsys-pipeline/frontend/) | Next.js (app-router) rendering of the analysis dashboard — the measurement and diagnostic sections as React components, served from a committed JSON snapshot. | [README](recsys-pipeline/frontend/README.md) |
| [`spark-analysis/`](spark-analysis/) | Standalone Spark/Flink material: streaming concepts plus production-grade Scala jobs for user-behaviour analysis, classification, and BigQuery retention labelling. Not part of the running platform. | [README](spark-analysis/README.md) |

What this repository stores is two-tier: Parquet files on disk (training samples, slates, evaluation
output) and Redis (real-time embeddings, counters, user history). The third tier belongs to the
backend — an in-process cache that collapses per-request Redis round-trips from O(N×features) to
O(1) — and so does the ONNX artifact its scorer loads. Neither is built or checked in here.

## Repository Structure

```
Recsys-Streaming-Pipeline/
├── recsys-pipeline/     # The streaming recommendation platform
│   ├── services/            # spark-streaming-job (Scala) · python-modeling (modeling + post-training)
│   ├── scripts/             # All runnable scripts; each cd's up to recsys-pipeline/
│   ├── integration-tests/   # Cross-service tests (pytest + shell)
│   ├── docs/                # Architecture and recommendation-flow docs
│   ├── frontend/            # Next.js dashboard + export_dashboard_json.py
│   ├── sampledata/          # ratings.csv, catalog.json, sample embeddings
│   ├── schemas/             # Avro event schema + the cross-repository contract table
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
