# Documentation Clarity Design

## Goal

Make the repository's local setup, pipeline, simulation, reporting, and dashboard instructions executable without relying on unstated working directories, service state, timing assumptions, or prior knowledge.

## Scope

Update:

- the repository-root `README.md`;
- every Markdown file under `recsys-pipeline/docs/recommendation_architecture/`;
- every Markdown file under `recsys-pipeline/docs/recommendation_flows/`.

Do not broadly rewrite `frontend/README.md` or `recsys-pipeline/README.md`. Correct links to those files only when needed for navigation from an in-scope document.

## Information Architecture

The root README is the canonical runnable entry point. It will contain one numbered local workflow:

1. verify prerequisites;
2. start local infrastructure;
3. detect and resolve port conflicts;
4. build required artifacts;
5. generate or simulate data;
6. wait for the workflow's explicit completion signal;
7. generate the desired report or frontend snapshot;
8. launch and refresh the dashboard;
9. stop local infrastructure.

Architecture documents explain component responsibilities, data contracts, configuration, and focused operational commands. They will link to the root quick start rather than duplicate a competing end-to-end setup.

Recommendation-flow documents remain conceptual request-path guides. They will explicitly identify required Redis state and link to the architecture reference that creates that state.

## Command Standard

Every runnable command sequence must state:

- the directory from which it runs;
- required services, artifacts, and input data;
- whether it is finite or long-running;
- its observable success or completion signal;
- its primary output path or endpoint.

Commands will use repository-relative paths consistently. Environment-variable examples will distinguish defaults from required overrides.

## Dashboard Workflow

The documentation will distinguish two dashboard artifacts:

- the standalone HTML report generated at a simulation-relative `report-dashboard/index.html`;
- the Next.js dashboard, which reads the static `frontend/data/dashboard.json` snapshot.

Snapshot-refresh instructions will use the movie-category simulation path when demonstrating populated Keyword Gap and L1/L2/L3 tables:

`/tmp/spark-recsys/movie-category-sim/training-samples`

They will require the simulation to reach `==> done` and Redis to remain running before export. They will state that browser refresh or a development-server restart may be necessary after regenerating the snapshot.

## Troubleshooting Coverage

Add a concise, evidence-oriented troubleshooting section covering:

- host port `6379` already allocated by another Redis container;
- Kafka topic exists but has zero offsets because no producer has emitted data;
- streaming producers and consumers are intentionally long-running;
- exporter invoked before the simulation writes training samples;
- frontend still serving an old committed dashboard snapshot;
- Keyword Gap reporting `unknown` and empty L1/L2/L3 because Redis movie metadata was unavailable during export;
- ONNX numeric IDs outside the model lookup vocabulary.

Each entry will include a diagnostic command, the expected interpretation, and the least-destructive remedy. Instructions will not stop or delete unrelated containers automatically.

## Consistency and Verification

Audit in-scope documents for:

- script and file existence;
- correct working directories;
- environment-variable names and defaults;
- ports and service names;
- output paths;
- relative Markdown links and anchors;
- conflicting duplicate instructions.

Verification will use repository searches, shell syntax where command blocks can be extracted safely, link/path checks, and a final diff review. Documentation examples that require a live Kafka/Spark workload will be validated against source and existing tests rather than rerunning expensive simulations solely for documentation.

## Non-Goals

- Reorganizing the entire documentation tree.
- Changing runtime behavior or application code.
- Rewriting learning-note content unrelated to operating the recommendation pipeline.
- Removing advanced reference material merely to shorten the docs.
