# Streaming Recsys Platform

`recsys-pipeline` is a recommendation-system playground organized around three pipelines:

- **Data pipeline** — Spark Structured Streaming jobs that ingest Kafka click and behavior events, join impressions with feedback into feature+label training samples, train Item2Vec and ALS embeddings from historical ratings, and keep per-user history and global item popularity fresh in Redis.
- **Model prediction pipeline** — A Spring Boot retrieval service that loads pre-trained ONNX models and embedding configs at startup, combines offline embedding scores with an online reward model, and serves ranked recommendations through a REST API.
- **Experiment pipeline** — A bandit-based evaluation layer (UCB, Thompson Sampling, Q-learning, SARSA) that tracks per-algorithm click-through and reward metrics, maintains a replay buffer for offline analysis, and supports hot-swapping model artifacts without redeployment.

## Service Layout

All independently runnable application code lives under `services/`:

| Service | Build tool | Responsibility |
|---|---|---|
| `services/spark-streaming-job` | sbt | Streaming ingestion, feature joins, offline embedding training, and candidate pre-computation |
| `services/java-retrieval-service` | Maven | Loads ONNX models and embedding configs at startup, scores candidates, runs bandit evaluation (UCB, Thompson, Q-learning, SARSA), and serves recommendations via REST |
| `services/python-modeling` | pip / pytest | Synthetic event producer, MovieLens two-tower training, ONNX model export, and evaluation utilities |

Infrastructure, shared sample data, and orchestration scripts remain at the `recsys-pipeline` root.

## Architecture

![Recsys Streaming Pipeline](recsys-streaming-pipeline-architecture.png)

> Interactive version: [recsys-streaming-pipeline.html](recsys-streaming-pipeline-architecture.html)


### Data Pipeline

Spark Structured Streaming ingestion, derived ML datasets, the Spark job package layout, the
real-time job path (producer + streaming jobs), and the offline embedding-training jobs live in
[Data_Pipeline.md](docs/recommendation_architecture/Data_Pipeline.md).

### Avro ingestion and archive replay

The `recsys_events` Kafka input uses the canonical Avro single-object event contract. Topic
creation is deliberate: Kafka auto-creation is disabled, and the local pipeline wrapper checks
Kafka health and provisions the checked-in catalog before it launches a process. Start the local
services, then provision explicitly when operating a single job:

```bash
docker compose up -d
python scripts/provision-kafka-topics.py --bootstrap-server localhost:9092
```

Install the live-test dependencies and assemble the Spark JAR before enabling the opt-in
round-trip test:

```bash
python -m pip install -r services/python-modeling/requirements.txt pytest
(cd services/spark-streaming-job && sbt assembly)
```

Host provisioning uses `kafka-topics` and `kafka-configs` on `PATH`. For the local Compose stack
instead, use its Kafka CLI explicitly:

```bash
python scripts/provision-kafka-topics.py \
  --bootstrap-server localhost:9092 --command-mode docker-compose
```

The real end-to-end check is opt-in and requires reachable Kafka and Redis. Choose the mode that
matches the provisioner (`host` is the default):

```bash
RUN_KAFKA_INTEGRATION=1 KAFKA_INTEGRATION_COMMAND_MODE=docker-compose \
  pytest -q integration-tests/test_avro_kafka_round_trip.py
```

When disabled, the test is one clean skip and imports no Kafka/Spark/Parquet dependencies. When
enabled, missing Python packages, the assembled JAR, reachable Kafka/Redis, or usable host/Compose
Kafka CLI cause a skip with the exact remediation rather than a false integration failure.

`recsys_events` is the live input. `recsys_events.backfill` is a separately provisioned,
short-retention replay target; nothing reads it by default. A consumer must deliberately opt in,
for example:

```bash
KAFKA_TOPIC=recsys_events.backfill ./scripts/run-streaming-job.sh
```

The Spark ingestion job archives valid events and dead letters before its business sinks:

```bash
RECSYS_EVENT_ARCHIVE_PATH=/data/recsys-events \
RECSYS_EVENT_DEAD_LETTER_PATH=/data/recsys-events-dead-letter \
  ./scripts/run-streaming-job.sh
```

Those paths must be on a filesystem that supports atomic, non-overwriting directory renames
(local/HDFS through Hadoop `FileContext`, for example). Object stores without that rename
guarantee are not a supported archive target. A committed archive batch has both `_SUCCESS` and
its version-2 `_COMMITTED` identity manifest below
`_queries/<checkpoint-hash>/_batches/<batch-id>/`. The manifest inventories every Parquet file and
its relative path, byte size, and SHA-256 digest plus the total row count; replay verifies all four
before publishing. `row_count=0` with an empty inventory is a valid all-invalid micro-batch.
Archive commit protocol v1 is a pre-release format and is rejected with an instruction to
regenerate the data. There is no v1 migration or backward-compatibility path.

Replay is always bounded and only publishes to `recsys_events.backfill`:

```bash
REPLAY_ARCHIVE_PATH=/data/recsys-events \
REPLAY_ARCHIVE_QUERY_NAMESPACE=<checkpoint-hash> \
REPLAY_OPERATION_ID=incident-2026-08-01 \
REPLAY_START_DATE=2026-08-01 REPLAY_END_DATE=2026-08-02 \
REPLAY_MAX_ROWS=100000 REPLAY_RECORDS_PER_SECOND=5000 \
  ./scripts/run-archive-replay.sh
```

Replay reads only numeric batches below the selected query namespace whose `_SUCCESS` and exact
`_COMMITTED` identity manifest both validate; attempt, dedupe, incomplete, and other-query data is
not eligible. `REPLAY_OPERATION_ID` is a stable operator-owned identity. Its atomic JSON manifest
is `$REPLAY_MANIFEST_DIR/<operation-id>.json` when configured, otherwise
`$REPLAY_ARCHIVE_PATH/_replay_manifests/<operation-id>.json`. A completed operation is a no-op on
rerun; an interrupted operation resumes from its last durably recorded
`(sorted committed file path, row group, row)` position.
Published records retain `event_id` and carry a stable `operation_id:event_id` Kafka key plus
operation/event headers.

Replay is at-least-once, not exactly-once. A process can stop after Kafka acknowledges a record but
before its cursor is persisted, so the cursor narrows but cannot eliminate duplicates. Backfill
consumers must deduplicate `event_id` (or the stable operation/event identity). See
[Data_Pipeline.md](docs/recommendation_architecture/Data_Pipeline.md#avro-kafka-ingestion-archive-and-replay)
for schema, retention, dead-letter, and replay constraints.

On the migrated Avro engine path, every business sink has a stable identity and a durable
`(query, sink, batchId)` completion marker. A completed first sink is skipped when a later sink
causes the micro-batch to retry. Parquet uses visible
`query=<hash>/sink=<hash>/batch=<id>` directories. The payload schema may not contain the reserved
control columns `query`, `sink`, or `batch`. Multiple identities may share a configured root, so
readers must not discover a schema from that mixed root. Scala/Spark consumers must call
`DurableParquetCommit.readIdentity` with `spark`, `configuredRoot`, `queryNamespace`,
`sinkNamespace`, and `expectedPayloadSchema`. It resolves the exact
`query=<hash>/sink=<hash>` path before reading, sets `basePath` to the configured root, and applies
the caller's explicit payload schema. Consumers may then filter the visible `batch` partition.
Redis popularity and sequence effects use one atomic Lua ledger hash per retained batch, a bounded
batch index, and one monotonic committed-batch watermark per stable query/sink. Completion advances
the fence and only then prunes outside the configurable recovery window (minimum two batches, so N
and N-1 remain retry-safe); delayed work below that horizon is skipped. Sequence ledger hashes,
index, and watermark share a renewed retry horizon, outlive target sequence data by one second,
and then expire. Kafka enables
producer idempotence and publishes a
stable record key and query/sink/batch headers; a failure across producer sessions can still repeat
an acknowledged record, so derived-topic consumers must deduplicate the stable key. The engine
rejects sinks without this retry contract and does not claim cross-system exactly-once delivery.

`./scripts/run-data-pipeline.sh` is deliberately the small operable vertical slice: it starts the
clickstream producer and `UserEventStreamingJob`, which reads `recsys_events`, archives its Avro
records, and updates Redis popularity. It does not launch `OnlineJoinerStreamingJob`,
`ExperienceCollectorStreamingJob`, or other derived-topic jobs. Those jobs retain their separate
launch commands, produce/consume legacy JSON derived topics such as `training_samples` and
`training_experiences`, and require an operator to provision those legacy topics separately before
running them.

### Model Prediction Pipeline

```text
movielens_pipeline.py ──► two-tower retrieval + transformer ranking ──► sampledata/*.onnx

sampledata/*.onnx ──────────────────────────────────────────────┐
Redis: embeddings, user history, candidate lists ───────────────┼──► java-retrieval-service  (FeatureCache / Caffeine)
Redis: reward stats, bandit counters ───────────────────────────┘          │
                                                                            ├──► GET  /recommend/{user}
                                                                            ├──► GET  /embedding/{item}
                                                                            └──► GET  /predict/{user}/{item}
```

### Experiment Pipeline

```text
GET /recommend/{user} ──► bandit arm selection ──► impression logged → Redis bandit:metrics:{algo}
                           UCB | Thompson |
                           Q-learning | SARSA

POST /feedback ──► online reward update ──► Redis reward-model:{item|genre|tag|global}
               └──► Redis replay:recommendations  (replay buffer)
                             │
                             ▼
                    run-retrain.sh ──► retrain embeddings + model ──► POST /actuator/model-reload  (hot-swap)

GET /metrics ──► cross-algorithm comparison  (UCB vs Thompson vs Q-learning vs SARSA)
```

## Scoring Model Architecture

See [6_Predicting_Scoring.md](docs/recommendation_flows/6_Predicting_Scoring.md) for the three-stage scoring model (offline / two-tower / online / bandit), the `offlineScore` → `learnedPrior` → `banditScore` formulas, and the per-algorithm bandit notes.

## Storage Architecture

Feature data is split across three tiers by access pattern and update frequency.

| Tier | Contents | Updated by |
|------|----------|------------|
| **Disk** (filesystem) | ONNX model (`mlp_embedding_model.onnx`), ID lookup tables (`mlp_embedding_lookups.json`), two-tower ONNX models (`movielens_user_tower.onnx`, `movielens_item_tower.onnx`, `movielens_ranking.onnx`), Parquet training samples partitioned by date | Training jobs; swappable at runtime via `ONNX_MODEL_PATH` without JAR rebuild |
| **Redis** | User click history, global item popularity, per-user columnar rating/click sequences (`seq:{id}:{kind}:{day}`), item/user embeddings (`i2vEmb:*`, `uEmb:*`, `alsItemEmb:*`, `alsUserEmb:*`, `twoTowerItemEmb:*`), bandit counters, reward model stats, replay buffer | Streaming jobs (each micro-batch) and `/feedback` calls |
| **In-memory** (Caffeine) | Item vectors (`i2vEmb:*`), reward model stats (`reward-model:*`) | Populated from Redis on first request; TTL-expired; invalidated on `/feedback` writes |

The in-memory cache (`FeatureCache`) eliminates O(N × features) Redis round-trips per recommendation request. Before the scoring loop, a single `MGET` loads all candidate and recent-item vectors; reward model estimates are cached per key for the configured TTL and invalidated immediately when `/feedback` updates them.

**Disk model hot-swap** — set `ONNX_MODEL_PATH` and `ONNX_LOOKUPS_PATH` to replace the MLP model artifacts on the filesystem without rebuilding the JAR. The service falls back to classpath resources when the env vars are unset (the development and test default). To enable the two-tower scoring path, set `ONNX_USER_TOWER_PATH`, `ONNX_ITEM_TOWER_PATH`, and `ONNX_RANKING_PATH` to the three ONNX files exported by `movielens_pipeline.py`.

## Recommendation Request Flow

For each incoming request, the retrieval service executes nine steps in order:

1. **Hydrate query** — 21 `QueryHydrator` implementations populate `ScoredMoviesQuery` with per-user context: watch history, rating sequences, social graph, served history, MinHash, cached candidates, bloom filter, geo, demographics, and inferred signals.
2. **Fetch popular candidates** — pulls top items from `global:item_popularity` (see [2_Fetch_Popular_Stuff.md](docs/recommendation_flows/2_Fetch_Popular_Stuff.md)).
3. **Generate cold-start candidates** — adds extra candidates from the configured catalog for users or items with no exposure history (see [3_Cold_Start.md](docs/recommendation_flows/3_Cold_Start.md)).
4. **Filter** — `CandidateFilter` drops seen, blocked, muted, and otherwise ineligible candidates (see [4_Filtering.md](docs/recommendation_flows/4_Filtering.md)).
5. **Hydrate candidates** — `CandidateHydrator` enriches surviving candidates with engagement counts, in-network signals, MinHash Jaccard similarity, and visibility flags (see [5_Candidate_Hydration.md](docs/recommendation_flows/5_Candidate_Hydration.md)).
6. **Score** — combines all three scoring stages: offline ONNX score, online reward-model estimate, and bandit arm score (see [6_Predicting_Scoring.md](docs/recommendation_flows/6_Predicting_Scoring.md)).
7. **Randomize** — shuffles the top scoring pool slightly to avoid deterministic repetition (see [7_Shuffling.md](docs/recommendation_flows/7_Shuffling.md)).
8. **Store context** — writes pending recommendation context to the replay buffer for downstream training (see [8_Store_Context.md](docs/recommendation_flows/8_Store_Context.md)).
9. **Track metrics** — records impressions, clicks, regret-style metrics, novelty, and catalog coverage (see [9_Track_Metrics.md](docs/recommendation_flows/9_Track_Metrics.md)).

Default catalog and ranking weights are in `services/java-retrieval-service/src/main/resources/application.yml`.

---

## Quick Start

**Prerequisites:** Java 17, Apache Spark 3.5.x (Scala 2.12), sbt, Maven 3.8+, Docker Compose, Python 3.

The five steps below bring up the full stack: offline embeddings → retrieval service → clickstream producer → streaming job → API.

Build the Spark job JAR (run from the repo root):

```bash
(cd services/spark-streaming-job && sbt assembly)
```

Start Kafka, Zookeeper, and Redis:

```bash
colima start
docker compose up -d
```

### Dashboard
```
cd recsys-pipeline/frontend && npm run validate:data
```

### Step 1 — Offline embeddings

> Run once before starting the retrieval service.

Train Item2Vec embeddings and write them to Redis (`i2vEmb:{itemId}` keys):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv \
ITEM2VEC_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
./scripts/run-offline-pipeline.sh
```

Train user embeddings from the item vectors above and write to Redis (`uEmb:{userId}` keys):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv \
ITEM2VEC_EMBEDDING_PATH=sampledata/item_embedding.txt \
USER_EMBEDDING_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
./scripts/run-user-embedding-pipeline.sh
```

**Optional** — train ALS collaborative-filtering embeddings (`alsItemEmb:*`, `alsUserEmb:*`):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv \
ALS_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
./scripts/run-als-pipeline.sh
```

To use ALS embeddings instead, set these before starting the retrieval service:

```bash
export ITEM_EMBEDDING_PREFIX=alsItemEmb
export USER_EMBEDDING_PREFIX=alsUserEmb
```

### Step 2 — Start the retrieval service

```bash
mvn -f services/java-retrieval-service/pom.xml spring-boot:run
```

The service loads `mlp_embedding_model.onnx` from the classpath at startup. To use a model trained outside the JAR, set `ONNX_MODEL_PATH` and `ONNX_LOOKUPS_PATH` before starting.

### Step 3 — Start the clickstream producer

```bash
pip install -r services/python-modeling/requirements.txt
python services/python-modeling/producer.py
```

### Step 4 — Run the streaming job

```bash
./scripts/run-streaming-job.sh
```

This populates `global:item_popularity` in Redis in real time; the retrieval service uses it as a popularity signal. (Per-user recency comes from `user:{id}:features`, written by `MovieLensContextCollectorStreamingJob`.)

### Optional — Build behavioral user profiles

The offline profile job reads interaction Parquet, derives decayed genre/tag preferences and
explainable personas, writes an immutable Parquet snapshot, then publishes run-scoped Redis values
before atomically advancing `user-profile:v1:active-run`:

```bash
USER_PROFILE_INPUT_PATH=/path/to/profile-events-parquet \
USER_PROFILE_OUTPUT_PATH=/path/to/user-profiles/run-2026-08-06 \
./scripts/run-user-profile-pipeline.sh
```

The retrieval service uses the active profile for content affinity and exposes it at
`GET /users/{user}/profile`. Missing, invalid, stale-version, or unreachable profile data safely
falls back to the established recommendation signals. Keep `USER_PROFILE_REDIS_KEY_PREFIX` on the
Spark job and `RECSYS_USER_PROFILE_KEY_PREFIX` on the service equal (both default to
`user-profile:v1`). Profile values default to a one-day TTL; the active-run pointer does not expire.
See [Data_Pipeline.md](docs/recommendation_architecture/Data_Pipeline.md#behavioral-user-profile-snapshots)
for input, decay, taxonomy, output, activation, metrics, and all environment variables, and
[API.md](docs/recommendation_architecture/API.md#get-usersuserprofile) for the response and 404
contract.

### Step 5 — Query the API

```bash
curl http://localhost:8080/recommend/user_1
curl http://localhost:8080/recommend/user_1?limit=10
curl http://localhost:8080/users/user_1/profile
curl http://localhost:8080/metrics
```

**Standalone alternative** — run the Python two-stage pipeline (two-tower retrieval + transformer ranking) without Spark or the Java service:

```bash
pip install torch onnx onnxruntime numpy
python services/python-modeling/movielens_pipeline.py
```

The first run trains and exports the three ONNX models; later runs reuse them. Optional flags:

```bash
python services/python-modeling/movielens_pipeline.py --user alice --top-k 5
python services/python-modeling/movielens_pipeline.py --user alice --user bob
python services/python-modeling/movielens_pipeline.py --force-train --model-dir /tmp/movielens-models

# Train from a real ratings CSV instead of the built-in synthetic catalog:
python services/python-modeling/movielens_pipeline.py \
    --ratings-csv sampledata/ratings.csv \
    --force-train

# Train, then write item embeddings to Redis (enables TwoTowerPredictionService):
python services/python-modeling/movielens_pipeline.py \
    --ratings-csv sampledata/ratings.csv \
    --save-embeddings-to-redis \
    --redis-host localhost
```

Run the cross-service integration tests:

```bash
pytest -q
```

---

## Automated Retraining

The `run-retrain.sh` script runs a full retraining pass and hot-reloads the ONNX model into the running Java service. It chains five stages:

1. **Replay export** — reads `replay:recommendations` from Redis and writes `sampledata/replay_training.csv`
2. **Spark ALS** — regenerates ALS item/user embeddings and writes them to Redis
3. **Spark UserEmbedding** — regenerates weighted-average user vectors in Redis
4. **Python two-tower** — fine-tunes the two-tower model on the merged dataset and writes item embeddings to Redis under the `twoTowerItemEmb:*` prefix
5. **Model hot-reload** — calls `POST /actuator/model-reload` to swap the ONNX model in the running Java service without restart

### One-Off Run

```bash
./scripts/run-retrain.sh
```

### Scheduled Run (cron)

Use the install script to add a cron job on the host machine:

```bash
# Install — runs every 6 hours, logs to /var/log/recsys-retrain.log
./scripts/install-cron.sh

# Custom schedule (every day at 2 AM)
./scripts/install-cron.sh --schedule "0 2 * * *" --log /var/log/recsys-retrain.log

# Preview the crontab entry without writing it
./scripts/install-cron.sh --dry-run

# Remove the cron job
./scripts/install-cron.sh --uninstall
```

The script is idempotent — running it twice does not create duplicate entries. Verify the installed entry with `crontab -l`.

To add the entry manually instead:

```
0 */6 * * * cd /path/to/recsys-pipeline && ./scripts/run-retrain.sh >> /var/log/recsys-retrain.log 2>&1
```

### Skip Individual Stages

```bash
./scripts/run-retrain.sh --skip-spark          # Only Python + reload
./scripts/run-retrain.sh --skip-python         # Only Spark + reload
./scripts/run-retrain.sh --skip-reload         # Train without hot-reload (offline mode)
```

### Dry Run

Preview the stages without training, writing embeddings, or hitting the service:

```bash
DRY_RUN=1 ./scripts/run-retrain.sh             # print each step; execute nothing
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis server port |
| `RECSYS_SERVICE_URL` | `http://localhost:8080` | Java retrieval service URL |
| `RATINGS_CSV` | `sampledata/ratings.csv` | Base training data |
| `MODEL_DIR` | `sampledata` | ONNX output directory |
| `DRY_RUN` | `0` | Set to `1` to print steps without executing |

### Replay Buffer Export

The `replay:recommendations` Redis list is populated by `ExperienceCollectorStreamingJob`. Run `replay_export.py` standalone to inspect or back up the buffer:

```bash
# 1. start the retrieval service
cd services/java-retrieval-service && mvn spring-boot:run    # binds :8080

# 2. in another shell — generate recommendations + feedback
curl 'http://localhost:8080/recommend/u_1?limit=6'
curl -X POST http://localhost:8080/feedback \
  -H 'Content-Type: application/json' \
  -d '{"user":"u_1","item":"movie_42","clicked":true,"reward":1.0}'

# 3. now the export has entries
python3 services/python-modeling/replay_export.py --output /tmp/replay_backup.csv --redis-host localhost

```

## API

REST endpoint reference (`/recommend`, `/users/{user}/profile`, `/predict`, `/feedback`, `/metrics`, `/embedding`) lives in
[API.md](docs/recommendation_architecture/API.md).

## Retrieval Pipeline

Before scoring, each request is enriched through two sequential pipelines — see [1_Query_Hydration.md](docs/recommendation_flows/1_Query_Hydration.md) for the query-hydration table, [4_Filtering.md](docs/recommendation_flows/4_Filtering.md) for the candidate filters, and [5_Candidate_Hydration.md](docs/recommendation_flows/5_Candidate_Hydration.md) for the candidate hydrators.

## Retrieval Service Configuration

`services/java-retrieval-service/src/main/resources/application.yml` defines Redis connectivity, in-memory cache settings, and recommendation parameters under `recsys`.

### Disk model paths

Set these to load model artifacts from the filesystem instead of the bundled classpath resources. When unset, falls back to classpath (the development and test default).

**MLP model** (`DeepLearningPredictionService`):

| Env var | Default | Description |
|---|---|---|
| `ONNX_MODEL_PATH` | *(classpath)* | Absolute path to `mlp_embedding_model.onnx` on the filesystem |
| `ONNX_LOOKUPS_PATH` | *(classpath)* | Absolute path to `mlp_embedding_lookups.json` on the filesystem |

**Two-tower model** (`TwoTowerPredictionService` — disabled unless all three are set):

| Env var | Default | Description |
|---|---|---|
| `ONNX_USER_TOWER_PATH` | *(unset)* | Absolute path to `movielens_user_tower.onnx` exported by `movielens_pipeline.py` |
| `ONNX_ITEM_TOWER_PATH` | *(unset)* | Absolute path to `movielens_item_tower.onnx` |
| `ONNX_RANKING_PATH` | *(unset)* | Absolute path to `movielens_ranking.onnx` (transformer re-ranker) |

### In-memory cache (FeatureCache)

| Property | Env var | Default | Description |
|---|---|---|---|
| `recsys.cache.item-vector-max-size` | `RECSYS_ITEM_VECTOR_CACHE_SIZE` | `10000` | Maximum cached item vectors |
| `recsys.cache.item-vector-ttl-seconds` | `RECSYS_ITEM_VECTOR_TTL` | `300` | Item vector TTL (seconds); matches training job cadence |
| `recsys.cache.reward-max-size` | `RECSYS_REWARD_CACHE_SIZE` | `50000` | Maximum cached reward model stat entries |
| `recsys.cache.reward-ttl-seconds` | `RECSYS_REWARD_TTL` | `30` | Reward model stat TTL; invalidated immediately on `/feedback` writes |

### Recommendation properties

**Embeddings**

| Property | Default |
|---|---|
| `recsys.embeddings.item-prefix` | `i2vEmb` |
| `recsys.embeddings.user-prefix` | `uEmb` |

**Bandit**

| Property | Default |
|---|---|
| `recsys.bandit.algorithm` | `ucb` |
| `recsys.bandit.exploration-alpha` | `0.75` |
| `recsys.bandit.max-exploration-bonus` | `0.25` |
| `recsys.bandit.cold-start-exposure-threshold` | `5` |
| `recsys.bandit.cold-start-boost` | `1.35` |
| `recsys.bandit.relevance-weight` | `0.6` |
| `recsys.bandit.content-weight` | `0.25` |
| `recsys.bandit.popularity-weight` | `0.15` |
| `recsys.bandit.deep-learning-weight` | `0.0` |
| `recsys.bandit.q-learning-alpha` | `0.1` |
| `recsys.bandit.q-learning-gamma` | `0.9` |
| `recsys.bandit.q-learning-epsilon` | `0.1` |

**Reward model**

| Property | Default |
|---|---|
| `recsys.reward-model.weight` | `0.25` |
| `recsys.reward-model.global-weight` | `0.15` |
| `recsys.reward-model.item-weight` | `0.45` |
| `recsys.reward-model.genre-weight` | `0.25` |
| `recsys.reward-model.tag-weight` | `0.15` |
| `recsys.reward-model.min-feature-count` | `3` |

Runtime overrides:

**Infrastructure**

| Env var | Default |
|---|---|
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `SERVER_PORT` | `8080` |

**Embeddings**

| Env var | Default |
|---|---|
| `ITEM_EMBEDDING_PREFIX` | `i2vEmb` |
| `USER_EMBEDDING_PREFIX` | `uEmb` |

**Bandit**

| Env var | Default |
|---|---|
| `RECSYS_BANDIT_ALGORITHM` | `ucb` |
| `RECSYS_EXPLORATION_ALPHA` | `0.75` |
| `RECSYS_MAX_EXPLORATION_BONUS` | `0.25` |
| `RECSYS_COLD_START_THRESHOLD` | `5` |
| `RECSYS_COLD_START_BOOST` | `1.35` |
| `RECSYS_RELEVANCE_WEIGHT` | `0.6` |
| `RECSYS_CONTENT_WEIGHT` | `0.25` |
| `RECSYS_POPULARITY_WEIGHT` | `0.15` |
| `RECSYS_DEEP_LEARNING_WEIGHT` | `0.0` |

**Reward model**

| Env var | Default |
|---|---|
| `RECSYS_REWARD_MODEL_WEIGHT` | `0.25` |
| `RECSYS_REWARD_GLOBAL_WEIGHT` | `0.15` |
| `RECSYS_REWARD_ITEM_WEIGHT` | `0.45` |
| `RECSYS_REWARD_GENRE_WEIGHT` | `0.25` |
| `RECSYS_REWARD_TAG_WEIGHT` | `0.15` |
| `RECSYS_REWARD_MIN_FEATURE_COUNT` | `3` |

**In-memory cache**

| Env var | Default |
|---|---|
| `RECSYS_ITEM_VECTOR_CACHE_SIZE` | `10000` |
| `RECSYS_ITEM_VECTOR_TTL` | `300` |
| `RECSYS_REWARD_CACHE_SIZE` | `50000` |
| `RECSYS_REWARD_TTL` | `30` |

**Disk model**

| Env var | Default |
|---|---|
| `ONNX_MODEL_PATH` | *(classpath fallback)* |
| `ONNX_LOOKUPS_PATH` | *(classpath fallback)* |

### Bandit algorithm notes

Moved to [6_Predicting_Scoring.md](docs/recommendation_flows/6_Predicting_Scoring.md#bandit-algorithm-notes) alongside the scoring model — covers how each of `ucb` / `thompson` / `q-learning` / `sarsa` turns `learnedPrior` into a `banditScore`.

### Real-time training write path

`/feedback` triggers online learning by updating reward statistics in Redis for the item, its genres, tags, and a global prior. Before batching, this was ~22 individual round-trips per feedback call. The current implementation collapses all writes into one:

```
GET  replay:pending:{user}:{item}           ← phase 1: read (before pipeline)
──────────────────────── pipeline (phase 2: write) ─────────────────────────
HINCRBY bandit:metrics clicks 1             ← if clicked
HINCRBY bandit:metrics:{algo} clicks 1
INCR    bandit:item:{item}:clicks
HINCRBY bandit:metrics reward_total {r}
HINCRBY bandit:metrics:{algo} reward_total {r}
HINCRBY reward-model:global count 1         ← online learning update
HINCRBY reward-model:global reward_total {r}
HINCRBY reward-model:item:{item} count 1
HINCRBY reward-model:item:{item} reward_total {r}
HINCRBY reward-model:genre:{g} ...          ← one pair per genre
HINCRBY reward-model:tag:{t} ...            ← one pair per tag
RPUSH   replay:recommendations {payload}    ← replay buffer
LTRIM   replay:recommendations -{max} -1
─────────────────────────────── flush ──────────────────────────────────────
featureCache.invalidateRewardStats(...)     ← phase 3: local cache purge
```

Replay and reward-model Redis keys:

| Key | Type | Contents |
|---|---|---|
| `replay:pending:{user}:{item}` | string | Last recommendation context for a served user-item pair |
| `replay:recommendations` | list | Rewarded replay events from `/feedback` |
| `reward-model:global` | hash | Global count and reward total |
| `reward-model:item:{item}` | hash | Per-item count and reward total |
| `reward-model:genre:{genre}` | hash | Per-genre count and reward total |
| `reward-model:tag:{tag}` | hash | Per-tag count and reward total |

## Recommendation Measurements

Measurement-only coverage of seven quality dimensions. **Nothing here changes candidate
generation, filtering, ranking, or selection** — the calculators read recorded outcomes, and the
Java service only observes the work it was already doing.

### Capture the inputs

`./scripts/run-movie-category-sim.sh` runs the whole capture-and-export sequence below as its last two
steps: it runs `ExperienceCollectorStreamingJob` with `EXPERIENCE_COLLECTOR_OUTPUT_PATH` set (so
slates land as Parquet, not just on the `training_experiences` Kafka topic), bursts traffic
against the retrieval service to populate `/metrics`, then exports and validates the snapshot —
one command, no manual steps. To capture the same inputs by hand from `recsys-pipeline/` against
a run already in progress:

```bash
# 1. live operational measurements from the running retrieval service
curl -s http://localhost:8080/metrics > /tmp/spark-recsys/live-metrics.json

# 2. offline + slate + live measurements from a run's outputs
IN=/tmp/spark-recsys/movie-category-sim
REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input "$IN/training-samples" \
  --experiences "$IN/slates" \
  --live-metrics "$IN/live-metrics.json" \
  --output frontend/data/dashboard.json
```

`/metrics` keeps every pre-existing key and adds `measurements` (schema `2.0`) with the live
latency, freshness, safety, and feedback-coverage sections. `--experiences` accepts a `.json` /
`.jsonl` file or a Parquet directory — `ExperienceCollectorStreamingJob` writes the slates
directly to Parquet under `$EXPERIENCE_COLLECTOR_OUTPUT_PATH` (no manual `kafka-console-consumer`
dump needed); without it, relevance and diversity stay N/A because they are the only measures
that need whole ranked slates.

### What each dimension measures

| Dimension | Measures | Denominator / support |
|---|---|---|
| Relevance | NDCG@k, MRR@k, leave-one-out recall@k and hit-rate@k | Slates whose items all carry labels; user folds for the LOO measures |
| Satisfaction | CTR, order rate, mean reward, mean rating, negative-feedback rate, dwell, completion | Recorded samples; each optional signal reports its own coverage |
| Freshness | Fresh share, mean/median content age, fresh vs established CTR and reward | Samples with an observed `published_at`, else the `new_release` boolean fallback |
| Diversity | Unique genres, normalized genre entropy, intra-list Jaccard distance, long-tail exposure share | Slate items with genres / popularity, averaged over slates |
| Fairness | Per-group exposure, CTR, order rate, reward, NDCG plus max-min gaps and disparity ratios | Candidates per group, after suppression |
| Safety | Filter-decision rate by reason, unknown share, unsafe exposure rate | Evaluated candidates; unsafe exposure over independently labeled exposures |
| Latency | Endpoint and stage p50/p95/p99, error and timeout rates | Requests recorded since service start |

### Availability rules

- Every section is an envelope: `status`, `headline`, `sampleSize`, `coverage`, `window`,
  `warnings`, `rows`.
- A missing signal is **unavailable with a named reason**, never zero. An observed `false` or `0`
  is a measured value and is reported as such.
- Every rate carries its denominator or sample size, and every optional signal carries coverage.
- Live rows are merged alongside the offline rows (`scope: offline` / `scope: live_service`); a
  live measurement never overwrites an offline one. A merged section's `sampleSize` and
  `coverage` describe the offline population; each live row carries its own denominator
  (`feedback_events`, `exposures`, `evaluated_candidates`).
- A live snapshot captured before any traffic contributes nothing: sections stay N/A rather than
  reporting "available" over zero observations.
- The diversity section publishes the all-slate aggregate plus a bounded sample of per-slate
  rows, and warns how many rows it dropped.

### Interpretation caveats

- **Fairness is observational.** Groups differ in catalog, intent, and traffic mix, so a gap is
  not evidence of discriminatory treatment. Groups below the configured minimum support
  (`--fairness-min-support`, default 100) are suppressed and only counted in
  `suppressed_group_count`.
- **Safety accounting is scoped to the catalog filter taxonomy** — `expired`,
  `muted_product_type`, `muted_genre`, `muted_keyword`, `muted_title`, and `unknown`. It records
  which policy rule rejected a candidate under `policy_version`; it is not a content-moderation
  verdict. `unsafe_exposure_rate` requires independently supplied `unsafe_label` values.
- **A filter decision is recorded for allowed candidates too, so `filter_decision_rate` is not a
  rejection rate.** `unknown` is the reason recorded when the policy has no catalog profile for a
  candidate and therefore could not classify it — the candidate still passes through.
  `ContentCandidateRetriever` returns `unknown` for any candidate missing from the catalog, so a
  service started without `RECSYS_CATALOG_PATH` covering the served items reports **100%
  `unknown`**. That is exactly what `run-movie-category-sim.sh` currently produces: it starts the
  retrieval service with the built-in demo catalog, which contains none of the sim's `movie_*`
  ids, so no expiry, muted-genre, or muted-keyword rule ever fires and the live safety row is
  entirely `unknown` (the inline catalog in `application.yml` holds `item1`…`itemN`). Read
  `unknown_share` alongside `filter_decision_rate` before drawing any
  conclusion from either. *Known follow-up: wire the sim's generated catalog into the service so
  the live safety row exercises the real rules.*
- **Latency is service time, not stream lag.** Endpoint/stage timers measure the request path.
  Pipeline delay is separate: `feedback_delay_ms` (impression → last feedback) and
  `kafka_ingest_lag_ms` (event time → Kafka record time) are emitted as Spark metric events.
- No user ID, item ID, request ID, or free-form reason is ever used as a metric label; filter
  reasons, countries, and subscription levels are bucketed to fixed allowlists.

### Configuration

| Variable | Default | Read by | Description |
|----------|---------|---------|-------------|
| `RECSYS_LATENCY_BUCKETS_MS` | `5,10,25,50,100,250,500,1000,2500` | retrieval service | Latency histogram boundaries |
| `RECSYS_SAFETY_POLICY_VERSION` | `catalog-filter-v1` | retrieval service | Version stamped on live safety accounting |
| `RECSYS_FAIRNESS_MIN_SUPPORT` | `100` | declared only | Minimum impressions before a fairness group is published |
| `RECSYS_FRESHNESS_WINDOW_DAYS` | `30` | declared only | Age below which an item counts as fresh |
| `RECSYS_LONG_TAIL_PERCENTILE` | `0.80` | declared only | Popularity percentile below which exposure is long-tail |
| `EXPERIENCE_COLLECTOR_OUTPUT_PATH` | unset (Parquet write disabled) | `ExperienceCollectorStreamingJob` | Directory to also write ranked slates as Parquet — the `--experiences` input; unset means slates publish only to the `training_experiences` Kafka topic |
| `MEASUREMENT_BURST_REQUESTS` | `50` | `run-movie-category-sim.sh` | Number of `/recommend` calls the sim's service burst makes to populate live latency/freshness/safety/feedback-coverage metrics |
| `RETRIEVAL_SERVICE_PORT` | `8080` | `run-movie-category-sim.sh` | Port the sim starts the retrieval service on for its traffic burst |
| `FEEDBACK_DELAY_SCALE` | `1.0` | live producers | Multiplies the click/order delays each slate already encodes, so a sim run can compress a ~2-minute feedback tail. For *orders* (21–120s base delay) against the streaming job's *default* 10-second trigger, any scale above ~0.5 still crosses it (21s × 0.5 = 10.5s). This does not hold for *clicks* (1–20s base delay): some clicks land under a 10-second trigger even at scale 1.0. The sims themselves run with `TRIGGER_INTERVAL="2 seconds"`, not the 10-second default, so cross-batch behavior in a sim run is governed by that 2-second figure, not this one |
| `FEEDBACK_TAIL_SECONDS` | `150` | segment and category sims | Floor before a drain may end, so the sim cannot declare completion before the last deferred order has arrived |
| `MOVIE_CATEGORY_LOOKBACK_DAYS`, `ENGAGEMENT_REPORT_LOOKBACK_DAYS`, `SEGMENT_REPORT_LOOKBACK_DAYS`, `SESSION_REPORT_LOOKBACK_DAYS`, `QUERY_ANALYSIS_LOOKBACK_DAYS`, `KEYWORD_ANALYSIS_LOOKBACK_DAYS`, `RELEVANCE_ANALYSIS_LOOKBACK_DAYS` | `30` | the matching report job | Most recent N partition dates of `training_samples` the report reads, anchored to the newest date present rather than the wall clock so re-running a historical report gives the same answer. `0` (or negative) reads all history — the previous behaviour, whose cost grew with total archive size rather than with the reported window |

`RECSYS_FAIRNESS_MIN_SUPPORT`, `RECSYS_FRESHNESS_WINDOW_DAYS`, and `RECSYS_LONG_TAIL_PERCENTILE`
describe offline calculations, so the retrieval service binds and validates them for a single
source of truth but does not act on them. **Set them on the exporter** —
`--fairness-min-support`, `--freshness-window-days`, `--long-tail-percentile`, and
`--safety-policy-version` — which is where those measurements are actually calculated.

`sampledata/catalog.json` carries both `publishedAt` (UTC ISO-8601, the timestamp freshness
source) and `newRelease` (the boolean fallback for services with no publication timestamp).

## Simulation Harnesses

End-to-end synthetic-stream sims drive the real pipeline (docker → producer → Spark jobs →
Parquet/Redis → PySpark report) and answer analytical questions. Each is a one-command script;
the producers embed a documented ground truth so the report can be validated against what was
generated.

| Sim | Script | Question answered |
|---|---|---|
| Engagement time-series | `run-engagement-sim.sh` | How does CTR trend over weeks (trend / weekly + diurnal seasonality / changepoint)? |
| User segments | `run-movielens-segment-sim.sh` | How does engagement differ by cohort / age / sex / education / geo / platform (+ `avg_rating`)? |
| Movie categories | `run-movie-category-sim.sh` | How does engagement differ by 3-level category (l1 genre family / l2 genre / l3 genre×decade)? |

Reports (`services/python-modeling/*_report*.py`, plus the Scala `com.demo.report.*` jobs) read the
resulting Parquet (joining Redis demographics/movie features where needed) and write per-segment CSVs.

> Run PySpark reports through the project's pinned Spark: `"$SPARK_HOME/bin/spark-submit"`. A
> mismatched pip `pyspark` (vs `$SPARK_HOME`) fails with `JavaPackage object is not callable`. See
> `docs/specs/` and `docs/plans/` for the per-sim specs/plans.
>
> `SPARK_HOME` must point at a **Spark 3.5.1 / Scala 2.12** distribution matching
> `services/spark-streaming-job/build.sbt` — not any pip-installed `pyspark`. A Scala 2.13 build
> (e.g. conda's `pyspark` 4.1.1) fails every streaming job in these sims with
> `NoSuchMethodError: ...wrapRefArray...`. That failure doesn't surface as a startup error — the
> job dies immediately but the sim's own drain loop keeps polling for up to `DRAIN_TIMEOUT`
> seconds (default 600) before giving up, so it presents as a silent multi-minute timeout. Check
> `$SIM_ROOT/*.log` (e.g. `redis.log`, `parquet.log`) first if a drain step stalls.
>
> These sims also need Docker running (`docker info` must succeed) — a Colima VM (`colima start`)
> works as a drop-in for Docker Desktop with no extra configuration.

## Analysis Reports

Offline reports over the engagement data (keyword/query/relevance distributions, recall/ranking
eval, off-policy evaluation, and a consolidated HTML dashboard). See
[Analysis_Report.md](docs/recommendation_architecture/Analysis_Report.md) for the full report table, shared definitions, and run
commands.

## Tests

| Service | Command (from repo root) | Covers |
|---|---|---|
| Spark jobs (Scala) | `cd services/spark-streaming-job && sbt test` | All streaming/offline jobs incl. recall/ranking/relevance derivations, session_id passthrough, dedup, event parsing |
| Retrieval service (Java) | `cd services/java-retrieval-service && mvn test` | Scoring, hydrators, behavioral-profile fixture contract, two-tower, catalog loader, model reload; real-Redis tests skip when Docker is unavailable |
| Python | `cd recsys-pipeline && pytest -q` | Producers, MovieLens pipeline, replay export, the simulation harnesses, and the analysis reports (query / relevance / recall-eval / ranking-eval / analysis-dashboard) |

The Scala suite includes a pure unit test for each derived-dataset job's `build*Samples` transform
(no Kafka/Redis needed). Some Python integration tests shell out to `"$SPARK_HOME/bin/spark-submit"`
and are skipped automatically when `SPARK_HOME` is unset.

## Cold-Start RL Extension Plan

See [3_Cold_Start.md](docs/recommendation_flows/3_Cold_Start.md) for the cold-start candidate generation details and the gradual RL extension plan (baseline → RL framing → DQN → Double DQN → Dyna-Q).

## Notes

- `docker-compose.yml` is for local development only; it runs Kafka and Redis without authentication.
- `run-streaming-job.sh` requires the assembled JAR at `services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar` (produced by `sbt assembly`).
- The retrieval service can serve recommendations without embeddings, but relevance scores will be zero until item and user vectors are loaded into Redis.

---

# Local Workflow Reference

> The canonical finite workflow below runs from `recsys-pipeline/`; optional
> reference workflows state their own working directories.

## End-to-end flow

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

## Components

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

## Canonical finite local workflow

### 1. Verify prerequisites

From `recsys-pipeline/`, verify the tools used by this workflow. Java 17, Spark 3.5 with
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

`spark-submit` must resolve to a **Spark 3.5.1 / Scala 2.12** distribution matching
`recsys-pipeline/services/spark-streaming-job/build.sbt` — set `SPARK_HOME` to that install, not
to a pip-installed `pyspark`. A Scala 2.13 build (e.g. conda's `pyspark` 4.1.1) fails every
streaming job with `NoSuchMethodError: ...wrapRefArray...`, which presents as a silent multi-minute
timeout rather than a startup error — see [Troubleshooting](#troubleshooting-the-local-workflow).
`docker compose version` only needs a working Docker daemon; a Colima VM (`colima start`) is a
drop-in for Docker Desktop here and needs no extra configuration.

### 2. Check host-port conflicts

From `recsys-pipeline/`, inspect the local ports before starting infrastructure:

```bash
lsof -nP -iTCP:6379 -sTCP:LISTEN
lsof -nP -iTCP:9092 -sTCP:LISTEN
lsof -nP -iTCP:2181 -sTCP:LISTEN
lsof -nP -iTCP:3000 -sTCP:LISTEN
```

No output means the port is free. If a command shows an owner, identify it and stop it only when
you know it belongs to this project; do not stop unrelated containers or services.

### 3. Start Kafka and Redis

From `recsys-pipeline/`:

```bash
docker compose up -d zookeeper kafka redis
docker compose ps
```

Do not continue until Kafka and Redis report `healthy`. Ports `9092`, `2181`, and `6379` are the
host listeners. A bind or readiness failure here is an infrastructure problem, not a Spark
failure.

### 4. Install dependencies and build the Spark artifact

From `recsys-pipeline/`:

```bash
python -m pip install -r services/python-modeling/requirements.txt
python -m pip install pandas pyarrow numpy redis
(cd services/spark-streaming-job && sbt assembly)
test -s services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

The final command exits successfully when the required fat jar exists.

### 5. Run the finite movie-category simulation

From `recsys-pipeline/`:

```bash
./scripts/run-movie-category-sim.sh
```

The harness is finite but can take several minutes. It resets this Compose project's volumes and
Redis state, generates movie metadata and engagement samples, runs the Spark jobs, creates
embeddings, and writes the standalone HTML report. Its primary Parquet output is
`/tmp/spark-recsys/movie-category-sim/training-samples`.

### 6. Wait for the completion signal

Do not export a snapshot while the harness is still draining Kafka or writing Parquet. Continue
only after the simulation prints the literal terminal line:

```text
==> done. CSVs under /tmp/spark-recsys/movie-category-sim/report-categories ; dashboard at /tmp/spark-recsys/movie-category-sim/report-dashboard/index.html
```

Keep Redis running after this line; the exporter still needs `movie:*:features`, `i2vEmb:*`, and
`uEmb:*`.

### 7. Export and validate the React snapshot

From `recsys-pipeline/`:

```bash
# Optional: capture live latency/freshness/safety/feedback coverage while the service runs.
curl -s http://localhost:8080/metrics > /tmp/spark-recsys/live-metrics.json

# The ranked slates that relevance and diversity need: ExperienceCollectorStreamingJob writes
# them straight to Parquet under $EXPERIENCE_COLLECTOR_OUTPUT_PATH — run-movie-category-sim.sh
# sets this (and captures /metrics above) automatically, so no manual Kafka dump is needed.
REDIS_HOST=localhost python frontend/export_dashboard_json.py \
  --input /tmp/spark-recsys/movie-category-sim/training-samples \
  --experiences /tmp/spark-recsys/movie-category-sim/slates \
  --live-metrics /tmp/spark-recsys/live-metrics.json \
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

cd frontend && npm run validate:data      # measurement-contract gate (also runs in `npm run build`)
```

Success is a `snapshot valid:` line with positive row and L1 counts, followed by
`dashboard.json valid: 7 measurement sections`. `frontend/data/dashboard.json` is the output
consumed by the React app.

The snapshot carries seven measurement sections — relevance, satisfaction, freshness, diversity,
fairness, safety, and latency — alongside the engagement/keyword/query/recall/ranking/OPE/MDP
diagnostics. Note the schema change at `schemaVersion: "2.0"`: `relevance` now holds the listwise
measurement envelope (NDCG/MRR), and the engagement funnel it used to hold moved to `engagement`. Any section whose inputs are missing reports `"status": "unavailable"` with an
explicit reason and renders an N/A card; nothing is zero-filled. See
[Recommendation Measurements](#recommendation-measurements) for the
metric definitions, denominators, interpretation caveats, and configuration variables.

### 8. Launch and refresh the React dashboard

From `recsys-pipeline/`:

```bash
cd frontend
npm install
npm run dev
```

This development server is intentionally long-running. Open `http://localhost:3000`. After
regenerating `frontend/data/dashboard.json`, hard-refresh the browser; if it still shows the old
row count, stop the server with `Ctrl-C` and run `npm run dev` again.

### 9. Stop processes and infrastructure

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

Finally, from `recsys-pipeline/`, stop only this repository's Compose project:

```bash
docker compose down
```

## Optional reference: manual streaming data pipeline

This path is useful for inspecting individual Kafka/Spark stages, but it is not the canonical
dashboard workflow. Run each producer or Spark command from the repository root in a separate
terminal; all Spark consumers are long-running until `Ctrl-C`.

```bash
cd recsys-pipeline

# Finite producer check: exits after 100 records.
EVENTS_PER_SECOND=20 LOG_EVERY=10 MAX_EVENTS=100 \
  python services/python-modeling/producer.py

# Individual long-running consumers:
./scripts/run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.OnlineJoinerStreamingJob ./scripts/run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.ExperienceCollectorStreamingJob ./scripts/run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.RecallSampleStreamingJob    ./scripts/run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.RankingSampleStreamingJob   ./scripts/run-streaming-job.sh
SPARK_MAIN_CLASS=com.demo.process.RelevanceSampleStreamingJob ./scripts/run-streaming-job.sh

# Or launch the operable Avro vertical slice (clickstream producer + archive/popularity consumer):
./scripts/run-data-pipeline.sh
```

Spark streaming jobs wait when `recsys_events` has no records. Zero Kafka offsets mean a producer
has not emitted to that topic; they do not prove a consumer crash. Topic names must match across
each producer and consumer. The individual derived-topic jobs above remain manually operated JSON
pipelines; provision their `training_*` topics separately before launching them because the Avro
catalog intentionally contains only `recsys_events` and `recsys_events.backfill`.

## Optional reference: standalone HTML dashboard

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
[Analysis Reports](docs/recommendation_architecture/Analysis_Report.md) for focused
report commands.

## Troubleshooting the local workflow

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
| A drain step (e.g. `[redis]`/`[parquet]`) polls for minutes and never reaches its target | `grep -i wrapRefArray /tmp/spark-recsys/movie-category-sim/*.log` | `SPARK_HOME` is a Scala 2.13 build (mismatched vs. `build.sbt`'s Scala 2.12); the job crashed on startup and the drain loop is polling a job that already died | point `SPARK_HOME` at a Spark 3.5.1 / Scala 2.12 install and rerun |

---

## Optional reference: modeling pipeline — Spark embeddings + Python two-tower → ONNX

This optional reference trains embeddings offline and produces the ONNX model the retrieval
service serves. Run the block from the repository root.

```bash
cd recsys-pipeline
# Item2Vec item embeddings (writes sampledata/item_embedding.txt + Redis)
RATINGS_INPUT_PATH=sampledata/ratings.csv ./scripts/run-offline-pipeline.sh

# ALS collaborative-filtering embeddings
RATINGS_INPUT_PATH=sampledata/ratings.csv ./scripts/run-als-pipeline.sh

# User embeddings (needs the item embedding file from the offline step)
RATINGS_INPUT_PATH=sampledata/ratings.csv \
  ITEM2VEC_EMBEDDING_PATH=sampledata/item_embedding.txt \
  ./scripts/run-user-embedding-pipeline.sh

# Export the Redis replay buffer back to a ratings CSV (for retraining)
python services/python-modeling/replay_export.py

# Full retrain: replay export → ALS → user emb → two-tower → hot-reload
./scripts/run-retrain.sh                       # flags: --skip-spark --skip-python --skip-reload; DRY_RUN=1
```

The retrain's final step hot-reloads the ONNX model in the running service (`:8080`):

```bash
curl -X POST http://localhost:8080/actuator/model-reload
```

---

<a id="3-experiment-pipeline--retrieval-service-8080"></a>

## Optional reference: experiment pipeline — retrieval service `:8080`

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

## Offline policy evaluation

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

## Ports

| Port | Service | Bound by | Notes |
|------|---------|----------|-------|
| `8080` | Retrieval service (Spring Boot) | `java-retrieval-service` (`SERVER_PORT`) | REST API: `/recommend`, `/predict`, `/feedback`, `/metrics` |
| `9092` | Kafka — host listener | `docker-compose.yml` | Producer and Spark jobs connect here (`localhost:9092`) |
| `29092` | Kafka — internal listener | `docker-compose.yml` | Inter-container only (`kafka:29092`) |
| `2181` | Zookeeper | `docker-compose.yml` | Kafka coordination |
| `6379` | Redis | `docker-compose.yml` (`REDIS_PORT`) | Embeddings, counters, user history |

> The Spark driver UI also binds `4040` (incrementing if taken) while a streaming/training job runs.


---
