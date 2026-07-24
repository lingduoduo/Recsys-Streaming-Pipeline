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
docker compose up -d
```

### Step 1 — Offline embeddings

> Run once before starting the retrieval service.

Train Item2Vec embeddings and write them to Redis (`i2vEmb:{itemId}` keys):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv \
ITEM2VEC_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
./run-offline-pipeline.sh
```

Train user embeddings from the item vectors above and write to Redis (`uEmb:{userId}` keys):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv \
ITEM2VEC_EMBEDDING_PATH=sampledata/item_embedding.txt \
USER_EMBEDDING_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
./run-user-embedding-pipeline.sh
```

**Optional** — train ALS collaborative-filtering embeddings (`alsItemEmb:*`, `alsUserEmb:*`):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv \
ALS_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
./run-als-pipeline.sh
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
./run-streaming-job.sh
```

This populates `global:item_popularity` in Redis in real time; the retrieval service uses it as a popularity signal. (Per-user recency comes from `user:{id}:features`, written by `MovieLensContextCollectorStreamingJob`.)

### Step 5 — Query the API

```bash
curl http://localhost:8080/recommend/user_1
curl http://localhost:8080/recommend/user_1?limit=10
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
./run-retrain.sh
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
0 */6 * * * cd /path/to/recsys-pipeline && ./run-retrain.sh >> /var/log/recsys-retrain.log 2>&1
```

### Skip Individual Stages

```bash
./run-retrain.sh --skip-spark          # Only Python + reload
./run-retrain.sh --skip-python         # Only Spark + reload
./run-retrain.sh --skip-reload         # Train without hot-reload (offline mode)
```

### Dry Run

Preview the stages without training, writing embeddings, or hitting the service:

```bash
DRY_RUN=1 ./run-retrain.sh             # print each step; execute nothing
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

REST endpoint reference (`/recommend`, `/predict`, `/feedback`, `/metrics`, `/embedding`) lives in
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

## Analysis Reports

Offline reports over the engagement data (keyword/query/relevance distributions, recall/ranking
eval, off-policy evaluation, and a consolidated HTML dashboard). See
[Analysis_Report.md](docs/recommendation_architecture/Analysis_Report.md) for the full report table, shared definitions, and run
commands.

## Tests

| Service | Command (from repo root) | Covers |
|---|---|---|
| Spark jobs (Scala) | `cd services/spark-streaming-job && sbt test` | All streaming/offline jobs incl. recall/ranking/relevance derivations, session_id passthrough, dedup, event parsing |
| Retrieval service (Java) | `cd services/java-retrieval-service && mvn test` | Scoring, hydrators, two-tower, catalog loader, model reload |
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
