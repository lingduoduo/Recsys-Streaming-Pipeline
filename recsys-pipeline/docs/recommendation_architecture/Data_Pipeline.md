# Data Pipeline

Spark Structured Streaming and offline embedding jobs: ingest Kafka click and behavior events, join
impressions with feedback into feature+label training samples, train Item2Vec and ALS embeddings
from historical ratings, and keep per-user history and global item popularity fresh in Redis.

Unless a command block changes directory explicitly, run it from the repository's
`recsys-pipeline/` directory.

![Feature pipeline reference architecture](feature.png)

*Reference feature-store architecture: online events (Kafka) and offline events flow through a
filter → featurize → enrich → transform job into an online/offline signal store that the serving
API consumes. This repo implements the realtime path with **Spark Structured Streaming** (not the
Flink shown above) and uses Redis as the online signal store.*

## Data Flow

```text
── Streaming features ───────────────────────────────────────────────────────────────────────

producer.py (clickstream, user_id key) ──► Kafka: recsys_events ──► UserEventStreamingJob ──► Redis global:item_popularity

producer.py (behavior, request_id key)  ──► Kafka: recsys_events ──► OnlineJoinerStreamingJob ──► Kafka: training_samples
                                                                                             └──► Parquet training-samples/date=YYYY-MM-DD/

(default topic = recsys_events for producer + both jobs; set KAFKA_TOPIC / ONLINE_JOINER_INPUT_TOPIC to split the two streams)

Kafka: training_samples     ──► ExperienceCollectorStreamingJob  ──► Kafka: training_experiences
Kafka: training_experiences ──► RecommendationResponseStatsJob   ──► Kafka: recommendation_metrics
Kafka: movielens_context    ──► MovieLensContextCollectorStreamingJob ──► Redis user context (serving hydration)
                                                                        └──► Redis movie context (training/report enrichment)

── Offline embeddings ───────────────────────────────────────────────────────────────────────

ratings.csv ──► ItemSequencePreprocessingJob ──► Item2VecTrainingJob ──► embedding.txt
                                                                     └──► Redis i2vEmb:{item}

ratings.csv + embedding.txt ──► UserEmbeddingTrainingJob ──► user_embedding.txt
ratings.csv                 ──► AlsEmbeddingTrainingJob  ──► als/userFactors + als/itemFactors

user_embedding.txt + item_embedding.txt ──► EmbeddingCandidateGenerationJob ──► Redis user:{id}:candidates  (top-K cosine)
                                                                             └──► Parquet candidate-generation/
```

> Note: serving query hydration reads `user:{id}:features`, not
> `movie:{id}:features`. The movie hashes feed the derived relevance dataset and Python
> training/report enrichment. `user:{id}:candidates` (written by
> `EmbeddingCandidateGenerationJob`) is also not currently read by the retrieval service.

## Storage Architecture

Feature data is split across three tiers by access pattern and update frequency.

| Tier | Contents | Updated by |
|------|----------|------------|
| **Disk** (filesystem) | ONNX model (`mlp_embedding_model.onnx`), ID lookup tables (`mlp_embedding_lookups.json`), two-tower ONNX models (`movielens_user_tower.onnx`, `movielens_item_tower.onnx`, `movielens_ranking.onnx`), Parquet training samples partitioned by date | Training jobs; swappable at runtime via `ONNX_MODEL_PATH` without JAR rebuild |
| **Redis** | User click history, global item popularity, per-user columnar rating/click sequences (`seq:{id}:{kind}:{day}`), item/user embeddings (`i2vEmb:*`, `uEmb:*`, `alsItemEmb:*`, `alsUserEmb:*`, `twoTowerItemEmb:*`), bandit counters, reward model stats, replay buffer | Streaming jobs (each micro-batch) and `/feedback` calls |
| **In-memory** (Caffeine) | Item vectors (`i2vEmb:*`), reward model stats (`reward-model:*`) | Populated from Redis on first request; TTL-expired; invalidated on `/feedback` writes |

The in-memory cache (`FeatureCache`) eliminates O(N × features) Redis round-trips per recommendation request. Before the scoring loop, a single `MGET` loads all candidate and recent-item vectors; reward model estimates are cached per key for the configured TTL and invalidated immediately when `/feedback` updates them.

**Disk model hot-swap** — set `ONNX_MODEL_PATH` and `ONNX_LOOKUPS_PATH` to replace the MLP model artifacts on the filesystem without rebuilding the JAR. The service falls back to classpath resources when the env vars are unset (the development and test default). To enable the two-tower scoring path, set `ONNX_USER_TOWER_PATH`, `ONNX_ITEM_TOWER_PATH`, and `ONNX_RANKING_PATH` to the three ONNX files exported by `movielens_pipeline.py`.

## Avro Kafka ingestion, archive, and replay

`recsys_events` is the live canonical-event topic. Its only replay counterpart is
`recsys_events.backfill`; this is a separate topic, not a mode on the live topic. Kafka
auto-creation is disabled. Start the local services, then provision both catalog entries after
Kafka is healthy and before starting producers or consumers:

```bash
docker compose up -d
python scripts/provision-kafka-topics.py --bootstrap-server localhost:9092
```

For a host installation, that command requires `kafka-topics` and `kafka-configs` on `PATH`. For
the local Docker Compose service, run the same provisioner in its explicit in-container mode:

```bash
python scripts/provision-kafka-topics.py \
  --bootstrap-server localhost:9092 --command-mode docker-compose
```

The opt-in live round-trip test needs the Python modeling requirements, an assembled Spark JAR,
and reachable Kafka plus Redis:

```bash
python -m pip install -r services/python-modeling/requirements.txt pytest
(cd services/spark-streaming-job && sbt assembly)
RUN_KAFKA_INTEGRATION=1 KAFKA_INTEGRATION_COMMAND_MODE=docker-compose \
  pytest -q integration-tests/test_avro_kafka_round_trip.py
```

`KAFKA_INTEGRATION_COMMAND_MODE=host` is the default when the host Kafka CLIs are available;
`docker-compose` requires the local Compose Kafka service. With `RUN_KAFKA_INTEGRATION` unset,
the test cleanly skips without importing Kafka/Spark/Parquet dependencies. With it set, any absent
package, JAR, Kafka/Redis endpoint, or provisioning mode skips with an exact remediation message.

`./scripts/run-data-pipeline.sh` performs that same health-then-provision sequence before it
starts the clickstream producer and `UserEventStreamingJob`. That wrapper is intentionally limited
to the direct Avro vertical slice: `recsys_events` → archive/Redis popularity. Topic creation is
idempotent, so `run-streaming-job.sh` also verifies the default consumer's catalog. For a host
without Kafka CLIs, the scripts use the local Docker Compose Kafka CLI only for
`localhost:9092`/`127.0.0.1:9092`; install the CLIs for any other broker endpoint.

The derived-topic jobs are not part of this wrapper. `OnlineJoinerStreamingJob` and
`ExperienceCollectorStreamingJob` retain their standalone launch commands and their
`training_samples`/`training_experiences` payloads are legacy JSON. Before running those jobs, an
operator must separately create and govern every legacy input/output topic they select; the Avro
catalog deliberately provisions only `recsys_events` and `recsys_events.backfill`.

### Topic policy and capacity boundary

The checked-in catalog is `config/kafka-topics.json`. `retention.bytes` is Kafka's **per-partition**
ceiling, while `storage_budget_bytes` is a separate whole-topic replicated-storage validation
budget. The provisioner rejects a policy before running Kafka commands unless:

```text
ceil(messages_per_second × average_record_bytes × 86400 × retention_days
     × replication_factor × overhead_factor) <= storage_budget_bytes
```

The current values are a committed development policy, not a 1M messages/second capacity claim.
`recsys_events` has three live partitions and one day / 500 MB-per-partition retention; the
one-partition `recsys_events.backfill` target has six hours / 250 MB-per-partition retention.
Adjust topic-policy inputs and validate storage before treating any higher workload as supported.

### Writer schema and dead letters

Producers emit Avro single-object messages: marker `c3 01`, followed by the eight-byte
little-endian CRC-64-AVRO parsing fingerprint, then the record. The shared schema is
`schemas/recsys-event-v1.avsc`; its current fingerprint is `225b275f487979ab`. Required fields
are `event_id`, `user_id`, `item_id`, `event_type`, and `timestamp_ms`.

Schema changes must be backwards-compatible additions with Avro defaults. Do not rename, remove,
or change the type of existing fields. The Spark reader accepts only fingerprints registered in its
local schema catalog, and archive replay requires every selected row's fingerprint to match the
local writer schema. Therefore deploy a compatible producer/consumer schema change together and
do not replay an archive under an unregistered schema.

Malformed records are archived separately at `RECSYS_EVENT_DEAD_LETTER_PATH` with Kafka topic,
partition, offset, timestamp, optional headers, `raw_value`, `schema_fingerprint`, `error_code`,
and `error_detail`. Codes are `invalid_marker`, `unknown_fingerprint`, `corrupt_payload`, and
`required_field`. The fingerprint is retained when a valid single-object header supplies one;
`invalid_marker` has no fingerprint.

### Archive and bounded replay operations

Run ingestion with explicit archive roots:

```bash
RECSYS_EVENT_ARCHIVE_PATH=/data/recsys-events \
RECSYS_EVENT_DEAD_LETTER_PATH=/data/recsys-events-dead-letter \
  ./scripts/run-streaming-job.sh
```

Archive roots require a filesystem with atomic, non-overwriting directory rename semantics (local
or HDFS via Hadoop `FileContext`). Do not place this archive on an object store that lacks that
atomic rename contract. Valid and dead-letter data is partitioned by UTC date below a
checkpoint-hash namespace.

A batch is consumable only when its directory has both `_SUCCESS` and the version-2 `_COMMITTED`
manifest with the expected query, kind, batch identity, row count, and exact Parquet inventory.
Every inventory entry records relative path, byte size, and SHA-256 digest, and replay validates
all three before publishing. An explicit zero row count and empty inventory is a committed
all-invalid batch, not an incomplete write. Protocol v1 was a pre-release archive format: replay
rejects it with an instruction to regenerate the data, and there is no v1 migration or
backward-compatibility path.

Replay takes an explicit half-open UTC date range (`start <= date < end`). It enforces its row
limit before creating a producer, rate-limits sends, and strips archive/Kafka lineage before
encoding the canonical event again. It can only target `recsys_events.backfill`, and it requires
the query namespace and durable operation identity rather than inferring them:

```bash
REPLAY_ARCHIVE_PATH=/data/recsys-events \
REPLAY_ARCHIVE_QUERY_NAMESPACE=<checkpoint-hash> \
REPLAY_OPERATION_ID=incident-2026-08-01 \
REPLAY_START_DATE=2026-08-01 REPLAY_END_DATE=2026-08-02 \
REPLAY_MAX_ROWS=100000 REPLAY_RECORDS_PER_SECOND=5000 \
  ./scripts/run-archive-replay.sh
```

Only numeric `_queries/<selected-query>/_batches/<batch>` directories with `_SUCCESS` and an exact
version/query/kind/batch/row-count/inventory `_COMMITTED` manifest are read. Orphan attempts, dedupe snapshots,
incomplete batches, and batches owned by another query are excluded; a missing or ambiguous query
identity is rejected.

Verification is scoped to the batches a replay reads. Each batch declares its partition dates in its
`_COMMITTED` inventory; a batch declaring no date in the requested range is skipped before anything
inside it is opened or hashed, so replaying one day costs one day rather than the whole archive.
Every batch that is read is validated in full, so no byte is published unverified. Two consequences
follow:

- **A damaged batch outside the requested range no longer blocks an unrelated recovery.** Replay is
  not a whole-archive integrity audit, so out-of-range damage will not surface here.
- **Pruning trusts the manifest, never the directory listing.** A batch whose declared partition has
  been deleted still fails validation instead of being mistaken for an empty batch. A declaration
  that cannot be trusted — missing, unparseable, wrong version, or claiming rows while listing no
  files — keeps its batch eligible, so full validation runs.

Set `REPLAY_MANIFEST_DIR` to choose the operation directory; otherwise the deterministic manifest
is written to `$REPLAY_ARCHIVE_PATH/_replay_manifests/<operation-id>.json`. It records the stable
operation ID, status, immutable selection contract, ordered source signature, acknowledged
physical `(file path, row group, row)` cursor,
schema fingerprints, timestamps, and any error. Reusing a completed operation ID is a no-op;
reusing an interrupted one resumes after the last persisted acknowledgement. Each record retains
`event_id`, uses `operation_id:event_id` as its Kafka key, and carries `replay_operation_id` and
`replay_event_id` headers.

This is at-least-once replay, not exactly-once. The cursor is persisted after every broker
acknowledgement, but a crash between that acknowledgement and cursor persistence can publish one
record again. Backfill consumers must deduplicate `event_id` or the stable operation/event identity.
Set `REPLAY_OVERRIDE_LIMIT=1` only after reviewing the selection. Consumers stay on the live topic
unless explicitly configured for the backfill stream, for example:

```bash
KAFKA_TOPIC=recsys_events.backfill ./scripts/run-streaming-job.sh
```

### Dead-letter re-drive operations

Of the four dead-letter codes, only some describe permanently bad data. `unknown_fingerprint` does
not: it means a producer shipped a schema the consuming catalog did not know yet. Once the catalog
knows it, those same bytes decode cleanly, and this command republishes them.

**Deploy the catalog fix to both the producer and the Spark job before re-driving.** The eligibility
gate proves a record decodes under the Python catalog; the pipeline decodes under the Scala one.
Both resolve the same checked-in `.avsc` by fingerprint, but they are two implementations.

```bash
REDRIVE_ARCHIVE_PATH=/data/recsys-events-dead-letter \
REDRIVE_ARCHIVE_QUERY_NAMESPACE=<checkpoint-hash> \
REDRIVE_OPERATION_ID=redrive-2026-08-10 \
REDRIVE_START_INGEST_DATE=2026-08-09 REDRIVE_END_INGEST_DATE=2026-08-10 \
REDRIVE_MAX_ROWS=50000 REDRIVE_RECORDS_PER_SECOND=2000 \
  ./scripts/run-dead-letter-redrive.sh
```

Bounds are **ingestion** dates, taken from the `kafka_timestamp` partition, because a record that
never decoded may have no usable event time. This differs from replay, whose bounds are event time;
the flags are named `--start-ingest-date` / `--end-ingest-date` so the two cannot be confused.

Eligibility is decided by re-decoding each row against the current catalog and applying required-field
validation, never by its recorded `error_code`. Nothing is published that would dead-letter again on
arrival, so `corrupt_payload` and `invalid_marker` rows can never be recovered, and a mislabeled row
whose bytes are fine still can be. Published values are the archived bytes verbatim, preserving the
original writer fingerprint; they are not re-encoded.

The manifest at `$REDRIVE_ARCHIVE_PATH/_redrive_manifests/<operation-id>.json` records selected,
examined, published, and skipped counts plus a per-`error_code` breakdown of everything skipped. A
run that publishes nothing reports why. The cursor is the physical `(file path, row group, row)`
position and advances over skipped rows too, so an interrupted operation resumes without
re-examining them; a completed operation rerun is a no-op. Keys and headers match replay
(`operation_id:event_id`, `replay_operation_id`, `replay_event_id`) so backfill consumers need no
change, with an extra `redrive_error_code` header carrying the original classification.

Re-drive is at-least-once for the same reason replay is, and the dead-letter archive is append-only:
a successful re-drive neither deletes nor marks its source rows. The manifest is the record of what
was recovered.

### Durable sink and retry contract

The migrated Avro engine scopes business completion to the stable
`(query identity, sink identity, batchId)` tuple. It rejects an unrecognized or duplicate sink
identity before archival or business effects, and a successful sink is not invoked again when a
later sink fails and Spark retries the same batch.

**Parquet.** Sinks atomically publish deterministic visible
`query=<hash>/sink=<hash>/batch=<id>` directories, so payload schemas must not contain the reserved
control columns `query`, `sink`, or `batch`. One root may hold several query/sink identities, so a
reader must not perform schema discovery at the configured root. Scala/Spark readers instead use
`DurableParquetCommit.readIdentity` with `spark`, `configuredRoot`, `queryNamespace`,
`sinkNamespace`, and `expectedPayloadSchema`: the helper resolves the exact visible
`query=<hash>/sink=<hash>` identity path, sets `basePath` to `configuredRoot`, and applies the
explicit payload schema. The returned frame keeps the visible `query`, `sink`, and `batch`
partition columns, so callers may filter `batch` after the identity-safe read.

**Redis.** Popularity increments and sequence hash updates atomically pair each effect with a
ledger field in that batch's hash. A bounded sorted-set index retains only the configured batch
window, and a stable per-query/sink watermark fences delayed work below the recovery horizon. The
window is at least two batches, preserves N and N-1 retries, and is pruned only after the newer
batch completes, so an executing higher batch cannot prune older recovery state. Each sequence
effect atomically renews the target plus the state, index, current ledger, and every retained
ledger. Control keys share a retry horizon one second longer than the target, then expire.

**Kafka.** Output enables producer idempotence and carries a stable key plus query/sink/batch
headers. A later producer session may still repeat a record after a partial batch failure, so
derived-topic consumers must deduplicate that stable key.

These are retry-safe per-sink contracts, not a cross-system exactly-once transaction.

## Derived ML Datasets

All three jobs below consume `training_samples` (the OnlineJoiner output, which now carries
`session_id` end-to-end) and emit one row per recommended impression to a new Kafka topic. They
are additive — no existing topic/schema changed — and run standalone via `run-streaming-job.sh`.

```text
Kafka: training_samples ──► RecallSampleStreamingJob     ──► Kafka: recall_samples
Kafka: training_samples ──► RankingSampleStreamingJob    ──► Kafka: ranking_samples     (+ Redis uEmb/i2vEmb)
Kafka: training_samples ──► RelevanceSampleStreamingJob  ──► Kafka: relevance_samples   (+ Redis movie:{id}:features)
```

| Job | Topic | Row schema |
|---|---|---|
| `RecallSampleStreamingJob` | `recall_samples` | `user_id`, `session_id`, `event_ts`, `recommended_movie_id`, `click_movie_id` (null unless clicked), `rating` (label) |
| `RankingSampleStreamingJob` | `ranking_samples` | + `user_features`/`item_features` maps, `user_embedding` (`uEmb:{user}`), `item_embedding` (`i2vEmb:{item}`), `is_click`, `rating` |
| `RelevanceSampleStreamingJob` | `relevance_samples` | LTR shape: `query` (`user_id:session_id`, or `user_id:request_id` when the sample has no session), `recommended_movie_id`, `title`/`genres`/`release_year` (from `movie:{id}:features`), `score` (label) |

`rating`/`score` is the implicit engagement label (click → 1.0, order → 2.0, else 0.0). The ranking
and relevance jobs join Redis per micro-batch (embeddings / movie metadata); missing keys yield an
empty vector / null fields. Env knobs: `{RECALL,RANKING,RELEVANCE}_INPUT_TOPIC` (default
`training_samples`), `{RECALL,RANKING,RELEVANCE}_OUTPUT_TOPIC`, and (ranking) `USER_EMBEDDING_PREFIX`
/ `ITEM_EMBEDDING_PREFIX`.

```bash
SPARK_MAIN_CLASS=com.demo.process.RankingSampleStreamingJob ./run-streaming-job.sh
```

## Behavioral User-Profile Snapshots

`UserBehaviorProfileBatchJob` turns a bounded Parquet history into one deterministic,
explainable version-one profile per user. It is an offline snapshot job: it writes an immutable
Parquet run, publishes run-scoped values to Redis, and changes the serving pointer only after every
profile write has succeeded.

### Input and evidence rules

The input is Parquet with the following fields. Missing optional columns are added as nulls before
validation.

| Field | Use |
|---|---|
| `sample_id` | Preferred deduplication identity; when absent, the job hashes the normalized user, request, item, timestamp, click/order flags, and rating |
| `request_id`, `user_id`, `item_id` | Event identity and grouping; blank/null users are rejected |
| `impression_ts` | Epoch seconds; null/invalid or outside the half-open source window is rejected |
| `clicked`, `ordered` | Nullable booleans normalized to `false` and used for rates and evidence weight |
| `rating` | Optional explicit rating; values outside the configured rating range are rejected |
| `genres`, `tags` | Optional term arrays, trimmed, lower-cased, deduplicated, and sorted |
| `new_release`, `published_at` | Optional freshness evidence; `published_at` determines recency when present, otherwise `new_release` is used |

Equivalent valid events are deduplicated after validation. Each surviving event contributes exactly
one base weight using strongest-evidence precedence: **rating**, then **order**, then **click**, then
**impression**. Ratings are mapped around the configured midpoint to `[-1, 1]`; the other default
weights are `3.0`, `1.0`, and `-0.1`. Every base weight is then decayed by
`0.5^(ageSeconds / halfLifeSeconds)`. Genre/tag scores are the decayed weight sum divided by
`evidence_count + shrinkage`, clamped to `[-1, 1]`, and ordered by descending score with the
normalized value as the stable tie-breaker. Non-positive preferences remain in the explanatory
profile but are not used as serving affinities.

### Output and activation

`USER_PROFILE_OUTPUT_PATH` is one immutable Spark Parquet directory (the job uses
`errorifexists`, not date partitions). Each row contains the public contract columns
`user_id`, `profile_version`, `run_id`, `generated_at`, `source_window`, `evidence_count`,
`preferences`, `behavioral_features`, and `personas`; it also retains flattened counts/features
and `profile_json`, the exact Redis/API JSON payload. A successful run prints one structured
`user_profile_run_completed` event with `run_id`, input, valid, rejected, deduplicated and profile
counts, plus the output path.

Redis publication uses this protocol:

1. Pipeline `SET EX` for every `USER_PROFILE_REDIS_KEY_PREFIX:{runId}:{userId}` profile.
2. Synchronize each partition and check every deferred Redis command response.
3. Only after all partitions succeed, set `USER_PROFILE_REDIS_KEY_PREFIX:active-run` to `runId`.

Profile values expire after `USER_PROFILE_REDIS_TTL_SECONDS`; the active pointer intentionally has
no TTL. If profile publication fails, the pointer is not advanced, so readers continue using the
previous complete run.

### Personas

Users below `USER_PROFILE_MINIMUM_EVIDENCE` receive only `new_or_unknown`. Otherwise classification
is deterministic and multi-label:

| Type | Default rule |
|---|---|
| `genre_enthusiast` | Top positive genre score >= `0.6` |
| `genre_explorer` | Genre diversity >= `0.6` and concentration <= `0.5` |
| `focused_viewer` | Preference concentration >= `0.7` |
| `recent_release_seeker` | Recent-release affinity >= `0.6` |
| `high_intent_engager` | Engagement >= `0.4` and conversion >= `0.1` |
| `casual_browser` | Enough impressions, engagement <= `0.1`, and conversion <= `0.02` |

Every persona includes a bounded confidence and the named evidence used by its rule.

### Run and configure

Assemble the Spark job first, then run the wrapper from `recsys-pipeline/`:

```bash
(cd services/spark-streaming-job && sbt assembly)
USER_PROFILE_INPUT_PATH=/path/to/profile-events-parquet \
USER_PROFILE_OUTPUT_PATH=/path/to/user-profiles/run-2026-08-06 \
./scripts/run-user-profile-pipeline.sh
```

Core environment variables:

| Env var | Default | Purpose |
|---|---|---|
| `USER_PROFILE_INPUT_PATH` | required | Input Parquet directory |
| `USER_PROFILE_OUTPUT_PATH` | `sampledata/user_profiles` | New immutable output directory |
| `USER_PROFILE_REFERENCE_EPOCH_SECONDS` | job start | Decay/source-window reference time |
| `USER_PROFILE_SOURCE_LOOKBACK_SECONDS` | `2592000` | Accepted history (30 days) |
| `USER_PROFILE_HALF_LIFE_SECONDS` | `604800` | Evidence decay half-life (7 days) |
| `USER_PROFILE_IMPRESSION_WEIGHT` / `CLICK_WEIGHT` / `ORDER_WEIGHT` | `-0.1` / `1.0` / `3.0` | Non-rating evidence weights |
| `USER_PROFILE_RATING_MIN` / `MIDPOINT` / `MAX` | `1.0` / `3.0` / `5.0` | Rating validation and normalization |
| `USER_PROFILE_SHRINKAGE` | `5.0` | Preference shrinkage toward zero |
| `USER_PROFILE_MINIMUM_EVIDENCE` | `5` | Low-evidence/persona boundary |
| `USER_PROFILE_MAX_GENRES` / `MAX_TAGS` | `10` / `20` | Preference list limits |
| `USER_PROFILE_RECENT_RELEASE_AGE_SECONDS` | `31536000` | Published-at freshness window |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Publication target |
| `USER_PROFILE_REDIS_KEY_PREFIX` | `user-profile:v1` | Versioned key namespace; serving must use the same prefix |
| `USER_PROFILE_REDIS_TTL_SECONDS` | `86400` | Run-scoped profile TTL |

Persona thresholds can be overridden with `USER_PROFILE_GENRE_ENTHUSIAST_THRESHOLD`,
`USER_PROFILE_GENRE_EXPLORER_DIVERSITY_THRESHOLD`,
`USER_PROFILE_GENRE_EXPLORER_MAX_CONCENTRATION`,
`USER_PROFILE_FOCUSED_VIEWER_CONCENTRATION_THRESHOLD`,
`USER_PROFILE_RECENT_RELEASE_AFFINITY_THRESHOLD`,
`USER_PROFILE_HIGH_INTENT_ENGAGEMENT_THRESHOLD`,
`USER_PROFILE_HIGH_INTENT_CONVERSION_THRESHOLD`,
`USER_PROFILE_CASUAL_BROWSER_ENGAGEMENT_THRESHOLD`, and
`USER_PROFILE_CASUAL_BROWSER_CONVERSION_THRESHOLD`. The wrapper also accepts `SPARK_HOME`,
`SPARK_MASTER`, `SPARK_DRIVER_MEMORY`, `SPARK_EXECUTOR_MEMORY`, and
`SPARK_SQL_SHUFFLE_PARTITIONS`.

## Spark Job Package Structure

| Package | Responsibility | Examples |
|---|---|---|
| `com.demo.process` | Transform, join, and label stream/batch data into training samples; derive recall/ranking/relevance datasets | `OnlineJoinerStreamingJob`, `ExperienceCollectorStreamingJob`, `RecommendationResponseStatsJob`, `MovieLensContextCollectorStreamingJob`, `RecallSampleStreamingJob`, `RankingSampleStreamingJob`, `RelevanceSampleStreamingJob`, `ItemSequencePreprocessingJob` |
| `com.demo.task` | Runnable entry points for streaming ingestion and offline embedding and CTR/ranking model training | `UserEventStreamingJob`, `Item2VecTrainingJob`, `UserEmbeddingTrainingJob`, `AlsEmbeddingTrainingJob`, `CtrRankingModelTrainingJob` |
| `com.demo.recommend` | Offline candidate pre-computation from trained embeddings | `EmbeddingCandidateGenerationJob` |
| `com.demo.sequence` | Columnar per-user rating/click sequence store: schema, encoder, Redis/Parquet sinks, one-shot backfill | `SequenceSchema`, `SequenceEncoder`, `SequenceRedisSink`, `SequenceParquetSink`, `SequenceBackfillJob` |
| `com.demo.sink` | External write helpers | `RedisWriter` |
| `com.demo.util` | Shared Spark session and environment utilities | `Env`, `SparkSessions` |

## Real-Time Path

The canonical [finite local workflow](../../../README.md#canonical-finite-local-workflow)
owns the full setup sequence. Run the bootstrap block below from the repository root; after
`cd recsys-pipeline`, the remaining commands execute in that subdirectory. Start the local
dependencies, check their readiness, install the Python producer requirements once, and assemble
the Spark job before starting a producer or Spark process:

```bash
cd recsys-pipeline
docker compose up -d zookeeper kafka redis
docker compose ps
python -m pip install -r services/python-modeling/requirements.txt
(cd services/spark-streaming-job && sbt assembly)
```

Do not continue until both Kafka and Redis report `healthy` in `docker compose ps`. A service still
showing `starting`, `unhealthy`, or absent is an infrastructure-readiness failure; producer and
Spark connection errors at that point do not indicate an application failure.

Producer and streaming-job commands are long-running unless a producer is bounded with
`MAX_EVENTS` or a job is externally stopped. Kafka topic names must match across producers,
consumers, and their environment variables — for example, `KAFKA_TOPIC` and
`ONLINE_JOINER_INPUT_TOPIC` must name the same topic when those processes form one flow. The local
`./run-streaming-job.sh` launcher bootstraps the default `UserEventStreamingJob` input topic
(`KAFKA_TOPIC`, default `recsys_events`) when it can reach the local Kafka tooling or Docker stack.

### Common environment variables

Every Spark streaming job below reads these. Per-job tables list only the variables that differ or
that the job alone uses.

| Env var | Default | Purpose |
|---|---|---|
| `SPARK_APP_NAME` | the job's class name | Spark application name |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `SPARK_SQL_SHUFFLE_PARTITIONS` | `8` (`4` for `UserEventStreamingJob` and `MovieLensContextCollectorStreamingJob`) | Shuffle parallelism |
| `SPARK_SQL_SESSION_TIMEZONE` | `UTC` | Session time zone. Date partitions do not depend on it — they are built from the epoch — but timestamp formatting does |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address |
| `KAFKA_STARTING_OFFSETS` | `earliest` (`latest` for `MovieLensContextCollectorStreamingJob`) | Initial offset policy |
| `MAX_OFFSETS_PER_TRIGGER` | `5000` | Per-micro-batch input cap |
| `TRIGGER_INTERVAL` | `10 seconds` (`5 seconds` for `UserEventStreamingJob`) | Processing-time trigger |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/<job>` | Per-job checkpoint directory; each job's default is listed in its own table |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis target, for the jobs that write it |
| `REDIS_PIPELINE_SIZE` / `REDIS_POOL_MAX_TOTAL` | `500` / `8` | Redis pipelining and per-executor pool size |

### `services/python-modeling/producer.py`

Publishes synthetic events to Kafka. In `clickstream` mode it writes simple click events keyed by
`user_id`. In `behavior` mode it writes impression/click/order slates keyed by `request_id`, which
co-partitions every event in a slate for the `OnlineJoinerStreamingJob` join.

Feedback in `behavior` mode is not sent as one instant alongside the impressions. Impressions go
out immediately; each click or order is deferred by `feedback_schedule.py` and released at the
offset its own payload already encodes — clicks 1–20s and orders 21–120s after the impression.

Messages use lz4 compression, and the event loop accounts for send latency so the configured rate
holds accurately at high throughput.

Environment variables:

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `recsys_events` | Kafka topic to publish to |
| `PRODUCER_MODE` | `clickstream` | `clickstream` emits single click events keyed by `user_id`; `behavior` emits full impression/click/order slates keyed by `request_id` |
| `EVENTS_PER_SECOND` | `1` | Target publish rate; the event loop corrects for send latency |
| `NUM_USERS` | `5` | Synthetic user pool size |
| `NUM_ITEMS` | `10` | Synthetic item pool size |
| `SLATE_SIZE` | `5` | Items per slate in `behavior` mode |
| `LOG_EVERY` | `100` | Log a summary line every N events |
| `MAX_EVENTS` | `0` | Stop accepting new events once N have been sent; `0` runs indefinitely. On reaching the cap, the producer still drains every feedback event already scheduled (deliberately not re-checking the cap while draining), so the actual total sent can exceed N |
| `FEEDBACK_DELAY_SCALE` | `1.0` | Multiplies the click/order delay each slate already encodes, compressing (or stretching) how long deferred feedback takes to be released |

`clickstream` mode event schema:

```json
{"user_id":"user_1","item_id":"item_3","event_type":"click","timestamp":1713600001}
```

`behavior` mode event schema:

```json
{
  "request_id": "req_abc123",
  "user_id": "user_1",
  "item_id": "item_3",
  "event_type": "impression",
  "timestamp": 1713600001,
  "position": 0,
  "user_features": {"tier": "vip"},
  "item_features": {"bucket": "b1"},
  "context_features": {"device": "ios", "country": "US"}
}
```

Check whether the default topic has received records:

```bash
docker compose exec -T kafka kafka-get-offsets \
  --bootstrap-server localhost:9092 --topic recsys_events
```

Zero Kafka offsets mean no messages were produced to that topic; they do not indicate a consumer
crash. If a producer or consumer uses a non-default topic, substitute that exact topic in the
diagnostic.

### `UserEventStreamingJob`

Consumes click events from Kafka and writes global item popularity to Redis. Connection pooling uses a per-executor `JedisPool` (one pool per JVM, reused across micro-batches) rather than a new TCP connection per partition.

For each micro-batch, it:

1. Filters to click events, then aggregates per-item click counts in a single pass.
2. Atomically pairs one `ZINCRBY` per unique item with a stable query/sink/batch Lua-ledger entry,
   so a partial micro-batch retry cannot increment an acknowledged item twice.

Redis keys written:

| Key | Type | Contents | TTL |
|---|---|---|---|
| `global:item_popularity` | sorted set | Global click counts | none |
| `seq:{id}:click:{day}` | hash | Per-user click sequence — see [Columnar sequence store](#columnar-sequence-store) | `SEQ_LOOKBACK_DAYS` days |

Job-specific environment variables (plus the [common set](#common-environment-variables)):

| Env var | Default |
|---|---|
| `KAFKA_TOPIC` | `recsys_events` |
| `REDIS_LEDGER_RETENTION_BATCHES` | `2` (minimum `2`; preserves retry state for N and N-1) |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/user-event-streaming-job` |

`ZCARD` reports the number of distinct clicked items, so it confirms that the popularity set is
present but does not show total clicks. Inspect the member scores to observe accumulated click
increments:

```bash
docker compose exec -T redis redis-cli ZCARD global:item_popularity
docker compose exec -T redis redis-cli ZRANGE global:item_popularity 0 -1 WITHSCORES
```

### `OnlineJoinerStreamingJob`

Joins impression events with later feedback from Kafka to produce labeled training samples (features + label).

Start the behavior-mode producer:

```bash
PRODUCER_MODE=behavior KAFKA_TOPIC=behavior_logs python services/python-modeling/producer.py
```

Start the joiner job:

```bash
SPARK_MAIN_CLASS=com.demo.process.OnlineJoinerStreamingJob \
ONLINE_JOINER_INPUT_TOPIC=behavior_logs \
ONLINE_JOINER_OUTPUT_TOPIC=training_samples \
ONLINE_JOINER_HDFS_OUTPUT_PATH=/tmp/spark-recsys/training-samples \
./run-streaming-job.sh
```

For each micro-batch, it:

1. Runs a **single-pass conditional `groupBy`** over `(request_id, user_id, item_id)`: impression/exposure rows contribute position, timestamp, and feature fields; click/order/purchase rows contribute feedback signals. Replaces the previous double-filter + join pattern — one shuffle and one scan instead of two each.
2. Drops groups with no impression in this batch (`impression_ts IS NULL`) — pure late-feedback events with no matching slate exposure.
3. Produces one sample per exposed item with `clicked`, `ordered`, and numeric `label` (`0.0` = not clicked, `1.0` = clicked, `2.0` = ordered).
4. Persists the joined samples (`MEMORY_AND_DISK_SER`) and writes to both sinks inside a `try/finally` that always unpersists.
5. Writes samples to Kafka for online model updates.
6. Writes samples to Parquet **partitioned by date** (`date=YYYY-MM-DD/`) for efficient incremental reads by downstream training jobs.

#### Feedback that arrives in a later micro-batch still joins

`buildTrainingSamples` is batch-local: step 2 drops groups with no impression in the current batch.
`LateFeedbackJoin` wraps it and spans batches. A slate's raw events are held in a durable pending
snapshot under `<archive>/_queries/<namespace>/_pending/<batchId>` until its feedback window closes,
and only then handed to `buildTrainingSamples`. A click or order arriving within
`FEEDBACK_JOIN_WAIT` of its impression therefore lands on the same training sample.

Each sample is published exactly once, when its window closes, so every `sample_id` remains unique
and no consumer of `training_samples` has to dedupe. The cost is latency: a sample reaches the topic
and the Parquet sink one `FEEDBACK_JOIN_WAIT` after its impression rather than in the impression's
own batch. Its `date` partition is still derived from the impression, so it lands in its
impression's date regardless of when it publishes — `TimePartitions.utcDate(impression_ts)`, built
from the epoch rather than a formatted local timestamp, so the partition does not move with the
deploy machine's time zone.

A slate's window closes when either of two arms fires:

- **Event time** — observed event time advances past the slate's deadline, `impression_ts +
  FEEDBACK_JOIN_WAIT`; for a slate that has not seen an impression, the deadline is its earliest
  observed event time plus the same window.
- **Wall clock** — the slate has been held that long in real time.

The event-time arm keeps an archive backfill fast. The wall-clock arm bounds how long a slate waits
while batches are still running, but it cannot drain a stream that has gone completely idle: the
joiner's plan is a stateless `foreachBatch`, so a quiet topic produces no no-data micro-batches and
the wall clock is never re-evaluated. A stopped stream leaves its remaining slates in the pending
snapshot until traffic resumes, at which point they publish on the first batch.

Feedback arriving *more* than `FEEDBACK_JOIN_WAIT` after its impression is still dropped — the
sample it belongs to has already published, and the one-row contract rules out restating it. That
residual is counted rather than left silent — every batch that sees an orphan logs a line:

    [late-feedback] batch=<id> orphan_slates=<n> orphan_events=<n>

`EVENT_WATERMARK_DELAY` remains unrelated to this: it governs deduplication only, not join buffering.

Job-specific environment variables (plus the [common set](#common-environment-variables)):

| Env var | Default |
|---|---|
| `ONLINE_JOINER_INPUT_TOPIC` | `recsys_events` |
| `ONLINE_JOINER_OUTPUT_TOPIC` | `training_samples` |
| `ONLINE_JOINER_HDFS_OUTPUT_PATH` | `/tmp/spark-recsys/training-samples` |
| `ONLINE_JOINER_CATALOG_PATH` | *(empty — enrichment disabled)* |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/online-joiner` |

#### Catalog enrichment and its refresh semantics

Setting `ONLINE_JOINER_CATALOG_PATH` to an object-map JSON file (`{itemId: {genres: [...], tags:
[...]}}`) attaches `genres` and `tags` to each sample by `item_id`. Unmatched items get empty arrays,
so the Parquet schema is identical with or without a catalog.

Enrichment applies to the **Parquet sink only**. The Kafka `training_samples` schema has no
`genres`/`tags` fields, so downstream Kafka consumers never see them. This is deliberate: the tags
are an offline-training signal.

The catalog DataFrame is built once, before the stream starts, and is never cached — so
`foreachBatch` re-scans it every micro-batch. What that observes depends on the shape of the path,
and the two cases behave oppositely:

| Path shape | Change made while the job runs | Observed? |
|---|---|---|
| A single file | Rewritten in place | **Yes**, from the next micro-batch |
| A directory | A file added or removed | **No**, the listing is fixed at startup |

Two consequences for operators:

- **Editing a live catalog file changes enrichment mid-run, and the write is not atomic.** A batch
  can read a partially written file. `from_json` returns null for unparseable input rather than
  failing, so the catalog silently empties and that batch's samples are written with empty
  `genres`/`tags` — indistinguishable in Parquet from legitimately untagged items. To change a
  catalog safely, write a new file and repoint `ONLINE_JOINER_CATALOG_PATH` with a restart, or
  write atomically (write-temp-then-rename on the same filesystem).
- **A directory path is effectively frozen at startup.** Adding a shard has no effect until the job
  restarts.

`CatalogRefreshSemanticsSpec` pins both behaviors so a Spark upgrade cannot change them silently.

Confirm that the Parquet sink created training-sample files:

```bash
find /tmp/spark-recsys/training-samples -name '*.parquet'
```

### Session tracking

Each behavior slate carries a `session_id` (producers group 1..`SESSION_MAX_SLATES` slates per user
into a session). `OnlineJoinerStreamingJob` threads it through to `training_samples` (Kafka value +
Parquet), and `ExperienceCollectorStreamingJob` carries it into `training_experiences`. It is
additive and nullable. `SessionReportJob` (Scala) aggregates session-level
engagement (sessions/user, slates/session, clicks/session, session CTR) from the Parquet.

### Event de-duplication (Phase 2)

`UserEventStreamingJob` and
`OnlineJoinerStreamingJob` drop duplicate `event_id`s within
`EVENT_WATERMARK_DELAY` (default `10 minutes`). Because this makes the queries
stateful, **existing checkpoints are incompatible** — on first deploy of this
change, point `SPARK_CHECKPOINT_LOCATION` at a fresh directory.

### Gate rejection accounting

Every job rejects rows it cannot use — a null `request_id`, an unclassifiable context record, an id
containing a packing separator. Those rejections are counted per reason and logged once per
micro-batch:

```text
[drop-metrics] job=OnlineJoinerStreamingJob batch=42 kept=4931 dropped=62
               null_request_id=3 null_user_id=7 null_item_id=48 null_event_type=0 null_timestamp=4
```

Attribution is **first-match**: a row violating several rules is counted once, under the first, so
the per-reason counts sum exactly to `dropped` and `kept + dropped` equals the input row count.
Every declared reason is printed every batch including zeros, and the line is printed even when
nothing was dropped — a steady `dropped=0` is the evidence that the counter is alive.

The engine emits the same line for decode outcomes, keyed by Avro error code (`invalid_marker`,
`unknown_fingerprint`, `corrupt_payload`, `required_field`), counted from the dead-letter frame
after it has been archived.

Counting is a driver-side aggregation inside `foreachBatch`, never `Dataset.observe`. The Avro
`ExecutionEngine.run` overload applies no stages to the streaming DataFrame, so its plan holds no
`CollectMetrics` node and `StreamingQueryProgress.observedMetrics` is always empty — an
observe-based counter there reports zero forever. Gating a streaming frame still filters, but logs
a warning that counts are unavailable rather than failing the query.

| Site | Reasons |
|---|---|
| `OnlineJoinerStreamingJob` | `null_request_id`, `null_user_id`, `null_item_id`, `null_event_type`, `null_timestamp` |
| `UserEventStreamingJob` | `null_user_id`, `null_item_id` |
| `ExperienceCollectorStreamingJob` | `null_request_id`, `null_user_id`, `null_item_id` |
| `Recall`/`Ranking`/`RelevanceSampleStreamingJob` | `null_user_id`, `null_item_id` |
| `MovieLensContextCollectorStreamingJob` | `unclassifiable_shape` |
| `SequenceEncoder` | `separator_in_identifier` |
| `ExecutionEngine` (decode) | the four Avro error codes |

### `ExperienceCollectorStreamingJob`

Consumes item-level training samples from Kafka and rebuilds each recommendation request as a list-level slate experience.

```bash
SPARK_MAIN_CLASS=com.demo.process.ExperienceCollectorStreamingJob \
EXPERIENCE_COLLECTOR_INPUT_TOPIC=training_samples \
EXPERIENCE_COLLECTOR_OUTPUT_TOPIC=training_experiences \
./run-streaming-job.sh
```

For each micro-batch, it groups samples by `(request_id, user_id)`, sorts items by `position`, and emits a slate JSON containing request context, item features, item labels, slate size, and aggregate slate reward.

Job-specific environment variables (plus the [common set](#common-environment-variables)):

| Env var | Default |
|---|---|
| `EXPERIENCE_COLLECTOR_INPUT_TOPIC` | `training_samples` |
| `EXPERIENCE_COLLECTOR_OUTPUT_TOPIC` | `training_experiences` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/experience-collector` |

### `RecommendationResponseStatsJob`

Consumes request-level slates from `training_experiences` and emits global response metric events to Kafka. Per response, the job produces a total counter, a country-bucketed total counter, selected item/ad counts, and guardrail checks for empty or insufficiently populated responses.

```bash
SPARK_MAIN_CLASS=com.demo.process.RecommendationResponseStatsJob \
RESPONSE_STATS_INPUT_TOPIC=training_experiences \
RESPONSE_STATS_OUTPUT_TOPIC=recommendation_metrics \
./run-streaming-job.sh
```

Each metric payload contains:

| Field | Value |
|---|---|
| `metric_name` | `RecommendationFeed.response` |
| `tags` | `type`, `subscription`, optional `country`, optional `blender`, optional `stage` |
| `value` | Count for that response/stat |

Tag sources:

| Tag | Source field(s) | Notes |
|---|---|---|
| `type` | `item_features.type` or `item_features.product_type` | `ad`, `ads`, `sponsored` → ad; all others → item |
| `subscription` | `user_features.subscription_level` or `user_features.subscription` | |
| `country` | Context/user country fields | Bucketed |
| `blender` | `context_features.AdsBlenderType` or `context_features.ads_blender_type` | Optional |

Job-specific environment variables (plus the [common set](#common-environment-variables)):

| Env var | Default |
|---|---|
| `RESPONSE_STATS_INPUT_TOPIC` | `training_experiences` |
| `RESPONSE_STATS_OUTPUT_TOPIC` | `recommendation_metrics` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/response-stats` |

### `MovieLensContextCollectorStreamingJob`

Consumes MovieLens user, movie, and rating context updates from Kafka and writes two Redis hash
families with different consumers. The retrieval service's query hydrators read compact
`user:{id}:features` state for demographics, rating history, and sequence views. The
`movie:{id}:features` hashes supply title/genre/year enrichment to downstream sample generation,
training, and reports; the retrieval service does not read those movie hashes at serve time.

```bash
SPARK_MAIN_CLASS=com.demo.process.MovieLensContextCollectorStreamingJob \
MOVIELENS_CONTEXT_INPUT_TOPIC=movielens_context \
./run-streaming-job.sh
```

For each micro-batch, it:

1. Classifies mixed JSON records with three **independent** flags — `is_rating`, `is_user_update`,
   `is_movie_update` — so a record carrying both a rating and demographics feeds both aggregates.
   A record matching none of them is dropped and counted as `unclassifiable_shape`.
2. Merges user demographic fields and rating aggregates (`avgRating`, `ratingCount`, `recentlyRatedMovieIds`, `actionSequenceMovieIds`) into `user:{id}:features`.
3. Stores movie title, genres, and release year under `movie:{id}:features`.

Redis keys written:

| Key | Type | Contents | TTL |
|---|---|---|---|
| `user:{id}:features` | hash | MovieLens user demographics and rating context consumed by serving query hydration | `MOVIELENS_CONTEXT_TTL_SECONDS` (default 30 days) |
| `movie:{id}:features` | hash | Movie title, genres, and release year for derived samples, training, and reports (not serving reads) | `MOVIELENS_CONTEXT_TTL_SECONDS` (default 30 days) |
| `seq:{id}:rating:{day}` | hash | Per-user rating sequence — see [Columnar sequence store](#columnar-sequence-store) | `SEQ_LOOKBACK_DAYS` days |

Job-specific environment variables (plus the [common set](#common-environment-variables)):

| Env var | Default |
|---|---|
| `MOVIELENS_CONTEXT_INPUT_TOPIC` | `movielens_context` |
| `MOVIELENS_CONTEXT_TTL_SECONDS` | `2592000` (30 days) |
| `MOVIELENS_RECENT_RATINGS_LIMIT` | `50` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/movielens-context-collector` |

Confirm that movie feature hashes exist in Redis:

```bash
docker compose exec -T redis redis-cli --scan --pattern 'movie:*:features'
```

### Columnar sequence store

A per-user, time-partitioned history of rating and click events, shared by the
streaming producers and a one-shot backfill. It is the successor to the legacy
`recentlyRatedMovieIds` CSV blob on `user:{id}:features`; the serving side chooses
between the two at request time (see **Serving** below).

**Partition key:** `seq:{userId}:{kind}:{bucket}`, where `kind ∈ {rating, click}`
and `bucket` is a UTC day stamp `YYYYMMDD`. Each key is one Redis HASH whose fields
are positionally aligned — row *i* is element *i* of every field:

| Field | Encoding | Example |
|---|---|---|
| `item_id` | `,`-joined | `31,1029,1061` |
| `ts` | `,`-joined epoch millis, ascending | `1690000001000,...` |
| `action` | `,`-joined | `click,rate,click` |
| `rating` | `,`-joined, empty element = null | `,4.0,` |
| `genres` | `,`-joined rows, `\|` within a row | `Drama\|Comedy,Action,` |
| `release_year` | `,`-joined, empty element = null | `1995,,1999` |
| `n` | row count (consistency guard) | `3` |

`genres` uses `|` within a row because genre strings already contain commas; `n` is
the guard a reader uses to detect and truncate a torn write.

Descriptive values have those two separators stripped before packing, which is cosmetic. Identity
values do not: `user_id` is part of the Redis key and `item_id` is what a recommendation resolves
to, so stripping would silently merge `a,b` into the unrelated id `ab`. Events whose `user_id` or
`item_id` contains `,` or `|` are therefore **dropped and counted** as `separator_in_identifier`
rather than mutated. The packing format is unchanged; see
[Gate rejection accounting](#gate-rejection-accounting). Column names, `kind`
values, and the bucket function all come from one `SequenceSchema` object, mirrored
by `SequenceSchemaConstants` in the Java retrieval service (a cross-language fixture
test asserts the two agree).

**Writers:**

- **Streaming (append).** `MovieLensContextCollectorStreamingJob` (rating events)
  and `UserEventStreamingJob` (click events) call `SequenceSinks.write` each
  micro-batch. Append mode reads the existing bucket and concatenates, capping each
  bucket at `SEQ_MAX_ROWS_PER_BUCKET`. Redis infrastructure errors fail the batch so
  Spark retries from the checkpoint; per-row data errors are skipped and logged.
- **Backfill (overwrite).** `SequenceBackfillJob` seeds the store from the
  historical ratings CSV. Overwrite mode replaces each bucket outright, so re-runs
  are idempotent and skip the read-merge phase.

```bash
spark-submit \
  --class com.demo.sequence.SequenceBackfillJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  /path/to/ratings.csv
```

Each bucket key gets an `EXPIRE` of `SEQ_LOOKBACK_DAYS` days, so old buckets vanish
without a compaction job. When `SEQ_PARQUET_PATH` is set, the same chunks are also
written to a Parquet mirror — exploded back to one row per event and
`partitionBy("bucket", "kind")` — for offline analysis.

**Producer-side kill switch.** `SEQ_WRITE_ENABLED=false` stops the Redis sequence write in both
streaming producers and in the backfill job, mirroring `recsys.sequence.mode` on the serving side.
Because Redis infrastructure errors fail the batch, a sequence-store Redis problem otherwise stalls
both jobs with no mitigation short of a redeploy; the flag takes the store off their critical path
at restart instead. It gates the Redis write only — the Parquet mirror keeps running, since a Redis
outage is no reason to stop it.

Turning the write back on does **not** backfill the gap: buckets that would have been written while
it was off are simply missing. Re-seed them with `SequenceBackfillJob`, whose overwrite mode makes
re-runs idempotent.

Environment variables (Spark writers):

| Env var | Default |
|---|---|
| `SEQ_LOOKBACK_DAYS` | `90` (also the Redis TTL, in days) |
| `SEQ_MAX_ROWS_PER_BUCKET` | `500` |
| `SEQ_PARQUET_PATH` | unset (Parquet mirror disabled) |
| `SEQ_WRITE_ENABLED` | `true` — set `false` to stop the Redis sequence write |
| `RATINGS_INPUT_PATH` | backfill only; may be passed as the first positional arg |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` |
| `REDIS_PIPELINE_SIZE` / `REDIS_POOL_MAX_TOTAL` | `500` / `8` |

**Serving.** `RatingSequencesQueryHydrator` in the Java retrieval service reads the
store via `RedisSequenceClient`, walking day buckets back over `lookbackDays` in
chunks of `bucketFetchChunk` keys. The source is selected by `recsys.sequence.mode`:
`off` serves the legacy CSV blob only, `shadow` reads both and serves legacy while
logging the diff, `on` serves the sequence store and falls back to legacy only on
error.

| Config property | Default |
|---|---|
| `recsys.sequence.mode` | `off` (`off` \| `shadow` \| `on`) |
| `recsys.sequence.lookback-days` | `90` |
| `recsys.sequence.bucket-fetch-chunk` | `7` |

## Offline Path

### `ItemSequencePreprocessingJob`

Builds time-ordered item sequences from ratings where `rating >= 3.5`.

```bash
spark-submit \
  --class com.demo.process.ItemSequencePreprocessingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  /tmp/spark-recsys/item-sequences
```

Environment variables:

| Env var | Description |
|---|---|
| `RATINGS_INPUT_PATH` | Path to the ratings CSV; overrides the first positional argument |
| `ITEM_SEQUENCES_OUTPUT_PATH` | Output directory for item sequences; overrides the second positional argument |

### `Item2VecTrainingJob`

Trains Spark MLlib `Word2Vec` on item sequences, writes item embeddings to a text file, and optionally publishes them to Redis for the retrieval service.

```bash
spark-submit \
  --class com.demo.task.Item2VecTrainingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/embedding.txt \
  item_1
```

Key environment variables:

| Env var | Default |
|---|---|
| `RATINGS_INPUT_PATH` | *(positional arg)* |
| `ITEM2VEC_EMBEDDING_PATH` | `recsys-pipeline/sampledata/embedding.txt` |
| `ITEM2VEC_QUERY_ITEM` | `592` |
| `ITEM2VEC_REDIS_KEY_PREFIX` | `i2vEmb` |
| `ITEM2VEC_REDIS_TTL_SECONDS` | `86400` (1 day) |
| `ITEM2VEC_MIN_COUNT` | `1` |
| `ITEM2VEC_SAVE_TO_REDIS` | `false` |

Train and publish embeddings to Redis:

```bash
ITEM2VEC_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
REDIS_PORT=6379 \
spark-submit \
  --class com.demo.task.Item2VecTrainingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/embedding.txt \
  item_1
```

Output format:

```text
item_1:0.0123 -0.4567 0.8910 ...
```

### `UserEmbeddingTrainingJob`

Builds a user embedding by averaging item vectors for ratings at or above `USER_EMBEDDING_MIN_RATING` (default `3.5`).

```bash
spark-submit \
  --class com.demo.task.UserEmbeddingTrainingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/item_embedding_sample.txt \
  sampledata/user_embedding.txt
```

Key environment variables:

| Env var | Default |
|---|---|
| `RATINGS_INPUT_PATH` | *(positional arg)* |
| `ITEM2VEC_EMBEDDING_PATH` | *(positional arg)* |
| `USER_EMBEDDING_OUTPUT_PATH` | `recsys-pipeline/sampledata/user_embedding.txt` |
| `USER_EMBEDDING_MIN_RATING` | `3.5` |

Output format:

```text
user_1:0.9 0.1 0.0
```

### `AlsEmbeddingTrainingJob`

Trains Spark ML ALS collaborative filtering on the ratings matrix and writes latent user and item factors.

```bash
spark-submit \
  --class com.demo.task.AlsEmbeddingTrainingJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv \
  sampledata/als
```

Key environment variables:

| Env var | Default |
|---|---|
| `RATINGS_INPUT_PATH` | *(positional arg)* |
| `ALS_EMBEDDING_OUTPUT_PATH` | `recsys-pipeline/sampledata/als` |
| `ALS_RANK` | `16` |
| `ALS_MAX_ITER` | `10` |
| `ALS_REG_PARAM` | `0.1` |

Output paths:

```text
sampledata/als/userFactors/part-...
sampledata/als/itemFactors/part-...
```

### `EmbeddingCandidateGenerationJob`

Pre-computes top-K candidates per user in batch. Loads pre-trained user and item embeddings (output of `UserEmbeddingTrainingJob` or `AlsEmbeddingTrainingJob`), broadcasts the full item catalog to every executor, and computes cosine similarity locally per user partition with no cross-join or shuffle. Writes results to Parquet and optionally to Redis.

```bash
spark-submit \
  --class com.demo.recommend.EmbeddingCandidateGenerationJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/user_embedding.txt \
  sampledata/embedding.txt \
  sampledata/candidates
```

Key environment variables:

| Env var | Default |
|---|---|
| `USER_EMBEDDING_PATH` | *(positional arg)* |
| `ITEM_EMBEDDING_PATH` | *(positional arg)* |
| `CANDIDATE_OUTPUT_PATH` | *(positional arg)* |
| `CANDIDATE_TOP_K` | `100` |
| `CANDIDATE_SAVE_TO_REDIS` | `false` |
| `CANDIDATE_REDIS_KEY_PREFIX` | `user` (writes `user:{id}:candidates`) |
| `CANDIDATE_REDIS_TTL_SECONDS` | `86400` (1 day) |

### `CtrRankingModelTrainingJob`

Offline batch trainer over the Parquet training-samples store (the
`OnlineJoinerStreamingJob` output). Reads the date-partitioned Parquet, assembles
features (hashed user/item/context map fields + `item_id` via `FeatureHasher`,
`genres`/`tags` via `HashingTF`, numeric `position`), does a temporal train/val
split by `date`, trains a click-probability classifier, and writes the Spark ML
model plus a `metrics.json` (AUC-ROC, PR-AUC, logloss). Offline only — no serving,
Redis, or ONNX changes.

```bash
CTR_INPUT_PATH=/tmp/spark-recsys/training-samples ./run-ctr-training.sh
```

Key environment variables:

| Env var | Default |
|---|---|
| `CTR_INPUT_PATH` | `/tmp/spark-recsys/training-samples` |
| `CTR_MODEL_OUTPUT_PATH` | `/tmp/spark-recsys/ctr-model` |
| `CTR_METRICS_OUTPUT_PATH` | `<model>/metrics.json` |
| `CTR_HOLDOUT_DAYS` | `1` |
| `CTR_ALGORITHM` | `logreg` (`logreg` \| `gbt`) |
| `CTR_LABEL_MODE` | `positive` (`positive` \| `click`) |
| `CTR_NUM_FEATURES` | `262144` |
