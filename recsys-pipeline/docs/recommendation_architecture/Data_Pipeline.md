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

## Avro Kafka ingestion, archive, and replay

`recsys_events` is the live canonical-event topic. Its only replay counterpart is
`recsys_events.backfill`; this is a separate topic, not a mode on the live topic. Kafka
auto-creation is disabled. Provision both catalog entries after Kafka is healthy and before
starting producers or consumers:

```bash
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
checkpoint-hash namespace. A batch is consumable only when its directory has both `_SUCCESS` and
the `_COMMITTED` manifest with the expected query, kind, and batch identity.

Replay is an explicit half-open UTC date range (`start <= date < end`), enforces its row limit
before creating a producer, rate-limits sends, and strips archive/Kafka lineage before encoding the
canonical event again. It can only target `recsys_events.backfill`. The query namespace and durable
operation identity are required rather than inferred:

```bash
REPLAY_ARCHIVE_PATH=/data/recsys-events \
REPLAY_ARCHIVE_QUERY_NAMESPACE=<checkpoint-hash> \
REPLAY_OPERATION_ID=incident-2026-08-01 \
REPLAY_START_DATE=2026-08-01 REPLAY_END_DATE=2026-08-02 \
REPLAY_MAX_ROWS=100000 REPLAY_RECORDS_PER_SECOND=5000 \
  ./scripts/run-archive-replay.sh
```

Only numeric `_queries/<selected-query>/_batches/<batch>` directories with `_SUCCESS` and an exact
version/query/kind/batch `_COMMITTED` manifest are read. Orphan attempts, dedupe snapshots,
incomplete batches, and batches owned by another query are excluded; a missing or ambiguous query
identity is rejected.

Set `REPLAY_MANIFEST_DIR` to choose the operation directory; otherwise the deterministic manifest
is written to `$REPLAY_ARCHIVE_PATH/_replay_manifests/<operation-id>.json`. It records the stable
operation ID, status, immutable selection contract, ordered source signature, acknowledged cursor,
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

The migrated Avro engine also scopes business completion to the stable
`(query identity, sink identity, batchId)` tuple. It rejects an unrecognized or duplicate sink
identity before archival or business effects. A successful sink is not invoked again when a later
sink fails and Spark retries the same batch. Parquet sinks atomically publish deterministic batch
directories. Redis popularity increments and sequence hash updates atomically pair each effect with
a non-expiring batch ledger entry using Lua. Kafka output enables producer idempotence and carries a
stable key plus query/sink/batch headers; because a later producer session may repeat a record after
a partial batch failure, derived-topic consumers still must deduplicate that stable key. These are
retry-safe per-sink contracts, not a cross-system exactly-once transaction.

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
| `RelevanceSampleStreamingJob` | `relevance_samples` | LTR shape: `query` (`user_id:session_id`), `recommended_movie_id`, `title`/`genres`/`release_year` (from `movie:{id}:features`), `score` (label) |

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
consumers, and their environment variables—for example, `KAFKA_TOPIC` and
`ONLINE_JOINER_INPUT_TOPIC` must name the same topic when those processes form one flow. The local
`./run-streaming-job.sh` launcher bootstraps the default `UserEventStreamingJob` input topic
(`KAFKA_TOPIC`, default `recsys_events`) when it can reach the local Kafka tooling or Docker stack.

### `services/python-modeling/producer.py`

Publishes synthetic events to Kafka. In `clickstream` mode it writes simple click events keyed by `user_id`. In `behavior` mode it writes full impression/click/order slates keyed by `request_id`, which co-partitions all events in the same slate for the `OnlineJoinerStreamingJob` join. It uses lz4 compression; the event loop accounts for send latency so the configured rate is maintained accurately at high throughput.

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
| `MAX_EVENTS` | `0` | Stop after N events; `0` runs indefinitely |

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

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `UserEventStreamingJob` |
| `SPARK_MASTER` | `local[*]` |
| `SPARK_SQL_SHUFFLE_PARTITIONS` | `4` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `recsys_events` |
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `REDIS_PIPELINE_SIZE` | `500` |
| `REDIS_POOL_MAX_TOTAL` | `8` |
| `MAX_OFFSETS_PER_TRIGGER` | `5000` |
| `TRIGGER_INTERVAL` | `5 seconds` |
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

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `OnlineJoinerStreamingJob` |
| `ONLINE_JOINER_INPUT_TOPIC` | `recsys_events` |
| `ONLINE_JOINER_OUTPUT_TOPIC` | `training_samples` |
| `ONLINE_JOINER_HDFS_OUTPUT_PATH` | `/tmp/spark-recsys/training-samples` |
| `SPARK_CHECKPOINT_LOCATION` | `/tmp/spark-recsys/online-joiner` |

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
change, point `SPARK_CHECKPOINT_LOCATION` at a fresh directory. Per-batch
`corrupt=<n>` counts are logged by the metrics listener.

### `ExperienceCollectorStreamingJob`

Consumes item-level training samples from Kafka and rebuilds each recommendation request as a list-level slate experience.

```bash
SPARK_MAIN_CLASS=com.demo.process.ExperienceCollectorStreamingJob \
EXPERIENCE_COLLECTOR_INPUT_TOPIC=training_samples \
EXPERIENCE_COLLECTOR_OUTPUT_TOPIC=training_experiences \
./run-streaming-job.sh
```

For each micro-batch, it groups samples by `(request_id, user_id)`, sorts items by `position`, and emits a slate JSON containing request context, item features, item labels, slate size, and aggregate slate reward.

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `ExperienceCollectorStreamingJob` |
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

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `RecommendationResponseStatsJob` |
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

1. Classifies mixed JSON records as `user_update`, `movie_update`, or `rating`.
2. Merges user demographic fields and rating aggregates (`avgRating`, `ratingCount`, `recentlyRatedMovieIds`, `actionSequenceMovieIds`) into `user:{id}:features`.
3. Stores movie title, genres, and release year under `movie:{id}:features`.

Redis keys written:

| Key | Type | Contents | TTL |
|---|---|---|---|
| `user:{id}:features` | hash | MovieLens user demographics and rating context consumed by serving query hydration | `MOVIELENS_CONTEXT_TTL_SECONDS` (default 30 days) |
| `movie:{id}:features` | hash | Movie title, genres, and release year for derived samples, training, and reports (not serving reads) | `MOVIELENS_CONTEXT_TTL_SECONDS` (default 30 days) |
| `seq:{id}:rating:{day}` | hash | Per-user rating sequence — see [Columnar sequence store](#columnar-sequence-store) | `SEQ_LOOKBACK_DAYS` days |

Environment variables:

| Env var | Default |
|---|---|
| `SPARK_APP_NAME` | `MovieLensContextCollectorStreamingJob` |
| `MOVIELENS_CONTEXT_INPUT_TOPIC` | `movielens_context` |
| `REDIS_HOST` | `localhost` |
| `REDIS_PORT` | `6379` |
| `MOVIELENS_CONTEXT_TTL_SECONDS` | `2592000` (30 days) |
| `MOVIELENS_RECENT_RATINGS_LIMIT` | `50` |
| `REDIS_PIPELINE_SIZE` | `500` |
| `REDIS_POOL_MAX_TOTAL` | `8` |
| `MAX_OFFSETS_PER_TRIGGER` | `5000` |
| `TRIGGER_INTERVAL` | `10 seconds` |
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
the guard a reader uses to detect and truncate a torn write. Column names, `kind`
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

Environment variables (Spark writers):

| Env var | Default |
|---|---|
| `SEQ_LOOKBACK_DAYS` | `90` (also the Redis TTL, in days) |
| `SEQ_MAX_ROWS_PER_BUCKET` | `500` |
| `SEQ_PARQUET_PATH` | unset (Parquet mirror disabled) |
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
