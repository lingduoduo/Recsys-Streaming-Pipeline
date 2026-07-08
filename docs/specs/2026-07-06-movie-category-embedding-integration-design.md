# Movie-Category Embedding Integration Design

## Goal

Make the movie-category simulation produce Item2Vec and user embeddings whose identifiers match its `movie_*` and `user_*` data, while making the analysis dashboard clearly distinguish unavailable embedding metrics from real zero performance.

## Current problem

The simulation emits `movie_*` items and `user_*` users, but the bundled ratings use `item_*` identifiers. Running the existing offline pipeline against bundled ratings therefore cannot supply vectors for the simulation corpus. When no vectors exist, Recall currently renders embedding metrics as numeric zero and Hybrid silently collapses to BM25.

## Data flow

1. `movie_segment_producer.py` continues producing movie metadata and behavior events.
2. The producer additionally writes a ratings CSV when `RATINGS_OUTPUT_PATH` is set. Rows use the same simulated `user_*` and `movie_*` identifiers. Clicks map to rating `4.0`; orders map to `5.0`; timestamps come from the corresponding feedback event. Within one slate, an order supersedes its click so a user/item interaction is written once.
3. `run-movie-category-sim.sh` sets `RATINGS_OUTPUT_PATH=$SIM_ROOT/ratings.csv`.
4. Between the collector and joiner drains, the script also drains the existing `UserEventStreamingJob` over the same behavior topic, aggregating clicks into the Redis ZSET `global:item_popularity`. This makes the dashboard's ranking `popularity` signal evaluable with `movie_*` identifiers (previously empty). Beyond the original embedding scope, but required for a fully-populated ranking section.
5. After producing Parquet and movie metadata, the script invokes the existing `Item2VecTrainingJob` through `run-offline-pipeline.sh`, with Redis publishing enabled under `i2vEmb`.
6. The script invokes the existing `UserEmbeddingTrainingJob` through `run-user-embedding-pipeline.sh`, with Redis publishing enabled under `uEmb`.
7. The consolidated dashboard runs after embeddings are published and writes `$SIM_ROOT/report-dashboard/index.html`.

Infra note: the script runs `docker compose down -v` before `up -d` so a stale ZooKeeper broker registration cannot fail Kafka startup with `NodeExistsException`.

## Simulation controls

Embedding generation is enabled by default for the movie-category simulation and can be disabled with `GENERATE_EMBEDDINGS=false` for a faster category-only run. Existing Item2Vec and user-embedding environment variables remain overridable. Simulation-local defaults are:

- ratings: `$SIM_ROOT/ratings.csv`
- item embedding file: `$SIM_ROOT/item-embedding.txt`
- user embedding output: `$SIM_ROOT/user-embedding`
- Redis prefixes: `i2vEmb` and `uEmb`
- query item: `movie_1`

The simulation fails with a clear message if embedding generation is enabled but the ratings CSV has no usable positive interactions or either training job fails.

## Dashboard fallback semantics

`compute_recall` will return explicit embedding availability and coverage alongside metric rows.

When no matching item vectors exist:

- BM25 metrics remain numeric.
- Embedding metrics render as `N/A`, not `0.0`.
- Hybrid is labeled `Hybrid (BM25 only)`.
- The headline states that embeddings are unavailable.
- The chart omits the unavailable embedding series.

When matching vectors exist, all three methods render normally. A genuine zero remains numeric zero only when the embedding method had nonzero evaluable coverage.

## Boundaries

- Reuse the existing Spark Item2Vec and user-embedding jobs; do not create a second vector implementation.
- Do not remap `item_*` vectors to `movie_*`.
- Do not change the simulation's category effects, event schemas, report metrics, or Redis prefix defaults.
- Generated ratings and embedding artifacts stay under `SIM_ROOT` and are not committed.

## Tests

- Producer unit tests verify ratings IDs, click/order rating mapping, order precedence, timestamps, and optional output behavior.
- Script tests verify the simulation passes matching paths and Redis flags to both existing embedding runners.
- Dashboard tests verify unavailable embeddings display N/A/BM25-only and available embeddings preserve numeric metrics.
- The focused Python suite, relevant shell integration tests, and Spark embedding tests run before publication.
- End-to-end verification checks nonzero `i2vEmb:movie_*` and `uEmb:user_*` counts, then regenerates and inspects the dashboard.

