# Plan: Close Consolidation Phase 2 & Phase 5 Gaps

Implementation plan for [2026-06-27-consolidation-phase2-phase5.md](../specs/2026-06-27-consolidation-phase2-phase5.md).
Shipped in PR #91 (branch `feat/consolidation-phase2-phase5`). One work item per
commit; tests green per component before each commit.

**Status:** ✅ all items shipped. Python 52 passed / 1 skipped · Java 38 passed ·
Scala 38 passed.

## Decisions taken

- **P5.1 output format:** CSV stays canonical; Parquet is an optional `--parquet`
  add-on. No HDFS (none exists in the local setup).
- **P2.3 catalog source:** broadcast-join the shared `catalog.json` (not relying
  on producer-emitted `item_features`). Open follow-up: confirm whether
  `producer.py` already emits genres/tags, which could make the join redundant.

## Tasks

### P2.1 — ratings.csv default → verify: `test_ratings_csv_defaults_to_shared_sampledata`, `test_no_ratings_csv_forces_builtin_data`
- `DEFAULT_RATINGS_CSV` constant; `--ratings-csv` defaults to it; `--no-ratings-csv`
  const-None override; `main()` warns + falls back when the file is absent.
- Files: `movielens_pipeline.py`, `test_movielens_pipeline.py`. Commit `0c31b99`.

### P2.2 — external catalog loader → verify: `CatalogLoaderTest` (4 cases)
- `catalogPath` property; `CatalogLoader` `@PostConstruct` merges `catalog.json`
  over the inline catalog; `catalog-path: ${RECSYS_CATALOG_PATH:}`; seed
  `sampledata/catalog.json`.
- Files: `RecommendationProperties.java`, `CatalogLoader.java`, `application.yml`,
  `sampledata/catalog.json`, `CatalogLoaderTest.java`. Commit `e27d0a8`.

### P2.3 — genres/tags in Parquet → verify: `enrichWithCatalog`/`withCatalog`/`loadCatalog` specs
- `loadCatalog` reads the object-map `catalog.json` via `from_json`+`explode`;
  `enrichWithCatalog` broadcast left-joins by `item_id`; applied to the Parquet
  branch only (Kafka value untouched); `ONLINE_JOINER_CATALOG_PATH` gate; empty
  arrays when unset.
- Files: `OnlineJoinerStreamingJob.scala`, `OnlineJoinerStreamingJobSpec.scala`.
  Commit `66c3eef`.

### P5.1 — replay Parquet → verify: `test_write_parquet_roundtrips_raw_tuples`, `test_main_writes_parquet_when_requested`
- `write_parquet` (lazy pandas import, clear error if missing); `--parquet` flag;
  CSV still written.
- Files: `replay_export.py`, `test_replay_export.py`. Commit `f6dae95`.

### P5.2 — warm-init fine-tuning → verify: `test_export_onnx_persists_pt_checkpoints`, `test_warm_init_*`
- `ArtifactPaths.checkpoint(...)`; save `.pt` state_dicts in `export_onnx`;
  `_maybe_warm_init` (shape-tolerant); `--warm-init` continues training at 0.25×
  LR and forces training when artifacts exist.
- Files: `movielens_pipeline.py`, `test_movielens_pipeline.py`. Commit `ae6f42b`.

### P5.3 — reward-weighted loss → verify: `test_load_reward_weights_parses_item_and_genre`, `test_bpr_triples_upweights_high_reward_items`
- `load_reward_weights` (mean reward → 1+mean); `_bpr_triples(item_weights)`
  oversamples high-reward positives; `train_ranking(genre_weights)` weights the
  per-candidate multi-task loss; `--use-reward-weights` wires Redis.
- Files: `movielens_pipeline.py`, `test_movielens_pipeline.py`. Commit `4c56700`.

### Build unblock (out of scope) → verify: full `mvn test` compiles + 38 green
- `KafkaEventSerializer.toJsonBytes` → `public`. Commit `b207e16`.
