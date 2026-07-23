# Spec: Close Training-Consolidation Phase 2 & Phase 5 Gaps

> Scoped follow-up to [training-consolidation.md](training-consolidation.md). An
> investigation found Phases 1, 3, 4 already implemented; Phase 2 (unified data
> contract) and Phase 5 (online→offline feedback closure) had only partial
> scaffolding. This spec covers finishing those two phases.

## Objective

Make the offline models train on the **same data the rest of the system uses**
(Phase 2), and let **online reward signals flow back into offline training**
(Phase 5) — without changing any external contract (Kafka topics, output
schemas, Redis keys) and with every new behavior **off by default**.

## Scope

- **In:** `movielens_pipeline.py`, `replay_export.py` (python-modeling); the
  `OnlineJoinerStreamingJob` Parquet path (spark-streaming-job); the catalog
  config of `java-retrieval-service`.
- **Out:** retraining orchestration (Phase 4, already shipped), the two-tower
  serving path (Phase 3, already shipped), unifying item-ID namespaces across
  `ratings.csv` / catalog / producer.

## Starting state (gaps)

| Gap | Before |
|-----|--------|
| **P2.1** | `movielens_pipeline` trained on hardcoded `USER_HISTORY`; `ratings.csv` only via opt-in flag. |
| **P2.2** | Java catalog hardcoded as a 7-item map in `application.yml`; no file loader. |
| **P2.3** | `OnlineJoiner` Parquet output had no genres/tags column. |
| **P5.1** | `replay_export.py` wrote only ratings-CSV; no Parquet. |
| **P5.2** | `--fine-tune-csv` appended data and trained from scratch — no warm-init, reward not used as label. |
| **P5.3** | Nothing read `reward-model:*`; BPR/ranking losses unweighted. |

## Work items & acceptance

**P2.1 — ratings.csv as the default training source.**
- *Accept:* a no-arg `movielens_pipeline` run trains on `sampledata/ratings.csv`;
  missing file falls back to the demo data with a warning; `--no-ratings-csv`
  forces the demo data.

**P2.2 — external catalog file for the Java service.**
- *Accept:* `recsys.catalog-path` (`RECSYS_CATALOG_PATH`) merges a `catalog.json`
  on top of the inline catalog at startup; empty path = inline only (no change);
  external entries override inline ones by id; missing file keeps the inline
  catalog.

**P2.3 — genres/tags column in the OnlineJoiner Parquet output.**
- *Accept:* with `ONLINE_JOINER_CATALOG_PATH` set, Parquet rows carry `genres`
  and `tags` from a broadcast-joined catalog; without it the columns are present
  but empty (stable schema); the Kafka `training_samples` value is unchanged.

**P5.1 — Parquet export of the replay buffer.**
- *Accept:* `replay_export.py --parquet PATH` writes the raw replay experience
  tuples; the ratings-CSV remains the default output; no HDFS dependency.

**P5.2 — warm-init fine-tuning.**
- *Accept:* `.pt` checkpoints are written next to the ONNX exports; `--warm-init`
  resumes training from them at a reduced LR; a grown embedding table loads the
  compatible tensors and trains the new rows fresh (no crash).

**P5.3 — online reward weights → offline loss.**
- *Accept:* `load_reward_weights` reads `reward-model:item:*` / `:genre:*` (hash
  fields `count`, `reward_total`); `--use-reward-weights` upweights high-reward
  items in BPR positive sampling and scales the ranking loss per candidate by
  genre reward; default off reduces to the prior plain-mean loss.

## Boundaries

- **Always:** keep topic/schema/Redis-key contracts stable; gate every new knob
  with a safe default; tests green per component before each commit.
- **Never:** change the Kafka `training_samples` / `training_experiences` value
  schemas; require HDFS; train on a different population than `ratings.csv`
  without an explicit flag.

## Out-of-scope fix shipped alongside

`java-retrieval-service` did not compile on `master` — the `bb7764a` package
reorg left `KafkaEventSerializer.toJsonBytes` package-private while `RecSysEvent`
moved to a sibling package. Made the method `public` (minimal visibility fix) to
restore compilation; required to build/test the Phase 2 Java work.
