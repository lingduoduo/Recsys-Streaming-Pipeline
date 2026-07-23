# CTR/Ranking Batch Trainer — Design

**Date:** 2026-07-23
**Status:** Approved (design), pending implementation plan

## Problem

The offline half of the feature-store architecture (daily dump → Spark batch
job → offline store → models) is only half-closed in this repo. The streaming
`OnlineJoinerStreamingJob` writes labeled feature rows to a Parquet
training-samples store (`training-samples/date=YYYY-MM-DD/`), but **no batch job
trains a model from that store**. The Parquet store is read only by the
`com.demo.report.*` analytics jobs and used as a retrain *trigger gate*
(`run-retrain.sh` counts files). The actual model trainers (Item2Vec, ALS,
UserEmbedding, the Python two-tower) all read raw `sampledata/ratings.csv`
instead.

This design adds the missing batch trainer: a Spark ML job that consumes the
Parquet training-samples offline store and trains a CTR/ranking model.

## Scope

**In scope (offline-only):**
- A new Scala Spark batch job that reads the Parquet training-samples store,
  assembles features, trains a click-probability classifier, evaluates it, and
  writes the model artifact + metrics to disk.

**Out of scope:**
- No serving / Java-API changes. No ONNX export. No new Redis keys.
- No changes to the streaming jobs or the existing offline embedding trainers.
- No doc-drift fixes (tracked separately).

## Component: `com.demo.task.CtrRankingModelTrainingJob`

New object under `services/spark-streaming-job/src/main/scala/com/demo/task/`,
alongside `Item2VecTrainingJob` and `AlsEmbeddingTrainingJob`. Batch
`spark-submit` entry point, not a streaming query.

### 1. Input & filter
- `spark.read.parquet(CTR_INPUT_PATH)` — default `/tmp/spark-recsys/training-samples`
  (the `ONLINE_JOINER_HDFS_OUTPUT_PATH` default).
- Drop rows with null `user_id`, `item_id`, or `impression_time`.
- Each row is already one exposed impression carrying its label — no join or
  groupBy is needed to reconstruct positives.

### 2. Label
- Binary CTR target: `is_click = (label > 0.0)` — folds `clicked` (1.0) and
  `ordered` (2.0) into the positive class.
- `CTR_LABEL_MODE` env allows switching: `positive` (default, `label > 0`) or
  `click` (`clicked == 1` only).

### 3. Feature assembly (pure function `assembleFeatures`)
- **FeatureHasher** over a curated string-feature set:
  - selected keys from `user_features`, `item_features`, `context_features`
    maps (`tier`, `bucket`, `device`, `country`) pulled via `element_at`;
  - `genres` and `tags` array tokens;
  - `item_id`.
- Plus numeric `position`.
- One Spark ML `Pipeline`. FeatureHasher chosen over per-key
  StringIndexer+OneHotEncoder because the feature maps are schemaless (arbitrary
  keys) and `item_id` is high-cardinality; hashing avoids per-key brittleness
  and unseen-category failures. Fixed hash space via `CTR_NUM_FEATURES`
  (default 262144).

### 4. Split (pure function `splitByDate`)
- Temporal split on the `date` partition column: train on all dates except the
  latest `CTR_HOLDOUT_DAYS` (default 1); validate on the held-out tail.
- Matches production (train past → predict future); avoids leakage that a random
  split would introduce across a user's impressions in one slate.

### 5. Model & metrics
- Baseline: Spark ML `LogisticRegression` (fast, interpretable, standard CTR
  baseline).
- `CTR_ALGORITHM=gbt` toggles `GBTClassifier` (cheap addition; same assembled
  feature vector).
- Evaluate on the validation split with `BinaryClassificationEvaluator`:
  **AUC-ROC**, **PR-AUC** (areaUnderPR), and **logloss** (computed from
  probability column). Also report positive rate and train/val row counts.

### 6. Outputs
- Spark ML model saved to `CTR_MODEL_OUTPUT_PATH` via `model.write.overwrite().save(...)`.
- `metrics.json` written to `CTR_METRICS_OUTPUT_PATH` (single JSON object:
  algorithm, label_mode, holdout_days, train_rows, val_rows, positive_rate,
  auc_roc, pr_auc, logloss).
- One console summary line.

### 7. Configuration (via `com.demo.util.Env`)

| Env var | Default |
|---|---|
| `CTR_INPUT_PATH` | `/tmp/spark-recsys/training-samples` |
| `CTR_MODEL_OUTPUT_PATH` | `/tmp/spark-recsys/ctr-model` |
| `CTR_METRICS_OUTPUT_PATH` | `/tmp/spark-recsys/ctr-model/metrics.json` |
| `CTR_HOLDOUT_DAYS` | `1` |
| `CTR_ALGORITHM` | `logreg` (`logreg` \| `gbt`) |
| `CTR_LABEL_MODE` | `positive` (`positive` \| `click`) |
| `CTR_NUM_FEATURES` | `262144` |

Spark session via the shared `SparkSessions.create(...)` helper.

## Testing

`CtrRankingModelTrainingJobSpec` (ScalaTest, no Kafka/Redis), matching the
existing `build*Samples` unit-test pattern:
- `labelColumn` — asserts `is_click` mapping for `positive` and `click` modes.
- `assembleFeatures` — on a tiny in-memory DataFrame mirroring the Parquet
  schema (IDs, feature maps, genres/tags, position), asserts a non-null
  `features` vector column of the configured size is produced.
- `splitByDate` — asserts the temporal holdout keeps the latest N dates in the
  validation set and the rest in train.
- One tiny end-to-end `main`-path train (LogisticRegression) over a few rows
  asserting `metrics.json` is written and `auc_roc ∈ [0, 1]`.

## Run

- Thin `run-ctr-training.sh` mirroring `run-als-pipeline.sh` (locates
  `spark-submit`, sets `JAVA_HOME`, invokes the class with env passthrough).
- Documented `spark-submit --class com.demo.task.CtrRankingModelTrainingJob`
  example in the script header.

## Success criteria

1. `sbt test` passes including the new spec.
2. Running the job over a populated `training-samples/` directory produces a
   saved Spark ML model and a `metrics.json` with finite AUC/PR-AUC/logloss.
3. No existing job, test, or serving behavior changes.
