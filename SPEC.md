# Spec: Consolidating Offline and Online Training in Recsys-Streaming-Pipeline

_Date: 2026-06-14_
_Branch: fix/remove-stale-posts-comments_

---

## 1. Purpose

This spec describes the current state of training in the recommendation pipeline, identifies the structural gaps that prevent offline and online training from sharing data and models, and proposes a phased plan to consolidate them into a single coherent loop.

---

## 2. Current Architecture

### 2.1 Data Flow (as implemented)

```
Producer (Python)
    │  synthetic user events
    ▼
Kafka: user_events
    │
    ▼
UserEventStreamingJob (Spark)
    │  click events → Redis
    │  user:{id}:recent  (LPUSH+LTRIM)
    │  global:item_popularity  (ZINCRBY)
    │
    ▼
Java Retrieval Service (Spring Boot)
    │  reads Redis for user history and popularity
    │  GET /recommend/{user}
    │
    ├── DeepLearningPredictionService
    │       loads mlp_embedding_model.onnx (classpath)
    │       weight = 0.0 by default → score unused
    │
    ├── OnlineLearningService
    │       Redis reward stats (item/genre/tag/global)
    │       UCB / Thompson / Q-learning / SARSA
    │
    └── HybridRecommendationService
            embedding cosine similarity (from Redis)
            content score (genre/tag overlap)
            popularity score
            bandit policy
            POST /feedback → reward stat update

Offline embedding jobs (Spark — manually triggered):
    ratings.csv
    └── Item2VecTrainingJob
            → Redis i2vEmb:{movieId}  (optional)
            → sampledata/item_embedding.txt

    ratings.csv
    └── AlsEmbeddingTrainingJob         ← no runner script
            → Redis alsItemEmb:{movieId}, alsUserEmb:{userId}

    ratings.csv + item_embedding.txt
    └── UserEmbeddingTrainingJob         ← no runner script
            → Redis uEmb:{userId}

Python DL pipeline (standalone, manually triggered):
    movielens_pipeline.py
    └── hardcoded USER_HISTORY (3 users, not ratings.csv)
        └── train_two_tower() + train_ranking()
            → sampledata/movielens_user_tower.onnx   ← never loaded
            → sampledata/movielens_item_tower.onnx   ← never loaded
            → sampledata/movielens_ranking.onnx      ← never loaded

Training data pipeline (Spark streaming — orphaned output):
    Kafka: behavior_logs
    └── OnlineJoinerStreamingJob
            → Kafka: training_samples
            → HDFS: /tmp/spark-recsys/training-samples/  ← nothing reads this back
    └── ExperienceCollectorStreamingJob
            → Kafka: training_experiences (slates)  ← nothing reads this back
```

### 2.2 Component Inventory

| Component | Language | Role | Status |
|-----------|----------|------|--------|
| `Item2VecTrainingJob` | Scala | Offline item embeddings (Word2Vec) | Working; has runner |
| `AlsEmbeddingTrainingJob` | Scala | Offline user+item embeddings (ALS) | Working; no runner |
| `UserEmbeddingTrainingJob` | Scala | Offline user embeddings (avg item vecs) | Working; no runner |
| `UserEventStreamingJob` | Scala | Streams events → Redis history+popularity | Working |
| `OnlineJoinerStreamingJob` | Scala | Joins impressions+clicks → training samples | Working; output orphaned |
| `ExperienceCollectorStreamingJob` | Scala | Assembles slates from samples | Working; output orphaned |
| `movielens_pipeline.py` | Python | Two-tower retrieval + transformer ranking | Working standalone; not integrated |
| `DeepLearningPredictionService` | Java | MLP ONNX model scorer | Loaded; weight=0.0 by default |
| `OnlineLearningService` | Java | Redis reward stats; bandit RL | Working |
| `HybridRecommendationService` | Java | Combines all signals | Working; DL signal disabled |

### 2.3 Redis Key Contract

| Key Pattern | Writer | Reader |
|-------------|--------|--------|
| `user:{id}:recent` | UserEventStreamingJob | HybridRecommendationService (via hydrators) |
| `global:item_popularity` | UserEventStreamingJob | HybridRecommendationService |
| `i2vEmb:{itemId}` | Item2VecTrainingJob | HybridRecommendationService (`ITEM_EMBEDDING_PREFIX=i2vEmb`) |
| `uEmb:{userId}` | UserEmbeddingTrainingJob | HybridRecommendationService (`USER_EMBEDDING_PREFIX=uEmb`) |
| `alsItemEmb:{movieId}` | AlsEmbeddingTrainingJob | HybridRecommendationService (if `ITEM_EMBEDDING_PREFIX=alsItemEmb`) |
| `alsUserEmb:{userId}` | AlsEmbeddingTrainingJob | HybridRecommendationService (if `USER_EMBEDDING_PREFIX=alsUserEmb`) |
| `reward-model:item:{id}` | OnlineLearningService | OnlineLearningService |
| `reward-model:genre:{g}` | OnlineLearningService | OnlineLearningService |
| `{algo}:q:{stateHash}` | HybridRecommendationService | HybridRecommendationService |
| `replay:recommendations` | HybridRecommendationService | nothing reads this back |

---

## 3. Gap Analysis

### G1 — Python ONNX models are never loaded by the Java service

`movielens_pipeline.py` produces three ONNX files (`movielens_user_tower.onnx`, `movielens_item_tower.onnx`, `movielens_ranking.onnx`) in `sampledata/`. The Java `DeepLearningPredictionService` loads a separate `mlp_embedding_model.onnx` (classpath resource) — a simpler MLP embedding model with different input/output signatures. The two-tower architecture from Python is never exercised in serving.

### G2 — No automated retraining loop

Training samples accumulate in HDFS (`/tmp/spark-recsys/training-samples/`) and `training_experiences` Kafka topic, but nothing triggers a retraining run. Both offline embedding jobs and the Python DL pipeline must be triggered manually.

### G3 — Python pipeline uses hardcoded catalog, not shared data

`movielens_pipeline.py` defines `USER_HISTORY` and `MOVIES` as Python constants. The Spark jobs read `ratings.csv`. There is no shared data layer, so models trained in each system reflect different user populations and item catalogs.

### G4 — Deep learning weight is zero by default

`application.yml` sets `RECSYS_DEEP_LEARNING_WEIGHT=0.0`. Even with `mlp_embedding_model.onnx` loaded, its scores are multiplied by 0 and contribute nothing to ranking. The model runs but is effectively disabled.

### G5 — ALS and UserEmbedding training jobs have no runner scripts

`run-offline-pipeline.sh` only runs `Item2VecTrainingJob`. `AlsEmbeddingTrainingJob` and `UserEmbeddingTrainingJob` have no corresponding scripts. They can only be run with a manual `spark-submit` invocation.

### G6 — Replay buffer in Redis is never consumed by offline retraining

`HybridRecommendationService` stores up to 10,000 RL experiences in `replay:recommendations` Redis list. This is a high-quality labeled dataset (state, action, reward, next state), but nothing exports it or uses it to retrain any model.

### G7 — Catalog is hardcoded in application.yml

The item catalog (title, genres, tags, new-release flag) is embedded in `application.yml` as a static map with 7 items (`item1`–`item7`). This is separate from both `ratings.csv` and `MOVIES` in Python. Content-based scoring (genre/tag overlap) only works for these 7 items.

### G8 — No model hot-reload

`DeepLearningPredictionService` loads the ONNX model once at Spring startup. There is no endpoint or watch mechanism to reload a newly trained model without restarting the service.

---

## 4. Optimization Plan

The goal is a closed loop: streaming events → training data → offline model training → serving → feedback → online learning → back to training. The plan is phased so each phase delivers standalone value.

### Phase 1 — Wire Up What Exists (Quick Wins)

**Effort**: Low (config changes + scripts)
**Value**: Makes the offline→serving path actually functional

**P1.1 — Add runner scripts for ALS and UserEmbedding**

Add `run-als-pipeline.sh` and `run-user-embedding-pipeline.sh` mirroring `run-offline-pipeline.sh`. Both jobs already support all env-var configuration.

```bash
# run-als-pipeline.sh (sketch)
exec "$SPARK_SUBMIT" \
  --class com.demo.task.AlsEmbeddingTrainingJob \
  "$JAR" "$RATINGS_INPUT_PATH" "$ALS_EMBEDDING_OUTPUT_PATH"
```

**P1.2 — Enable deep learning weight**

Change the default from 0.0 to a non-zero value (e.g. 0.15), or document that users must set `RECSYS_DEEP_LEARNING_WEIGHT=0.15` when they want the MLP signal. Without this, the ONNX model is a no-op.

```yaml
# application.yml change
deep-learning-weight: ${RECSYS_DEEP_LEARNING_WEIGHT:0.15}
```

**P1.3 — Document the end-to-end startup sequence**

Update README to show the correct order:
1. Start Docker (Kafka + Redis)
2. Run `Item2VecTrainingJob` with `ITEM2VEC_SAVE_TO_REDIS=true`
3. Run `UserEmbeddingTrainingJob` with `USER_EMBEDDING_SAVE_TO_REDIS=true`
4. Start retrieval service
5. Run producer → starts real-time history updates

---

### Phase 2 — Unified Data Contract

**Effort**: Medium (Python refactor + Spark schema alignment)
**Value**: Single source of truth for all models

**P2.1 — Python pipeline reads from ratings.csv**

Refactor `movielens_pipeline.py` to load user history from `sampledata/ratings.csv` (same file as Spark jobs) instead of the hardcoded `USER_HISTORY` dict. The catalog (`MOVIES`) should be generated from the unique item IDs in ratings.

```python
# Proposed: replace hardcoded USER_HISTORY with
def load_user_history(ratings_csv: Path) -> dict[str, dict]:
    ...  # read CSV, group by userId, build watched/rated_high from rating>=4.0
```

**P2.2 — Unified item catalog**

Replace the 7-item `application.yml` catalog with a catalog loader that reads from a JSON/CSV file (same item metadata used by the Python pipeline). The Java `RecommendationProperties` already supports a `Map<String, MovieProfile>` catalog — inject it from a file at startup.

```yaml
recsys:
  catalog-path: ${RECSYS_CATALOG_PATH:sampledata/catalog.json}
```

**P2.3 — HDFS training samples as shared training set**

Make `OnlineJoinerStreamingJob`'s HDFS output the authoritative training dataset for all model updates. Add a catalog column to the Parquet output so Python/Spark can read genres/tags per item.

---

### Phase 3 — Two-Tower Integration

**Effort**: Medium (Java service extension + Python ONNX contract)
**Value**: Best offline model (two-tower + transformer) used in serving

**P3.1 — Extend DeepLearningPredictionService to load two-tower models**

The existing service loads one MLP model. Extend it (or create a `TwoTowerPredictionService`) to load the three Python-produced models via configurable env vars:

```
ONNX_USER_TOWER_PATH  → sampledata/movielens_user_tower.onnx
ONNX_ITEM_TOWER_PATH  → sampledata/movielens_item_tower.onnx
ONNX_RANKING_PATH     → sampledata/movielens_ranking.onnx
```

The two-tower service would:
1. Compute user embedding via user tower
2. Compute item embeddings (cached) via item tower
3. Dot-product retrieval for candidates
4. Run ranking transformer for final multi-task scores (click, rating, favorite, rewatch, dwell)
5. Aggregate into a single `dlScore` (same interface as current `predictBatch`)

**P3.2 — Align Python ONNX export with Java rating data**

The Python pipeline must train on the same `ratings.csv` so user/item ID lookups match the Java service's `mlp_embedding_lookups.json`. Export user/item lookup tables alongside the ONNX models.

```python
# Export lookup tables for Java consumption
export_lookup_tables(
    users=USER_TO_IDX,
    items=MOVIE_TO_IDX,
    destination=artifacts.ranking.parent / "movielens_lookups.json"
)
```

**P3.3 — Write two-tower embeddings to Redis**

After training, write item embeddings from the item tower to Redis (`i2vEmb:{itemId}` or a new prefix `twoTowerItemEmb:{itemId}`). This allows `HybridRecommendationService.batchRelevanceScores()` to use them without going through ONNX at inference time.

---

### Phase 4 — Automated Retraining Loop

**Effort**: High (orchestration layer)
**Value**: Models stay fresh without manual intervention

**P4.1 — Retraining trigger from HDFS**

Add a batch job (Spark or Python) that watches `ONLINE_JOINER_HDFS_OUTPUT_PATH` and triggers retraining when a configurable number of new samples accumulate (e.g. 10,000 rows since last training run).

```bash
#!/usr/bin/env bash
# run-retrain.sh — check sample count, retrain if threshold crossed
SAMPLE_COUNT=$(hdfs dfs -count $TRAINING_PATH | awk '{print $2}')
if [ "$SAMPLE_COUNT" -gt "$RETRAIN_THRESHOLD" ]; then
  ./run-offline-pipeline.sh          # Item2Vec + UserEmbedding
  python services/python-modeling/movielens_pipeline.py --force-train
  # signal Java service to reload (Phase 4.2)
fi
```

**P4.2 — ONNX model hot-reload endpoint**

Add a Spring Boot actuator endpoint `POST /actuator/model-reload` (secured, actuator-only) that closes and re-opens the ONNX session from the file path. This avoids restart on model update.

```java
// ReloadController.java
@PostMapping("/actuator/model-reload")
public Map<String, String> reload() {
    predictionService.reload();   // close + reopen OrtSession
    return Map.of("status", "ok");
}
```

**P4.3 — Scheduled retraining script**

For environments without Airflow, a cron-based shell script that runs daily:
```
0 2 * * * /app/run-retrain.sh >> /var/log/recsys/retrain.log 2>&1
```

For environments with Airflow/Prefect, a DAG with tasks: wait-for-samples → spark-jobs → python-train → export → reload-java.

---

### Phase 5 — Online-to-Offline Feedback Closure

**Effort**: Medium (Python + Spark)
**Value**: Online learning signals improve offline models

**P5.1 — Export replay buffer to HDFS**

Add a periodic job that reads `replay:recommendations` from Redis and writes the RL experience tuples (state, action, reward, next_state) to HDFS:

```python
# replay_export.py
def export_replay_buffer(redis_client, hdfs_path, batch_size=10_000):
    experiences = redis_client.lrange("replay:recommendations", 0, batch_size - 1)
    # parse JSON, convert to Parquet
```

**P5.2 — Fine-tune Python ranking model on replay data**

Add a fine-tuning mode to `movielens_pipeline.py` that:
1. Loads existing ONNX models
2. Converts them back to PyTorch (or trains from warm init)
3. Fine-tunes on replay buffer samples using the reward signal as the ranking label

**P5.3 — Propagate online item/genre weights to offline scoring**

Periodically read online reward statistics from Redis (`reward-model:item:*`, `reward-model:genre:*`) and use them to weight training samples in the offline BPR/MSE loss:
- High-reward items get higher positive sampling probability in BPR
- Genre weights inform the multi-task loss weighting in the transformer

---

## 5. Implementation Priorities

| Priority | Phase | Change | Files Affected |
|----------|-------|--------|----------------|
| P0 | 1.2 | Enable DL weight default | `application.yml` |
| P0 | 1.3 | Document startup sequence | `recsys-pipeline/README.md` |
| P1 | 1.1 | Add ALS + UserEmb runner scripts | `run-als-pipeline.sh`, `run-user-embedding-pipeline.sh` |
| P1 | 3.1 | TwoTowerPredictionService in Java | `DeepLearningPredictionService.java` (extend or replace) |
| P2 | 2.1 | Python reads from ratings.csv | `movielens_pipeline.py` |
| P2 | 3.2 | Export lookup tables from Python | `movielens_pipeline.py` |
| P2 | 3.3 | Write two-tower embeddings to Redis | `movielens_pipeline.py` |
| P3 | 2.2 | External catalog file | `RecommendationProperties.java`, `application.yml` |
| P3 | 4.2 | ONNX hot-reload endpoint | new `ReloadController.java` |
| P4 | 4.1 | Retraining trigger script | `run-retrain.sh` |
| P4 | 5.1 | Replay buffer export | new `replay_export.py` |
| P5 | 5.2 | Fine-tuning on replay data | `movielens_pipeline.py` |
| P5 | 5.3 | Online weights → offline loss | `movielens_pipeline.py` |

---

## 6. Architecture After Consolidation

```
ratings.csv + HDFS training samples
    │
    ├── Spark offline jobs (ALS / Item2Vec / UserEmbedding)
    │       → Redis embeddings: i2vEmb, uEmb, alsItemEmb, alsUserEmb
    │
    └── Python two-tower pipeline (movielens_pipeline.py)
            reads ratings.csv
            reads HDFS samples (for fine-tuning in Phase 5)
            reads Redis reward stats (for loss weighting in Phase 5)
            → sampledata/movielens_*.onnx
            → sampledata/movielens_lookups.json
            → Redis: twoTowerItemEmb:{itemId}
            → POST /actuator/model-reload

Producer → Kafka: user_events
    │
    ▼
UserEventStreamingJob
    → Redis: user:{id}:recent, global:item_popularity

Kafka: behavior_logs
    │
    ▼
OnlineJoinerStreamingJob
    → HDFS: training_samples (partitioned by date)
    → Kafka: training_samples

    ▼
ExperienceCollectorStreamingJob
    → Kafka: training_experiences (slates)

    ▼
run-retrain.sh (cron / Airflow DAG)
    monitors HDFS sample count
    triggers Spark + Python training
    calls POST /actuator/model-reload

Java Retrieval Service
    ├── TwoTowerPredictionService (new)
    │       loads movielens_user_tower.onnx
    │       loads movielens_item_tower.onnx
    │       loads movielens_ranking.onnx
    │       hot-reloads on POST /actuator/model-reload
    │
    ├── OnlineLearningService (existing)
    │       Redis reward stats
    │       Bandit (UCB/Thompson/Q/SARSA)
    │
    ├── HybridRecommendationService (extended)
    │       embedding cosine (Redis ← Spark offline jobs)
    │       DL score (TwoTowerPredictionService, weight > 0)
    │       online reward score
    │       bandit policy
    │
    └── Replay buffer → replay_export.py → HDFS → fine-tuning loop
```

---

## 7. Testing Strategy

| Phase | Test | Verify |
|-------|------|--------|
| 1 | Run `Item2VecTrainingJob` + `UserEmbeddingTrainingJob` with `SAVE_TO_REDIS=true` | `redis-cli keys "i2vEmb:*"` and `"uEmb:*"` populated |
| 1 | Call `GET /recommend/someUser` | response includes `relevanceScore > 0` |
| 2 | Run `movielens_pipeline.py --model-dir sampledata/` | 3 ONNX files created in sampledata/ |
| 3 | Set `ONNX_USER_TOWER_PATH`, restart service | `GET /predict/metadata` shows two-tower model name |
| 3 | Call `GET /predict/{user}/{item}` | returns non-zero score for known user/item |
| 4 | Run `run-retrain.sh` with sample threshold=0 | new ONNX files created, service reloads |
| 5 | Send 100 feedback events, run replay export | `sampledata/replay_*.parquet` contains labeled rows |

---

## 8. Out of Scope

- Distributed model serving (TensorFlow Serving, Triton) — current ONNX runtime is sufficient
- Feature store (Feast, Tecton) — Redis serves this role; a full feature store is a separate project
- A/B testing infrastructure — the bandit algorithms serve as the exploration mechanism
- Online gradient updates to the DL model — SGD inside the Java service would require GPU; keep DL offline
