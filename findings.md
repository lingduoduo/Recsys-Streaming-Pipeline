# findings.md — Recsys-Streaming-Pipeline Consolidation Research

_Created: 2026-06-14_

---

## 1. Codebase Map

```
recsys-pipeline/
├── services/
│   ├── spark-streaming-job/        # Scala / Spark — offline training + streaming
│   │   ├── task/
│   │   │   ├── AlsEmbeddingTrainingJob.scala    # ALS matrix factorization
│   │   │   ├── Item2VecTrainingJob.scala         # Word2Vec on item sequences
│   │   │   ├── UserEmbeddingTrainingJob.scala    # Weighted-avg user embeddings
│   │   │   └── UserEventStreamingJob.scala       # Kafka→Redis (recent/popularity)
│   │   └── process/
│   │       ├── OnlineJoinerStreamingJob.scala    # behavior logs→training samples
│   │       ├── ExperienceCollectorStreamingJob.scala # samples→slates
│   │       └── ItemSequencePreprocessingJob.scala
│   ├── java-retrieval-service/     # Java / Spring Boot — serving
│   │   └── service/
│   │       ├── DeepLearningPredictionService.java  # offline ONNX model
│   │       ├── OnlineLearningService.java           # reward stats in Redis
│   │       └── HybridRecommendationService.java     # combines all signals
│   └── python-modeling/
│       ├── movielens_pipeline.py    # two-tower + transformer, exports ONNX
│       └── producer.py              # Kafka event producer
```

---

## 2. Three Offline Training Systems (Unconnected)

### 2A. Spark ALS (`AlsEmbeddingTrainingJob`)
- Input: `ratings.csv` (userId, movieId, rating, timestamp)
- Algorithm: Collaborative filtering (matrix factorization, rank=16, maxIter=10)
- Output: text files + optional Redis (`alsItemEmb:{movieId}`, `alsUserEmb:{userId}`)
- TTL: 86400s default
- **No runner script** — not in `run-offline-pipeline.sh`

### 2B. Spark Item2Vec (`Item2VecTrainingJob`)
- Input: `ratings.csv` → item sequences via `ItemSequencePreprocessingJob`
- Algorithm: Word2Vec (vectorSize=10, window=5, minCount=1, numIterations=10)
- Output: text file + optional Redis (`i2vEmb:{itemId}`)
- **Has runner script** — `run-offline-pipeline.sh` runs only this job

### 2C. Spark UserEmbedding (`UserEmbeddingTrainingJob`)
- Input: `ratings.csv` + item embedding file from 2B
- Algorithm: Weighted-average of item embeddings for rated items (minRating=3.5)
- Output: text file + optional Redis (`uEmb:{userId}`)
- **No runner script** — depends on 2B output, but not scripted

### 2D. Python Two-Tower + Ranking (`movielens_pipeline.py`)
- Input: **hardcoded `USER_HISTORY` dict** (3 users: alice, bob, charlie) — NOT ratings.csv
- Models: UserTower (embedding+MLP), ItemTower (embedding+genre projection), RankingTransformer (2-layer transformer, 4-head)
- Training: BPR loss for two-tower, multi-task BCE+MSE for ranker
- Output: 3 ONNX files in `sampledata/`: `movielens_user_tower.onnx`, `movielens_item_tower.onnx`, `movielens_ranking.onnx`
- **NOT loaded by Java retrieval service**

---

## 3. Online Training System

### 3A. Reward Statistics Model (`OnlineLearningService.java`)
- Storage: Redis hashes
  - `reward-model:global` → {count, reward_total}
  - `reward-model:item:{id}` → {count, reward_total}
  - `reward-model:genre:{g}` → {count, reward_total}
  - `reward-model:tag:{t}` → {count, reward_total}
- Update: `POST /feedback` triggers pipeline write (count++, reward_total+=reward)
- Score: weighted combination of global/item/genre/tag estimates with confidence scaling
- Bandit algorithms: UCB, Thompson sampling, Q-learning (TD), SARSA
- Q-values stored in Redis: `{algo}:q:{stateHash}` → {itemId: qValue}
- Replay buffer: Redis list `replay:recommendations`, max 10,000 entries

---

## 4. Java Retrieval Service — What It Actually Loads

### Offline model (DeepLearningPredictionService)
- Loads: `mlp_embedding_model.onnx` + `mlp_embedding_lookups.json` from classpath
- Overridable via env: `ONNX_MODEL_PATH`, `ONNX_LOOKUPS_PATH`
- This is an MLP embedding model — **different from the two-tower models in movielens_pipeline.py**
- Inputs: `user_ids` (long[]), `item_ids` (long[]) → scores (float[])

### Embedding reads (HybridRecommendationService)
- Item vectors: Redis key `{ITEM_EMBEDDING_PREFIX}:{itemId}` (default: `i2vEmb:{itemId}`)
- User vectors: Redis key `{USER_EMBEDDING_PREFIX}:{userId}` (default: `uEmb:{userId}`)
- Keys set via env: `ITEM_EMBEDDING_PREFIX`, `USER_EMBEDDING_PREFIX`
- **This aligns with Item2Vec and UserEmbedding jobs — only missing runner scripts**

### Scoring pipeline (`HybridRecommendationService.scoreCandidate`)
```
offlineScore = relevanceWeight * cosine(userVec, itemVec)
             + contentWeight * genre/tag overlap
             + popularityWeight * log1p(popularity)
             + deepLearningWeight * mlpScore (default weight=0.0!)
onlineScore = OnlineLearningService.score() [reward stats]
learnedPrior = offlineScore*(1-onlineWeight) + onlineScore*onlineWeight
banditScore = UCB/Thompson/Q-learning on top of learnedPrior
```

Note: `RECSYS_DEEP_LEARNING_WEIGHT=0.0` by default — MLP score is ignored unless explicitly set.

---

## 5. Training Data Pipeline (Spark Streaming)

| Job | Input Kafka Topic | Output |
|-----|-------------------|--------|
| `UserEventStreamingJob` | `user_events` | Redis `user:{id}:recent`, `global:item_popularity` |
| `OnlineJoinerStreamingJob` | `behavior_logs` | Kafka `training_samples` + HDFS Parquet |
| `ExperienceCollectorStreamingJob` | `training_samples` | Kafka `training_experiences` (slates) |

**Dead end**: Training samples accumulate in HDFS and `training_experiences` Kafka topic, but **no job reads them back to trigger retraining**.

---

## 6. Critical Gaps (Root Causes)

| # | Gap | Impact |
|---|-----|--------|
| G1 | Python ONNX models never loaded by Java service | two-tower/ranking model unused |
| G2 | No automated retraining loop | offline models stale after initial training |
| G3 | Python pipeline uses hardcoded data, not ratings.csv | Python and Spark work on different datasets |
| G4 | deepLearningWeight=0.0 default | MLP model loaded but not scored |
| G5 | ALS + UserEmbedding have no runner scripts | those models can't be run without manual spark-submit |
| G6 | Replay buffer in Redis never consumed by offline jobs | online signals don't feed back into offline models |
| G7 | Embedding key scheme is env-var-only, undocumented | easy to misconfigure; user→uEmb mapping unclear |
| G8 | Catalog hardcoded in application.yml | no way to update catalog without redeployment |

---

## 7. What IS Connected and Working

- Kafka → Spark → Redis (user history + popularity): **working**
- Redis embeddings → Java scoring: **working** (if embeddings were written)
- Feedback endpoint → Redis reward stats: **working**
- Bandit policy: **working**
- Candidate pipeline (filter/hydrate/score): **working**
- `run-offline-pipeline.sh` → Item2Vec → file: **working** (Redis write optional)
