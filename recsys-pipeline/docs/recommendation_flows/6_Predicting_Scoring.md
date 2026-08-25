# Scoring Model Architecture

**Flow:** [Previous](5_Candidate_Hydration.md) · **Current: Predicting and scoring** · [Next](7_Shuffling.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

For the complete local startup sequence, follow the [root quick start](../../../README.md#recsys-pipeline).

Candidate items are scored through three stages, each with a different learning paradigm:

## Required state

- `DeepLearningPredictionService` always loads the base MLP and lookup table when its Spring bean
  is created. `ONNX_MODEL_PATH` and `ONNX_LOOKUPS_PATH` override the default classpath pair
  `mlp_embedding_model.onnx` and `mlp_embedding_lookups.json`; an absent, unreadable, malformed, or
  invalid artifact aborts application startup with `Failed to load deep learning prediction
  artifacts`. This service backs the standalone `GET /predict/*` endpoints (see
  [API.md](../recommendation_architecture/API.md)); it is not part of the `offlineScore` /
  `onlineScore` / `banditScore` pipeline below.
- Hybrid relevance reads Redis vectors at
  `{recsys.embeddings.user-prefix}:{user}` and
  `{recsys.embeddings.item-prefix}:{item}` (defaults `uEmb:*` and `i2vEmb:*`). Online scoring reads
  `reward-model:global`, `reward-model:item:*`, `reward-model:genre:*`, and
  `reward-model:tag:*`; bandit scoring reads item impression/click counters and, for tabular
  policies, `q-learning:q:{stateKey}` or `sarsa:q:{stateKey}`.
- Raw numeric calls to `GET /predict/id` must use zero-based internal lookup indices:
  `userId` in `0..users-1` and `itemId` in `0..items-1`, using the sizes from
  `GET /predict/metadata`. External IDs should use `GET /predict/{user}/{item}` so the lookup table
  resolves them.

If Redis vectors or reward counters are absent, the affected score components fall back to empty
vectors or zero/prior values and the remaining signals still rank candidates. A valid base lookup
file may contain no matching external ID; that request returns `unknown_user_or_item` without
stopping the service. An out-of-bounds numeric index returns HTTP 400 instead of being passed to
ONNX.

| Model type | Class | Signal | Update cadence |
|---|---|---|---|
| **Online** | `OnlineLearningService` | Weighted mean reward per item, genre, tag, and global prior | After every `/feedback` call |
| **Bandit / RL** | `HybridRecommendationService` | UCB, Thompson Sampling, Q-learning, or SARSA score from replay state/action/reward events | After impressions and `/feedback` rewards |

All paths share the same `offlineScore` and `learnedPrior` base. The final `banditScore` diverges by algorithm:

```
offlineScore  = relevanceWeight × cosine(userEmb, itemEmb)
              + contentWeight   × genreTagOverlap
              + popularityWeight × normalizedPopularity

learnedPrior  = offlineScore × (1 − onlineWeight) + onlineScore × onlineWeight

── UCB / Thompson ────────────────────────────────────────────────
banditScore   = BetaPosteriorMean(learnedPrior, clicks, impressions)
              + explorationBonus(UCB | Thompson)

── Q-learning / SARSA ───────────────────────────────────────────
banditScore   = Q(stateKey, item)   ← tabular Q-value in Redis, updated via Bellman equation
```

## Bandit algorithm notes

All four algorithms consume the same `learnedPrior` — a blend of `offlineScore` (static signals) and `onlineScore` (real-time reward model) — so bandit updates refine rather than replace the base ranker.

- **`ucb`** — builds a Beta-smoothed posterior mean for each item, then adds a confidence term proportional to `sqrt(log(total_impressions) / pulls)`.
- **`thompson`** — builds the same posterior and ranks items by sampling from the Beta posterior, giving a stochastic arm draw per request.
- **`q-learning`** — stores tabular Q-values in Redis under `q-learning:q:{stateKey}` and updates from feedback with `Q(s,a) += alpha * (reward + gamma * max_a Q(s',a) - Q(s,a))`.
- **`sarsa`** — stores tabular Q-values under `sarsa:q:{stateKey}` and updates with the on-policy target `reward + gamma * Q(s', a')`, where `a'` is selected by the same epsilon-greedy policy used for serving.

The `relevanceWeight` / `contentWeight` / `popularityWeight` weights do not need to sum to `1.0`; scores are clamped to `[0, 1]` at each stage.

Switch algorithms by setting `RECSYS_BANDIT_ALGORITHM` to `ucb`, `thompson`, `q-learning`, or `sarsa`.

The bandit and reward-model weights that feed these formulas are configured in the
[Retrieval Service Configuration](../../README.md#retrieval-service-configuration).
