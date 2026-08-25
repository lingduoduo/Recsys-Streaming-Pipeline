# Consolidating the supervised rankers onto a single Spark SFT stage

**Date:** 2026-08-21
**Status:** Approved, ready for implementation planning

## Context

The repository trains four supervised rankers. Three of them never influence a recommendation:

| Ranker | Trained on | Reaches serving? |
|---|---|---|
| PyTorch `train_ranking` → ONNX | 3 hardcoded users, 30 hardcoded movies | No — `deepLearningWeight` defaults to `0.0` |
| PyTorch `train_two_tower` → ONNX | same | No — same gate |
| Spark `CtrRankingModelTrainingJob` | the real `training_samples` store | No — documented "offline only" |
| `OnlineLearningService` reward model | live feedback | Yes |

The duplication is not dead files — every one of these has a script, docs, and a test. It is four
implementations of the same idea, most of them dark. That is what makes the repository hard to read:
`movielens_pipeline.py` is its single largest module and looks central, while producing nothing any
serving path consumes.

Three facts establish that last claim, each verified against the code:

1. **The ranking and two-tower ONNX models are unreachable.** `deepLearningEnabled` is
   `deepLearningWeight > 0.0` (`HybridRecommendationService:212`), the property defaults to `0.0`,
   and no script or test sets `RECSYS_DEEP_LEARNING_WEIGHT`. `predictBatch` is never called, so every
   candidate's logged `deepLearningScore` is a constant `0.0`.
2. **The Redis embeddings it writes have no reader.** `--save-embeddings-to-redis` writes
   `twoTowerItemEmb:*`; that prefix appears nowhere outside `movielens_pipeline.py`. The serving path
   reads `i2vEmb` / `uEmb` (`application.yml:35-41`), written by the Spark Item2Vec and
   UserEmbedding jobs.
3. **Nothing imports it.** No Python module imports `movielens_pipeline`, and `ONNX_MODEL_PATH` —
   the env var `DeepLearningPredictionService` needs — is set by no script.

## Decision

`CtrRankingModelTrainingJob` becomes the single supervised-ranker stage. The PyTorch training
pipeline is deleted, and so is the Java ONNX serving path that would otherwise be left with no
producer.

Deleting the producer while keeping the consumer would leave *more* orphaned code than we started
with, which is why the serving path goes in the same change.

## Scope

### Deleted — producer

- `services/python-modeling/movielens_pipeline.py` (1198 lines)
- `integration-tests/python_modeling/test_movielens_pipeline.py` (446 lines)
- `sampledata/movielens_ranking.onnx`, `movielens_user_tower.onnx`, `movielens_item_tower.onnx`
- `scripts/run-retrain.sh` steps 5 and 6 — the pipeline invocation, and the
  `POST /actuator/model-reload` call that would have nothing left to reload

### Deleted — orphaned consumer

- `DeepLearningPredictionService.java`, `TwoTowerPredictionService.java`,
  `ModelReloadController.java`, and their tests
- `deepLearningWeight` from `RecommendationProperties` and `application.yml`
- the `dlScore` plumbing in `HybridRecommendationService:212-235`
- `deepLearningScore` from `modelPredictions`
- the `deepLearningWeight` / `deepLearningScore` parameters of
  `RecommendationConstants.blendOfflineScore`
- `EXPLOITATION_DL_WEIGHT` and the `0.75 * clamp(dlScore)` logit term in `MovieLensOutcomeScorer`
- the `com.microsoft.onnxruntime` dependency and its `onnxruntime.version` property from
  `services/java-retrieval-service/pom.xml` — with both ONNX services gone, nothing links against it

### Deleted — documentation

References in `README.md` (three places), `docs/recommendation_architecture/Data_Pipeline.md`, and
`docs/recommendation_flows/6_Predicting_Scoring.md`.

### Untouched

The live embedding path (Item2Vec, UserEmbedding, ALS), the online reward model, the bandit and
tabular-RL policies, the post-training track, and `next_item_model.py` — an offline evaluation
harness, not a serving ranker, and not part of this duplication.

## The property that makes this safe

**Every removed term already contributes exactly zero**, because `deepLearningWeight` is `0.0`:

- `blendOfflineScore` adds `deepLearningWeight * clamp(deepLearningScore)` to the numerator and
  `deepLearningWeight` to `weightSum`. Both addends are `0`, so removing the parameter pair leaves
  the returned value identical.
- `MovieLensOutcomeScorer` contributes `EXPLOITATION_DL_WEIGHT * clamp(dlScore)` = `0.15 * 0` and
  `0.75 * clamp(dlScore)` = `0.75 * 0` to the base logit. Both are `0`.

**The surviving weights are therefore NOT renormalized.** Leaving them preserves output exactly;
renormalizing would change every score in the service. The comment in `MovieLensOutcomeScorer`
claiming the exploitation weights "sum to 1.0" is corrected to state the real maximum, `0.85`, which
is what it has always been in practice.

This turns the refactor into a behavior-preserving one, which is what makes it verifiable.

## Tests

The existing suites must pass **unchanged**, except four that deliberately exercise the removed
path and are amended or deleted with it:

| Test | Why it changes |
|---|---|
| `RecommendationConstantsTest:32` | asserts dlScore blending at a non-zero weight; that case is deleted |
| `HybridRecommendationServiceTest:112` | asserts the ONNX model does not run at the default weight; moot once the path is gone |
| `integration-tests/test_application_config.py:28-33` | validates the `deep-learning-weight` config key |
| `TwoTowerPredictionServiceTest` | deleted with the service it tests |

## Success criteria

1. **A characterization test written BEFORE any deletion pins the scoring output.** It calls
   `RecommendationConstants.blendOfflineScore` and `MovieLensOutcomeScorer.score` with fixed inputs
   and asserts their exact current values, then must pass **unchanged** after the removal. This is
   the primary criterion, and it is deliberately not a live `/recommend` diff: the service needs
   Redis and Kafka, which are not available here, and a criterion that cannot be run is not a
   criterion. Pinning the two pure functions the removed terms feed into tests the same property
   and runs anywhere.
2. The Java and Python suites pass with only the four amendments listed above.
3. `grep -ri "onnx\|deepLearning"` over `services/java-retrieval-service/src/main`,
   `services/java-retrieval-service/pom.xml`, and `services/python-modeling` returns nothing.
4. `run-retrain.sh` completes without its removed steps.
5. No remaining reference to the deleted files in `README.md` or `docs/`.

## Consequences

- Newly logged `modelPredictions` no longer carry `deepLearningScore`, removing a constant-zero dead
  feature from the post-training arms' feature vectors. Replay dumps recorded before this change keep
  working, since `ope_eval_report.feature_names` derives the schema from the data on each run.
- Serving a neural model in future means re-adding the plumbing. That is the accepted cost: the
  plumbing as it stands has never served one, and an unused path that looks used is worse than an
  absent one.
- Roughly 1,900 lines are removed and the repository is left with one supervised ranker instead of
  four.

## Risks

- **The `run-retrain.sh` step numbering and the README's pipeline diagram both encode the removed
  steps.** Deleting code without updating them leaves the documentation describing a flow that no
  longer exists — the exact failure this change exists to remove. Criterion 5 covers it.
- **`ModelReloadController` may be referenced by an actuator-endpoint test or config allow-list**
  beyond the greps performed during design. The implementation must search for it rather than assume
  the three known references are complete.
- **Behavior preservation depends on `deepLearningWeight` being `0.0` everywhere**, including any
  environment file not in the repository. If a deployment sets `RECSYS_DEEP_LEARNING_WEIGHT`, this
  change alters its ranking. The repository contains no such setting; a deployment that added one is
  outside what can be verified here and is called out in the README removal.
