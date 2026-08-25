# SFT Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Leave the repository with one supervised ranker — Spark's `CtrRankingModelTrainingJob` — by deleting the PyTorch training pipeline and its orphaned two-tower Java ranking path, without changing any score the service computes. The independent `/predict` ONNX API is preserved.

**Architecture:** This is a deletion refactor. Behaviour is pinned first by characterization tests, then the producer (Python) and its `TwoTowerPredictionService` consumer are removed, then documentation is swept. `DeepLearningPredictionService`, `ModelReloadController`, the `/predict` endpoints, their resources and tests, and the ONNX runtime dependency remain because they form a separate live API.

**Tech Stack:** Java 17 + Maven + JUnit 5 (retrieval service), Python 3 + pytest (modeling), Scala + sbt (Spark jobs — untouched).

**Spec:** `.superpowers/docs/specs/2026-08-25-consolidate-sft-design.md`

## Global Constraints

- **Java builds and tests require JDK 17.** The default `java` on this machine is 18 and the build fails on it. Every Maven command must be prefixed:
  `JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home`
  Verified working: `mvn -q test -Dtest=RecommendationConstantsTest` runs 5 tests green.
- **Do NOT renormalize any surviving weight.** Every removed term already contributes exactly zero, so leaving the other weights untouched preserves output exactly; renormalizing would change every score. The `MovieLensOutcomeScorer` comment claiming the exploitation weights "sum to 1.0" is corrected to state its real maximum, `0.85`.
- **The two removal sites are inert for different reasons.** `blendOfflineScore` is inert because `deepLearningWeight` is `0.0`. `MovieLensOutcomeScorer` is *not* gated by that weight — it applies a hardcoded `0.15` to `input.dlScore()`, and is inert only because `dlScore` is always `0.0` at runtime (the `deepLearningEnabled` gate stops `predictBatch` from ever running).
- **Untouched:** the Spark jobs, the live embedding path (`i2vEmb` / `uEmb` / ALS), the online reward model, the bandit and tabular-RL policies, the `post-training/` track, and `next_item_model.py`.
- **Never stage `recsys-pipeline/frontend/data/dashboard.json`** — it is dirty from unrelated user work. Use explicit `git add` paths.
- Python tests run from `recsys-pipeline/`: `python3 -m pytest integration-tests/python_modeling/ -q` (485 passing at the start of this plan).

## File Structure

| File | Change |
|---|---|
| `services/java-retrieval-service/src/test/java/com/demo/retrieval/service/RecommendationConstantsTest.java` | Add characterization tests (Task 1), amend two dl tests (Task 3) |
| `.../service/scorers/MovieLensOutcomeScorerTest.java` | Add golden-value characterization test (Task 1) |
| `services/python-modeling/movielens_pipeline.py` | **Delete** (Task 2) |
| `integration-tests/python_modeling/test_movielens_pipeline.py` | **Delete** (Task 2) |
| `sampledata/movielens_{ranking,user_tower,item_tower}.onnx` | **Delete** (Task 2) |
| `scripts/run-retrain.sh` | Remove steps 5 and 6 (Task 2) |
| `.../service/TwoTowerPredictionService.java` + test | **Delete** (Task 3) |
| `.../service/HybridRecommendationService.java` | Remove dl plumbing (Task 3) |
| `.../service/RecommendationConstants.java` | Drop two parameters (Task 3) |
| `.../service/scorers/MovieLensOutcomeScorer.java` | Drop dl terms and `ScoringInput.dlScore` (Task 3) |
| `.../config/RecommendationProperties.java`, `src/main/resources/application.yml` | Drop `deepLearningWeight` (Task 3) |
| `.../service/DeepLearningPredictionService.java`, `.../controller/ModelReloadController.java`, resources, tests, and `pom.xml` | **Keep** for the independent `/predict` API |
| `integration-tests/test_application_config.py` | Drop the `deep-learning-weight` assertion (Task 3) |
| `README.md`, `docs/recommendation_architecture/Data_Pipeline.md`, `docs/recommendation_flows/6_Predicting_Scoring.md` | Documentation sweep (Task 4) |

---

### Task 1: Characterization tests

Pin the behaviour that must not change, **before anything is deleted**. These tests are the licence for the removal.

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/RecommendationConstantsTest.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorerTest.java`

**Interfaces:**
- Consumes: `RecommendationConstants.blendOfflineScore(double, double, double, double, double, double, double, double)` (8 args today), `MovieLensOutcomeScorer.score(ScoringInput)`, `ScoringInput(String itemId, double relevance, double content, double popularity, double posteriorMean, double banditRankingScore, double explorationBonus, double noveltyScore, double dlScore, double qValue, long impressions, long clicks)`, `ScoringResult.predictionScore()`, `ScoringResult.estimatedReward()`.
- Produces: two test methods in each file, named below. Task 3 updates their call sites but must not change any asserted value.

- [ ] **Step 1: Add the blend characterization tests**

Append inside the `RecommendationConstantsTest` class body:

```java
    // ---- Characterization: pinned before the deep-learning path is removed --------------------
    // The expected value is written as explicit arithmetic over the SURVIVING weights only, so
    // this assertion is unchanged when the two deep-learning parameters are dropped. If the number
    // still matches after the removal, no score moved.
    @Test
    void offlineBlendCharacterizedAtTheProductionDeepLearningWeight() {
        double expected = (0.6 * 0.8 + 0.25 * 0.5 + 0.15 * 0.4) / (0.6 + 0.25 + 0.15);
        double actual = RecommendationConstants.blendOfflineScore(
            0.6, 0.8,
            0.25, 0.5,
            0.15, 0.4,
            0.0, 0.9   // deep-learning weight 0.0 is the production default
        );
        assertEquals(expected, actual, 1e-12);
    }

    // At the production weight the deep-learning SCORE cannot matter at all, whatever it is.
    // This is the property that makes dropping the parameter pair safe.
    @Test
    void offlineBlendIgnoresTheDeepLearningScoreEntirelyAtWeightZero() {
        double withScore = RecommendationConstants.blendOfflineScore(
            0.6, 0.8, 0.25, 0.5, 0.15, 0.4, 0.0, 0.9);
        double withoutScore = RecommendationConstants.blendOfflineScore(
            0.6, 0.8, 0.25, 0.5, 0.15, 0.4, 0.0, 0.0);
        assertEquals(withScore, withoutScore, 0.0);
    }
```

- [ ] **Step 2: Add the scorer characterization test**

The scorer is a chain of sigmoids, so its expected values are obtained by running rather than by hand. This is a deliberate golden-value test, not a placeholder — Step 3 gives the exact procedure for filling in the two numbers.

Append inside the `MovieLensOutcomeScorerTest` class body:

```java
    // ---- Characterization: pinned before the deep-learning terms are removed ------------------
    // Unlike blendOfflineScore, this class is NOT gated by deepLearningWeight -- it applies a
    // hardcoded 0.15 to input.dlScore() directly. It is inert only because dlScore is always 0.0
    // at runtime (the deepLearningEnabled gate stops predictBatch from ever running). So the
    // input below uses dlScore = 0.0, the only value that actually occurs, and pins the result.
    // Removing the dl terms must leave both numbers untouched.
    @Test
    void scoringCharacterizedAtTheOnlyDeepLearningScoreThatOccurs() {
        MovieLensOutcomeScorer scorer = new MovieLensOutcomeScorer();
        MovieLensOutcomeScorer.ScoringResult result = scorer.score(
            new MovieLensOutcomeScorer.ScoringInput(
                "m1",
                0.70,   // relevance
                0.50,   // content
                0.40,   // popularity
                0.30,   // posteriorMean
                0.60,   // banditRankingScore
                0.05,   // explorationBonus
                0.20,   // noveltyScore
                0.0,    // dlScore -- the only value the gate permits
                0.10,   // qValue
                40L,    // impressions
                8L      // clicks
            ));

        assertEquals(0.0, result.predictionScore(), 1e-9);
        assertEquals(0.0, result.estimatedReward(), 1e-9);
    }
```

The two `0.0` expectations are deliberately wrong so the first run reports the real values.

- [ ] **Step 3: Run the scorer test and substitute the real golden values**

Run:
```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/services/java-retrieval-service
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
  mvn -q test -Dtest=MovieLensOutcomeScorerTest
```
Expected: FAIL, with two messages of the form `expected: <0.0> but was: <X>`.

Read the two actual values from the failure output and replace the `0.0` expectations with them, keeping the `1e-9` tolerances. Record both numbers in your report — they are the characterization.

If the class or record names differ from the snippet (for example if `ScoringResult` is a
top-level type rather than nested), adjust the references to match the real source and say so in
your report; do not change the input values or the tolerances.

- [ ] **Step 4: Run both test classes to verify they pass**

Run:
```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/services/java-retrieval-service
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
  mvn -q test -Dtest='RecommendationConstantsTest,MovieLensOutcomeScorerTest'
```
Expected: PASS. Confirm via `cat target/surefire-reports/com.demo.retrieval.service.RecommendationConstantsTest.txt` that failures and errors are 0.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/RecommendationConstantsTest.java \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorerTest.java
git commit -m "test: characterize offline blend and outcome scoring before removing the ONNX path"
```

---

### Task 2: Delete the PyTorch producer

**Files:**
- Delete: `recsys-pipeline/services/python-modeling/movielens_pipeline.py`
- Delete: `recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py`
- Delete: `recsys-pipeline/sampledata/movielens_ranking.onnx`, `movielens_user_tower.onnx`, `movielens_item_tower.onnx`
- Modify: `recsys-pipeline/scripts/run-retrain.sh`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: nothing Task 3 depends on. These two tasks are independent; only the docs sweep in Task 4 depends on both.

- [ ] **Step 1: Confirm nothing imports the module**

Run:
```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
grep -rn "import movielens_pipeline\|from movielens_pipeline" --include="*.py" . | grep -v __pycache__
```
Expected: no output. If there IS output, stop and report — the spec's premise that nothing imports it would be wrong.

- [ ] **Step 2: Delete the module, its test, and the artifacts**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git rm recsys-pipeline/services/python-modeling/movielens_pipeline.py \
       recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py \
       recsys-pipeline/sampledata/movielens_ranking.onnx \
       recsys-pipeline/sampledata/movielens_user_tower.onnx \
       recsys-pipeline/sampledata/movielens_item_tower.onnx
```

- [ ] **Step 3: Remove steps 5 and 6 from the retrain script**

In `recsys-pipeline/scripts/run-retrain.sh`, delete the whole `if [[ "${SKIP_PYTHON}" == "0" ]]` block that begins with `echo "Step 5: Running Python two-tower pipeline with fine-tuning"`, and the whole `if [[ "${SKIP_RELOAD}" == "0" ]]` block that begins with `echo "Step 6: Hot-reloading ONNX model in Java service at ${RECSYS_SERVICE_URL}"`.

Then remove anything left dangling by those two blocks: the `SKIP_PYTHON` and `SKIP_RELOAD` variables and their argument parsing, the `MODEL_DIR` and `RECSYS_SERVICE_URL` variables if nothing else uses them, and any `--skip-python` / `--skip-reload` entries in the script's usage text. Verify with:

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
grep -n "SKIP_PYTHON\|SKIP_RELOAD\|MODEL_DIR\|RECSYS_SERVICE_URL\|model-reload\|movielens_pipeline" scripts/run-retrain.sh
```
Expected: no output. Renumber any remaining `Step N:` echoes so the sequence stays contiguous.

- [ ] **Step 4: Verify the script still parses and the Python suite passes**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
bash -n scripts/run-retrain.sh && echo "syntax OK"
python3 -m pytest integration-tests/python_modeling/ -q 2>&1 | tail -2
```
Expected: `syntax OK`, then the suite passes with 485 minus the deleted file's tests. Report the new count.

- [ ] **Step 5: Commit**

```bash
git add -A recsys-pipeline/services/python-modeling recsys-pipeline/integration-tests/python_modeling \
           recsys-pipeline/sampledata recsys-pipeline/scripts/run-retrain.sh
git commit -m "refactor: delete the PyTorch training pipeline and its ONNX artifacts

Nothing consumed any of its three outputs: the ranking and two-tower ONNX
models are unreachable behind a deepLearningWeight that defaults to 0.0, and
the twoTowerItemEmb Redis prefix it wrote appears nowhere else. Spark's
CtrRankingModelTrainingJob is the supervised ranker from here."
```

---

### Task 3: Remove the Java two-tower ranking path

This is one task because the build does not compile between deleting `TwoTowerPredictionService` and removing its injection and scoring call sites. Do not split it. The similarly named `DeepLearningPredictionService` is not part of this path and must remain.

**Files:**
- Delete: `.../service/TwoTowerPredictionService.java` and its test
- Modify: `.../service/HybridRecommendationService.java`, `.../service/RecommendationConstants.java`, `.../service/scorers/MovieLensOutcomeScorer.java`, `.../config/RecommendationProperties.java`, `src/main/resources/application.yml`
- Keep byte-identical: `DeepLearningPredictionService`, `ModelReloadController`, their resources and tests, and the `onnxruntime` dependency
- Modify: `.../service/RecommendationConstantsTest.java`, `.../service/HybridRecommendationServiceTest.java`, `recsys-pipeline/integration-tests/test_application_config.py`

**Interfaces:**
- Consumes: the characterization tests from Task 1. Their asserted values must not change.
- Produces: `RecommendationConstants.blendOfflineScore(double relevanceWeight, double relevance, double contentWeight, double content, double popularityWeight, double popularity)` — six arguments. `ScoringInput` without its `dlScore` component.

- [ ] **Step 1: Find every reference before deleting anything**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/services/java-retrieval-service
grep -rn "DeepLearningPredictionService\|TwoTowerPredictionService\|ModelReloadController\|deepLearningWeight\|dlScore\|deepLearningScore\|onnx\|Onnx\|ONNX" src/ pom.xml
```
Record the full list in your report. Use it to distinguish the ranking path from the independent `/predict` API; do not delete a reference merely because it mentions ONNX.

- [ ] **Step 2: Delete the two-tower class and its test**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git rm recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/TwoTowerPredictionService.java \
       recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/TwoTowerPredictionServiceTest.java
```

Confirm `DeepLearningPredictionService`, `ModelReloadController`, their tests/resources, and the three `/predict` endpoints still exist.

- [ ] **Step 3: Drop the two parameters from `blendOfflineScore`**

In `.../service/RecommendationConstants.java`, change the method to:

```java
    public static double blendOfflineScore(
        double relevanceWeight, double relevance,
        double contentWeight, double content,
        double popularityWeight, double popularity
    ) {
        double weightSum = relevanceWeight + contentWeight + popularityWeight;
        if (weightSum <= 0.0) {
            return SCORE_LOWER_BOUND;
        }
        double weighted = (relevanceWeight * clamp(relevance))
            + (contentWeight * clamp(content))
            + (popularityWeight * clamp(popularity));
        return clamp(weighted / weightSum);
    }
```

Update its Javadoc if it mentions the deep-learning component.

- [ ] **Step 4: Remove the deep-learning terms from the scorer**

In `.../service/scorers/MovieLensOutcomeScorer.java`:

1. Delete the `EXPLOITATION_DL_WEIGHT` constant (line 17) and its term from the `exploitation` sum (line 38).
2. Delete the `+ 0.75 * clamp(input.dlScore())` term from the base logit (line 86).
3. Delete `double dlScore,` from the `ScoringInput` record (line 152).
4. Correct the comment above the exploitation constants. It currently claims the weights "sum to 1.0"; they now sum to `0.55 + 0.25 + 0.05 = 0.85`. Replace it with:

```java
    // Exploitation blend applied to the ranking score in score() (weights sum to 0.85).
    // They summed to 1.0 only on paper: the fourth term was a deep-learning score that was always
    // 0.0 at runtime, so the reachable maximum has always been 0.85. The remaining weights are
    // deliberately NOT renormalized -- renormalizing would change every score in the service.
```

**Do not renormalize the surviving weights.** That is the whole point of the change.

- [ ] **Step 5: Remove the plumbing from `HybridRecommendationService`**

Delete the `deepLearningEnabled` / `dlScoresRaw` / `dlScores` block and both ranking-path constructor injections (`TwoTowerPredictionService` and `DeepLearningPredictionService`) from `HybridRecommendationService`. At the per-candidate scoring call, the `dlScores.getOrDefault(item, 0.0)` argument goes away along with the corresponding parameter. Delete `predictions.put("deepLearningScore", round(candidate.dlScore()));` from `modelPredictions`, and the `dlScore` component from the `ScoredCandidate` record and every construction site. Keep the separate `DeepLearningPredictionService` bean and its injection into `RecommendationController`, which serves the `/predict` endpoints. Follow the compiler: run Step 7 repeatedly and fix what it names.

- [ ] **Step 6: Remove the ranking configuration**

- `.../config/RecommendationProperties.java`: delete the `deepLearningWeight` field and its getter and setter (lines 244, 314-319).
- `src/main/resources/application.yml`: delete the `deep-learning-weight` line.
- Keep the `com.microsoft.onnxruntime` dependency and version property; `DeepLearningPredictionService` still requires them.

- [ ] **Step 7: Compile, then fix the two dl-specific tests**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/services/java-retrieval-service
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn -q -DskipTests compile
```
Repeat until it compiles. Then amend the tests:

- Delete `RecommendationConstantsTest.deepLearningScoreIsClampedIntoRange` entirely — it exists only to test dlScore clamping.
- Rewrite `offlineScoreStaysInRangeWhenWeightsSumAboveOne`. It used a `0.15` deep-learning weight to push the sum to 1.15; with three weights the sum is 1.0, so the "above one" premise is gone. Replace the whole method with:

```java
    // Normalizing by the weight sum keeps a saturated blend at exactly the upper bound.
    @Test
    void offlineScoreStaysInRangeWhenAllComponentsSaturate() {
        double score = RecommendationConstants.blendOfflineScore(
            0.6, 1.0,   // relevance
            0.25, 1.0,  // content
            0.15, 1.0   // popularity
        );
        assertEquals(1.0, score, 1e-9, "all components saturated must yield exactly the upper bound");
    }
```

- In `HybridRecommendationServiceTest`, delete the two lines at 112-113 (`// deep-learning-weight defaults to 0.0...` and the `verify(predictionService, never()).predictBatch(any(), any());`) plus the now-unused `predictionService` mock and its imports if nothing else uses them.
- Update the Task 1 characterization tests to the six-argument call. **The asserted values must not change** — that is the proof the refactor preserved behaviour. In `offlineBlendCharacterizedAtTheProductionDeepLearningWeight`, drop the trailing `0.0, 0.9` arguments and rename it to `offlineBlendCharacterized`. Delete `offlineBlendIgnoresTheDeepLearningScoreEntirelyAtWeightZero` — its job (licensing the removal) is complete and its subject no longer exists; say so in your report.
- In `MovieLensOutcomeScorerTest`, drop the `0.0, // dlScore` argument from the `ScoringInput` construction. **The two golden values must still pass unchanged.** If either moves, stop and report — that means the refactor changed behaviour.

- [ ] **Step 8: Remove the Python config assertion**

In `recsys-pipeline/integration-tests/test_application_config.py`, delete the test containing the `deep-learning-weight` assertion (the block at lines 24-34 reading `config["recsys"]["bandit"]["deep-learning-weight"]`), including its `def test_...` line and its docstring or comment.

- [ ] **Step 9: Run both full suites**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/services/java-retrieval-service
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn test 2>&1 | tail -20
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest integration-tests/ -q 2>&1 | tail -4
```
Expected: the Java suite passes. The Python suite shows the same two pre-existing failures it had before this plan started (`test_readme_documents_every_measurement_environment_variable` and `test_frontend_documentation_uses_relocated_paths`) — both reproduce on `master` and are unrelated. Report both counts and confirm no new failure appeared.

- [ ] **Step 10: Commit**

```bash
git add -A recsys-pipeline/services/java-retrieval-service recsys-pipeline/integration-tests/test_application_config.py
git commit -m "refactor: remove the orphaned two-tower ranking path

Deleting the PyTorch producer left this path with no model to load, and it
was already unreachable because deepLearningWeight defaults to 0.0. The
independent /predict ONNX API remains. The surviving weights are deliberately not
renormalized -- every removed term contributed exactly zero, so leaving them
preserves output exactly. The characterization tests pin that."
```

---

### Task 4: Documentation sweep and final verification

**Files:**
- Modify: `recsys-pipeline/README.md`, `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`, `recsys-pipeline/docs/recommendation_flows/6_Predicting_Scoring.md`

**Interfaces:**
- Consumes: the deletions from Tasks 2 and 3.
- Produces: no code interfaces.

- [ ] **Step 1: Find every stale reference**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
grep -rn -i "movielens_pipeline\|model-reload\|onnx\|deep.learning\|two-tower\|twoTower" README.md docs/ | grep -v node_modules
```
Work from this list. Known sites: `README.md` lines 6, 79, 264-265, 930 and the retrieval-service configuration table; `Data_Pipeline.md`; `6_Predicting_Scoring.md` line 42 and its scoring table.

- [ ] **Step 2: Rewrite each reference**

For every hit, remove the claim or replace it with what is now true. Specifically:
- The pipeline diagram at `README.md:79` shows `run-retrain.sh ──► retrain embeddings + model ──► POST /actuator/model-reload  (hot-swap)`. The hot-swap arrow is gone; the retrain script now only refreshes embeddings.
- `README.md:264-265` lists the Python two-tower step and the model hot-reload step. Both are gone; renumber the surrounding list.
- The scoring table in `6_Predicting_Scoring.md` lists the ranking-time **Two-tower** (`TwoTowerPredictionService`) row. Delete that row and remove `deepLearningWeight × onnxScore` from the `offlineScore` formula shown beneath it. Keep standalone `/predict` documentation for `DeepLearningPredictionService`.
- The retrieval-service configuration table in `README.md` documents `recsys.bandit.deep-learning-weight` / `RECSYS_DEEP_LEARNING_WEIGHT`. Delete that row.
- Where a doc describes `CtrRankingModelTrainingJob` as "offline only", leave it — that remains true.

Do not add new claims about the Spark job being served. It is not.

- [ ] **Step 3: Verify no reference survives**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
grep -rn -i "movielens_pipeline\|deep-learning-weight\|RECSYS_DEEP_LEARNING_WEIGHT\|TwoTowerPredictionService\|ONNX_USER_TOWER_PATH\|ONNX_ITEM_TOWER_PATH\|ONNX_RANKING_PATH" README.md docs/ services/java-retrieval-service/src/main services/java-retrieval-service/pom.xml services/python-modeling | grep -v node_modules
```
Expected: no output. Generic ONNX, `model-reload`, `DeepLearningPredictionService`, and `ModelReloadController` references are expected because the independent `/predict` API survives.

- [ ] **Step 4: Re-run both suites one final time**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline/services/java-retrieval-service
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn test 2>&1 | tail -8
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
python3 -m pytest integration-tests/ -q 2>&1 | tail -4
```
Expected: Java green; Python with only the two known pre-existing failures. Note that `test_readme_documents_every_measurement_environment_variable` reads the README — confirm your edits did not change which measurement variables it finds.

- [ ] **Step 5: Report the net change**

Run `git diff --stat master...HEAD | tail -3` and record the total lines removed. The spec projected roughly 1,900.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/README.md recsys-pipeline/docs
git commit -m "docs: remove the two-tower ranking path and PyTorch pipeline"
```

---

## Self-Review Notes

**Spec coverage.** Producer deletion → Task 2. Orphaned two-tower consumer deletion while preserving the independent `/predict` API → Task 3. Documentation → Task 4. The no-renormalization rule → Task 3 Steps 3-4 and the Global Constraints. Success criterion 1 (characterization) → Task 1, verified again in Task 3 Step 7. Criterion 2 (suites pass with four amendments) → Task 3 Steps 7-9. Criterion 3 (scoped greps clean) → Task 4 Step 3. Criterion 4 (`run-retrain.sh`) → Task 2 Step 4. Criterion 5 (no stale doc references) → Task 4 Step 3.

**Ordering.** Task 1 must precede Tasks 2-3 or there is nothing pinning behaviour. Tasks 2 and 3 are independent of each other. Task 4 requires both.

**Why Task 3 is not split.** Deleting `TwoTowerPredictionService` while `HybridRecommendationService` still injects it leaves the module uncompilable, so its deletion and ranking-plumbing removal are atomic.

**Known imprecision in the spec, already corrected there.** The spec initially claimed both removal sites are inert "because `deepLearningWeight` is 0.0". Only `blendOfflineScore` is. `MovieLensOutcomeScorer` is not gated by that weight and is inert only because `dlScore` is always `0.0` at runtime. Both the Global Constraints and Task 1 Step 2's comment state the distinction.

**Type consistency.** `blendOfflineScore` is six arguments everywhere after Task 3. `ScoringInput` loses exactly one component, `dlScore`. The characterization tests' asserted values are identical before and after; only their call sites change.
