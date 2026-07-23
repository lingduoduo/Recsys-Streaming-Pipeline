# Offline MovieLens MDP Evaluation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the sampled evaluator appended to `RecommendationProperties.java` with a compact, deterministic, tested offline MovieLens MDP policy-evaluation CLI.

**Architecture:** Restore Spring configuration to configuration-only code, then add an immutable dataset, finite-horizon environment, action-selecting policies, and CLI/report statistics under `com.demo.retrieval.evaluation`. The evaluator stays independent of Spring, Redis, serving types, and live RL semantics.

**Tech Stack:** Java 17, Maven, JUnit 5, standard Java file/CSV APIs, no new dependencies.

## Global Constraints

- Do not change serving, Spring configuration shape, Redis state, replay schemas, online learning, Q-learning/SARSA behavior, or HTTP endpoints.
- Required CLI input: `--ratings <path>`.
- Defaults: episodes `5000`, candidate pool `100`, slate `10`, minimum user ratings `20`, minimum movie ratings `10`, unrated reward `-1.0`, discount `0.99`, seed `42`, bootstrap samples `1000`.
- CSV columns: `policy,episodes,mean_return,mean_steps,standard_error,ci95_low,ci95_high`.
- Bootstrap intervals quantify episode-sampling uncertainty for a fixed policy and environment, not MovieLens dataset uncertainty.
- Missing ratings are unknown feedback; their simulated reward is configurable.
- Add no dependency.

---

## Files

- Restore: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java`
- Create: `.../src/main/java/com/demo/retrieval/evaluation/MovieLensDataset.java`
- Create: `.../src/main/java/com/demo/retrieval/evaluation/FiniteHorizonEnvironment.java`
- Create: `.../src/main/java/com/demo/retrieval/evaluation/EvaluationPolicy.java`
- Create: `.../src/main/java/com/demo/retrieval/evaluation/MovieLensPolicyEvaluation.java`
- Create matching tests under `.../src/test/java/com/demo/retrieval/evaluation/`.

### Task 1: Restore Configuration and Add the Dataset

**Interfaces:**
- `MovieLensDataset.load(Path,int,int)`
- `userIds()`, `movieIds()`, `ratingsFor(int)`, `rating(int,int)`, `movieCounts()`, `globalMean()`, `scoreExcludingUser(int,int)`.

- [ ] **Step 1: Capture the broken baseline**

Run from `recsys-pipeline/services/java-retrieval-service`:

```bash
mvn -q -DskipTests compile
```

Expected: compilation fails because the sample adds imports and a second public type after `RecommendationProperties`.

- [ ] **Step 2: Write failing dataset tests**

Create `MovieLensDatasetTest.java` with `@TempDir` CSV fixtures. Verify valid loading, sorted immutable IDs, iterative user/movie filtering, and leave-one-user-out score:

```java
MovieLensDataset data = MovieLensDataset.load(csv, 2, 2);
assertEquals(List.of(1, 2), data.userIds());
assertEquals((1.0 + 20.0 * data.globalMean()) / 21.0,
    data.scoreExcludingUser(1, 10), 1e-12);
```

Add exact validation tests for wrong header, short row, nonnumeric and nonfinite rating, duplicate `(user,movie)`, negative thresholds, and empty post-filter data. Malformed-row messages must contain file and line number.

- [ ] **Step 3: Verify RED**

```bash
mvn -q -Dtest=MovieLensDatasetTest test
```

Expected: test compilation fails because `MovieLensDataset` is absent and the appended sample still breaks compilation.

- [ ] **Step 4: Restore and implement**

Delete everything after the original closing brace of `RecommendationProperties`. Implement an immutable final dataset class. Accept header prefix `userId,movieId,rating`, parse with `split(",", -1)`, reject duplicate/nonfinite data, and filter users/movies repeatedly until stable. Use prior strength `20.0` for leave-one-user-out smoothing.

- [ ] **Step 5: Verify GREEN and commit**

```bash
mvn -q -Dtest=MovieLensDatasetTest test
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/evaluation/MovieLensDataset.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/evaluation/MovieLensDatasetTest.java
git commit -m "feat(evaluation): add immutable MovieLens dataset"
```

Expected: all dataset tests pass.

### Task 2: Add the Finite-Horizon Environment and Policies

**Interfaces:**
- `State(int userId,List<Integer> availableActions,int step)`
- `Step(State nextState,double reward,boolean done)`
- `Rollout(double discountedReturn,int steps)`
- `initialState(int,Random)`, `step(State,int)`, `rollout(int,EvaluationPolicy,Random,double)`
- `EvaluationPolicy.select(State,Random)`, `uniform()`, `greedy(MovieLensDataset)`.

- [ ] **Step 1: Write failing environment tests**

Assert seeded candidate pools are reproducible and bounded, transitions remove exactly one action and increment the step, unavailable actions fail, rated reward is `rating - 3.0`, unrated reward is configurable, and termination occurs at slate size or exhaustion. Use a two-step fixture proving rewards `2` then `1` with discount `0.5` return `2.5`.

- [ ] **Step 2: Write failing policy tests**

Assert uniform selection is seeded and always available. Assert greedy selects highest leave-one-user-out score and lower movie ID on ties. Both policies reject an empty action list.

- [ ] **Step 3: Verify RED**

```bash
mvn -q -Dtest=FiniteHorizonEnvironmentTest,EvaluationPolicyTest test
```

Expected: test compilation fails because the environment and policy are absent.

- [ ] **Step 4: Implement minimal MDP and policies**

Copy state action lists with `List.copyOf`. Build candidates from a seeded rated subset plus seeded unseen movies ordered by descending retained count then ascending ID. Validate positive pool/slate sizes and discount in `[0,1]`. Uniform uses `random.nextInt`; greedy compares score descending then ID ascending. Rollout accumulates `discountPower * reward`.

- [ ] **Step 5: Verify GREEN and commit**

```bash
mvn -q -Dtest=FiniteHorizonEnvironmentTest,EvaluationPolicyTest test
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/evaluation/FiniteHorizonEnvironment.java recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/evaluation/EvaluationPolicy.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/evaluation/FiniteHorizonEnvironmentTest.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/evaluation/EvaluationPolicyTest.java
git commit -m "feat(evaluation): add finite-horizon MovieLens MDP"
```

### Task 3: Add Statistics and CLI Reporting

**Interfaces:**
- `Options.parse(String[])`
- `Result(policy,episodes,meanReturn,meanSteps,standardError,ci95Low,ci95High)`
- `evaluate(...)`, `bootstrapBounds(...)`, `run(String[],PrintStream,PrintStream)`, `main(String[])`.

- [ ] **Step 1: Write failing statistics tests**

Verify repeated evaluation is deterministic, one episode has standard error `0.0`, bootstrap bounds are deterministic, and results remain ordered `uniform`, then `greedy`.

- [ ] **Step 2: Write failing CLI tests**

Cover required `--ratings`, every numeric range, discount bounds, duplicate/unknown flags, unreadable files, the fixed-dataset disclaimer, and exact CSV header:

```text
policy,episodes,mean_return,mean_steps,standard_error,ci95_low,ci95_high
```

Assert `run` returns `2` with `error: <message>` for invalid input and `0` for a valid temporary dataset.

- [ ] **Step 3: Verify RED**

```bash
mvn -q -Dtest=MovieLensPolicyEvaluationTest test
```

Expected: test compilation fails because the CLI is absent.

- [ ] **Step 4: Implement evaluator and output**

Use a manual `--key value` parser with exact defaults. Derive stable environment/policy episode seeds from base seed, episode index, and policy name. Store raw returns; standard error is sample standard deviation divided by `sqrt(n)`. Bootstrap complete episode returns with replacement using a deterministic derived seed and percentile endpoints `2.5%`/`97.5%`. Print the uncertainty disclaimer and write CSV only for `--output`. `main` exits with `run`'s status.

- [ ] **Step 5: Verify focused suite and commit**

```bash
mvn -q -Dtest=MovieLensDatasetTest,FiniteHorizonEnvironmentTest,EvaluationPolicyTest,MovieLensPolicyEvaluationTest test
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/evaluation/MovieLensPolicyEvaluation.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/evaluation/MovieLensPolicyEvaluationTest.java
git commit -m "feat(evaluation): add offline MovieLens policy report"
```

### Task 4: Full Verification and Smoke Test

- [ ] **Step 1: Run the entire retrieval suite**

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn -q test
git diff --check
```

Expected: zero test failures and no whitespace errors.

- [ ] **Step 2: Smoke-test repository data**

```bash
mvn -q -DskipTests package
java -cp target/classes com.demo.retrieval.evaluation.MovieLensPolicyEvaluation --ratings ../../sampledata/ratings.csv --episodes 20 --min-user-ratings 1 --min-movie-ratings 1 --bootstrap-samples 20
```

Expected: exit zero, two result rows, and the fixed-dataset bootstrap disclaimer.

- [ ] **Step 3: Verify scope**

```bash
rg -n "MovieLensPolicyEvaluation" src/main/java/com/demo/retrieval/config/RecommendationProperties.java
git diff --stat HEAD~3..HEAD
git status --short
```

Expected: the search is empty; only intended evaluator/config/test files and pre-existing planning artifacts are present.

- [ ] **Step 4: Record evidence**

Report exact Maven test count and duration, smoke-test output status, CLI defaults, three implementation commit hashes, and the fixed-dataset bootstrap limitation.

