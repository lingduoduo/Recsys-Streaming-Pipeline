# Offline MovieLens MDP Evaluation — Design

## Goal

Turn the sampled MovieLens Markov Decision Process code appended to `RecommendationProperties.java` into a compact, tested offline Java policy-evaluation CLI that fits the existing repository without changing live recommendation behavior.

## Current Problem

The sample currently adds imports and a second public top-level class after `RecommendationProperties` closes, so the Java source cannot compile. It also places executable data loading, simulation, policy, statistics, and console code inside a Spring configuration file.

The sample is conceptually separate from the live Redis-backed bandit implementation. Its states use numeric MovieLens users and shrinking candidate sets, its rewards derive from explicit ratings, and its episodes end after a finite slate. The live service uses string identifiers, observed feedback rewards, replay events, taste-profile state keys, and Redis Q-values. The offline evaluator must therefore remain isolated rather than sharing types whose semantics differ.

## Scope

The implementation will:

- restore `RecommendationProperties.java` to configuration only;
- add an offline evaluation package to the Java retrieval module;
- compare uniform-random and greedy leave-one-user-out movie-score policies;
- measure finite-horizon discounted return over deterministic seeded episodes;
- report mean return, mean steps, standard error, and reproducible 95% bootstrap intervals in console and CSV form;
- validate inputs and cover the MDP components with focused tests.

It will not modify serving, Spring configuration, Redis state, replay schemas, online reward learning, Q-learning/SARSA behavior, or HTTP endpoints.

## Package Structure

Create `com.demo.retrieval.evaluation` with four focused units:

### `MovieLensDataset`

Loads `ratings.csv`, filters users and movies by minimum counts, and exposes immutable retained ratings and movie aggregates. It computes a smoothed movie score while excluding the evaluated user's own rating to reduce target leakage.

Movie titles and genres are not loaded because they do not affect policy value or the structured report.

### `FiniteHorizonEnvironment`

Owns the offline MDP semantics:

- **State:** user identifier, immutable available-action list, and step number.
- **Actions:** retained movie identifiers in the current candidate pool.
- **Transition:** remove the chosen movie and increment the step number.
- **Reward:** retained explicit rating minus `3.0`; missing ratings receive configurable `unratedReward` and remain explicitly documented as unknown feedback rather than observed dislike.
- **Discount:** supplied to rollout evaluation and validated in `[0, 1]`.

Initial candidate pools combine a seeded sample of the user's rated movies with seeded unseen popular movies, capped at `candidatePoolSize`.

### `EvaluationPolicy`

A small interface selects one action from an environment state using a supplied `Random` instance. Two implementations ship:

- uniform policy samples directly from the available actions;
- greedy policy selects the highest leave-one-user-out smoothed movie score, with stable movie-ID tie-breaking.

Policies return one action rather than allocating complete action-probability maps.

### `MovieLensPolicyEvaluation`

The CLI validates arguments, runs both policies using reproducible seeds, computes aggregate statistics, prints a compact comparison, and optionally writes CSV. Point estimates remain full-dataset episode means.

## CLI

Required input:

- `--ratings <path>`

Optional flags with defaults:

- `--episodes 5000`
- `--candidate-pool-size 100`
- `--slate-size 10`
- `--min-user-ratings 20`
- `--min-movie-ratings 10`
- `--unrated-reward -1.0`
- `--discount 0.99`
- `--seed 42`
- `--bootstrap-samples 1000`
- `--output <csv>`

Invalid numeric ranges, missing inputs, unreadable files, and an empty dataset after filtering fail with concise actionable messages and a nonzero exit.

## Evaluation and Statistics

Each policy is evaluated over the configured number of episodes. A deterministic episode seed schedule ensures reproducibility and supports paired policy comparisons without coupling the policies' internal random draws.

Each result contains:

- policy name;
- episode count;
- mean discounted return;
- mean steps;
- standard error of episode returns;
- percentile-bootstrap 95% lower and upper bounds.

Bootstrap resamples complete episode returns with replacement using a deterministic seed derived from the CLI seed. Intervals quantify episode-sampling uncertainty for the fixed policy and environment configuration; they do not represent uncertainty in the MovieLens dataset itself.

## Input Validation

The ratings loader accepts the repository's `userId,movieId,rating,timestamp` shape, ignores the timestamp after validating the required first three columns, and reports malformed records with file and line number. It rejects non-finite ratings and duplicate `(userId, movieId)` rows rather than silently replacing them.

Filtering runs to a fixed point because removing sparse users can make movies sparse and vice versa. The loader rejects a result with no eligible users or movies.

The environment rejects unavailable actions and invalid configuration such as nonpositive episode, pool, slate, or bootstrap counts; negative minimum-count thresholds; and discounts outside `[0, 1]`.

## Output

Console output is a compact table. CSV columns are:

`policy,episodes,mean_return,mean_steps,standard_error,ci95_low,ci95_high`

No per-recommendation narratives, user leaderboards, or hard-coded discount sweeps are retained.

## Testing

Focused unit tests will verify:

- valid CSV loading and iterative user/movie filtering;
- malformed rows, duplicate ratings, and empty filtered data errors;
- leave-one-user-out smoothed movie scores;
- seeded candidate-pool construction and size bounds;
- state, action, transition, reward, termination, and discount semantics;
- uniform and greedy policy behavior, including stable tie-breaking;
- deterministic rollout and evaluation results;
- reproducible bootstrap intervals and one-episode behavior;
- CLI validation, console output, and CSV shape.

The full Java retrieval-service suite must remain green.

## Acceptance Criteria

The sampled block is removed from `RecommendationProperties.java`; the module compiles; the new offline CLI produces deterministic random-versus-greedy results and optional CSV from MovieLens ratings; every stated validation and MDP semantic has focused coverage; and no live serving behavior or configuration changes.
