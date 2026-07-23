# Bandit Off-Policy Evaluation (OPE) Harness — Design

## Goal

Let bandit recommendation policies be evaluated offline from the logged `rl_experience`
replay buffer, and fix the Java↔Python replay contract that currently prevents any
Python consumer from reading real serve-time events.

## Problem

Two coupled gaps in the current experiment pipeline (`HybridRecommendationService` +
replay buffer):

1. **Broken contract.** Java writes replay events with field names `user` / `action`
   (`ReplayEvent.USER` / `ReplayEvent.ACTION`), but `replay_export.py` reads
   `e["userId"]` / `e["itemId"]` — a `KeyError` on real events. The exporter only works
   on Python-shaped test fixtures, so the serve→collect→retrain loop is unverified
   against production events.
2. **No real evaluation.** The service exposes in-service vanity metrics and a
   *self-referential* pseudo-regret (oracle = the model's own `estimatedReward`). Replay
   events carry everything needed to compare policies, but nothing consumes them for
   off-policy evaluation.

## Constraints / decisions (locked in brainstorming)

- **Scope:** contract fix **and** an OPE harness.
- **Estimator:** **Direct Method (DM)** on existing logs — no serving/wire change. The
  deterministic top-K logging policy logs only a uniform `1/slateSize` propensity, so
  propensity-weighted estimators (IPS/DR) are not unbiased on today's data; DM avoids the
  hot serving path and ships on current logs.
- **Home:** a standalone Python offline report, mirroring
  `recall_eval_report.py` / `ranking_eval_report.py`. No service change.
- **Reward model:** a freshly-fit **dependency-light pure-numpy logistic regression**
  trained on logged (features → observed reward), with a held-out calibration report
  (AUC + MSE). No sklearn (not a repo dependency; numpy already present via pandas).

## Available data (per replay event, verified)

Top-level (the *taken* action): `user`, `action`, `reward`, `clicked`, `timestamp`,
`banditScore`, `estimatedReward`, `onlineScore`, `coldStart`, `actionPosition`,
`slateSize`, `modelPredictions`, `policy{name, rankingScore, explorationBonus,
propensity}`.

Per-candidate (`actionSpace`, one entry per candidate in the snapshot): `item`,
`modelPredictions`, `coldStart`, `impressions`, `clicks`. **Non-selected candidates do
NOT carry a `banditScore`** — so offline target policies must be definable purely over
`{modelPredictions, coldStart, impressions, clicks}`. This is why UCB-vs-Thompson offline
comparison is out of scope for v1.

## Component 1 — Contract fix

Fix the non-conforming Python side (Java `ReplayEvent` is the schema source of truth); no
serving change.

- **`replay_buffer.py`** (new, shared loader): read `replay:recommendations` from Redis
  (or a `--parquet` dump), JSON-parse each entry, and expose records keyed by the real
  schema names. One place owns the Redis key + schema mapping.
- **`replay_export.py`**: `entries_to_rows` reads `e["user"]` → `userId` column and
  `e["action"]` → `movieId` column (`reward` / `timestamp` names already match). Uses
  `replay_buffer.py` for loading.
- **Tests:** update `replay_export` fixtures from `userId`/`itemId` to the real
  `user`/`action` shape; add a `replay_buffer` load/parse test.

## Component 2 — OPE report (`ope_eval_report.py`, Direct Method)

Standalone `services/python-modeling/ope_eval_report.py` with pure functions +
a CLI, structured like `ranking_eval_report.py`.

### 2a. Reward estimator (fit fresh; honest, not circular)

- **Training set:** one row per replay event = features of the *taken* action. The
  regression **target is the observed `reward`** (continuous in [0,1]); logistic
  cross-entropy handles soft targets. `q(features) ∈ [0,1]` is the estimated reward.
- **Features:** sourced from the taken action's own `actionSpace` candidate entry (matched
  by `item == action`) so training and target-policy scoring read the identical schema —
  `coldStart`, `impressions`, `clicks`, and the numeric entries of `modelPredictions.*`
  (union of keys, missing→0). `actionPosition` is excluded because it is top-level-only
  (absent per-candidate) and would be dead weight at scoring time. Standardized; intercept added.
- **Model:** pure-numpy logistic regression (batch gradient descent, L2, fixed iters/lr)
  in a small `logistic.py` helper — `fit(X, y) → weights`, `predict_proba(X, w)`.
- **Calibration report:** deterministic train/test split (hash of `requestId`, no RNG),
  reporting on the held-out set: **MSE** of `q` vs observed `reward`, and **AUC** of `q`
  vs the binary `clicked` label (rank quality). Printed prominently so DM values are read
  alongside their estimator quality. AUC is `None`/skipped if the held-out set is single-class.

### 2b. Policies compared

Policy value = mean over events of `q(features of the action the policy selects from that
event's `actionSpace`)`, except the logging baseline which uses observed reward directly.

| Policy | Selection rule | Value |
|--------|----------------|-------|
| `logging` | the actually-taken `action` | **on-policy** mean observed `reward` (anchor) |
| `popularity` | argmax candidate `impressions` | DM |
| `ctr` | argmax candidate `clicks / max(impressions, 1)` | DM |
| `model:<key>` | argmax candidate `modelPredictions[<key>]` (one row per key present) | DM |
| `random` | uniform over candidates (deterministic: seeded by `requestId` hash) | DM |

### 2c. Output

Printed table + CSV (`--output`), one row per policy:
`policy | value | lift_vs_logging | n_events | estimator_auc | estimator_mse`.
`lift_vs_logging = value / logging_value - 1`. Shares the CSV-writing style of the
existing reports.

## CLI

```
REDIS_HOST=localhost python services/python-modeling/ope_eval_report.py \
    [--key replay:recommendations] [--parquet <dump>] [--output <csv>] [--limit N]
```

Reads the same buffer as `replay_export.py`. Exits with a clear message when the buffer is
empty or has no feedback-completed events (no `reward`).

## Boundaries / non-goals

- No serving/wire change; no IPS/DR (deferred — would need stochastic logging).
- No UCB-vs-Thompson offline comparison (needs per-candidate bandit scores not logged).
- No dashboard integration in v1 (standalone report only; a later section can embed it).
- No new runtime dependency (numpy only).

## Testing

- `replay_buffer` load/parse (real `user`/`action` schema).
- `replay_export` rows use `user`→userId, `action`→movieId (updated fixtures).
- `logistic` fit: separable toy data → AUC 1.0; monotonic loss decrease.
- OPE: synthetic replay events where a known signal drives reward → that policy ranks top
  and `logging` value equals mean observed reward; empty-buffer and no-reward guards.
- Focused Python suite stays green.

## Verification

End-to-end: run the movie-category sim to populate a replay buffer (or feed synthetic
events), run `ope_eval_report.py`, confirm the table ranks policies with a plausible
calibration report and `logging` anchored to observed reward.
