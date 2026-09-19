# Track Metrics

**Flow:** [Previous](8_Store_Context.md) · **Current: Track metrics**

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

> **Where this runs:** this stage executes in the retrieval service, now in
> [lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service) — see
> the [repository boundary](../../../README.md#repository-boundary). The Redis key contract below is
> owned by this repository and is authoritative. The class and bean names are as of the split and
> may have been renamed there; nothing in either repository detects that.

For the complete local startup sequence, follow the
[canonical finite local workflow](../../README.md#canonical-finite-local-workflow).

Recommendation serving records the request count, item impressions, estimated reward,
regret-style metrics, novelty, and catalog coverage. A later `POST /api/v1/retrieval/feedback` call for that
request records clicks and the observed reward. Aggregates are exposed through the `GET /api/v1/retrieval/metrics`
endpoint and stored under `bandit:metrics*` Redis keys.

## Required state

- Successful recommendation calls populate `bandit:metrics`,
  `bandit:metrics:{algorithm}`, `bandit:item:{item}:impressions`,
  `bandit:last_served:{item}`, `bandit:exposed_items`, and the pending/request context keys
  described in stage 8.
- `POST /api/v1/retrieval/feedback` populates click and observed-reward fields in the global and per-algorithm
  metric hashes, item click counters, `reward-model:*` hashes, and
  `replay:recommendations`. For Q-learning and SARSA, feedback with a valid pending state also
  updates the matching Q-table and Q-update metrics.

A fresh Redis has none of these request, feedback, or replay writes. Numeric aggregates therefore
return zero, catalog coverage is zero, inactive algorithms are omitted from `allAlgorithms`, and
the replay list is empty. Send `GET /api/v1/retrieval/recommend/{user}` first to create impressions and pending
context, then send matching `POST /api/v1/retrieval/feedback` calls to create clicks, rewards, and labeled replay
events.

## `GET /api/v1/retrieval/metrics`

Returns aggregate online metrics for the active algorithm and a per-algorithm comparison view.

```bash
curl http://localhost:8080/api/v1/retrieval/metrics
```

| Field | Description |
|---|---|
| `requests` | Total recommendation requests served |
| `recommendationsServed` | Total items recommended |
| `clicks` | Total clicks recorded via `/feedback` |
| `ctr` | Click-through rate |
| `avgObservedReward` | Mean reward from `/feedback` calls |
| `avgEstimatedReward` | Mean predicted reward at serve time |
| `avgPseudoRegret` | Mean per-request regret estimate |
| `cumulativePseudoRegret` | Running sum of pseudo-regret |
| `avgNoveltyScore` | Mean novelty of served items |
| `coldStartImpressions` | Impressions on cold-start items |
| `exploratoryImpressions` | Impressions driven by exploration bonus |
| `catalogCoverage` | Fraction of catalog served at least once |
| `allAlgorithms.ucb` | Per-metric breakdown for UCB |
| `allAlgorithms.thompson` | Per-metric breakdown for Thompson Sampling |
| `global` | Aggregate across all algorithms |

Redis keys:

| Key | Scope |
|---|---|
| `bandit:metrics` | All traffic combined |
| `bandit:metrics:ucb` | UCB only |
| `bandit:metrics:thompson` | Thompson Sampling only |
| `bandit:metrics:q-learning` | Q-learning only |
| `bandit:metrics:sarsa` | SARSA only |
| `replay:recommendations` | Rewarded replay events populated by `POST /api/v1/retrieval/feedback` |

## `measurements` (schema 2.1)

`GET /api/v1/retrieval/metrics` nests a `measurements` object beside the bandit aggregates.

| Section | Contents |
|---|---|
| `latency` | p50/p95/p99, count, error rate, and timeout rate per endpoint (`recommend`, `feedback`, `predict`, `embedding`, `profile`) and per stage |
| `throughput` | `qps`, `windowRequests`, `windowSeconds`, and `observedSeconds` per endpoint |
| `cache` | `hitCount`, `missCount`, `hitRate`, `evictionCount`, and `estimatedSize` for `item_vectors` and `reward_stats` |
| `freshness` | Fresh-item exposure share |
| `safety` | Candidate-filter decisions by reason |
| `feedbackCoverage` | Presence rate of each optional feedback signal |

Percentiles decay over `RECSYS_PERCENTILE_WINDOW_SECONDS` (default 300), so they
describe recent traffic. Counts, error rate, and timeout rate stay cumulative for
the life of the process. The throughput window is `RECSYS_THROUGHPUT_WINDOW_SECONDS`
(default 60).

`qps` divides by `observedSeconds` — the time actually observed, capped at the
window — rather than the full window width, so a short burst is not understated.
An endpoint that has served nothing reports `qps: null`, not zero.
