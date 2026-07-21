# Track Metrics

The final step of each request records impressions, clicks, regret-style metrics, novelty, and catalog coverage. Aggregates are exposed through the `GET /metrics` endpoint and stored under `bandit:metrics*` Redis keys.

## `GET /metrics`

Returns aggregate online metrics for the active algorithm and a per-algorithm comparison view.

```bash
curl http://localhost:8080/metrics
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
