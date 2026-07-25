# Randomization

**Flow:** [Previous](6_Predicting_Scoring.md) · **Current: Shuffling** · [Next](8_Store_Context.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

For the complete local startup sequence, follow the [root quick start](../../../README.md#recsys-pipeline).

This stage documents the intended post-score randomization control and the policy-level randomness
that affects final ordering. The current selector behavior is described explicitly below.

## Required state

`recsys.candidate-generation.top-n-randomization-pool`
(`RECSYS_RANDOMIZATION_POOL`, default `5`) is declared in `application.yml`. In the current serving
implementation, however, `TopKScoreSelector` sorts by final score and does not read this property,
so there is no separate post-score shuffle. With identical inputs, UCB selection is deterministic.
Thompson sampling is non-deterministic because it draws from each arm's posterior, and Q-learning
or SARSA can vary when epsilon-greedy exploration is non-zero.

If the shuffle setting is absent, Spring uses the default value, but current serving order is
unchanged because the selector does not consume it. If the Redis counters/reward state that feeds a
stochastic policy is absent, those policies start from their configured priors rather than from
learned ordering.

### Configuration

**Candidate generation property**

| Property | Default |
|---|---|
| `recsys.candidate-generation.top-n-randomization-pool` | `5` |

**Runtime override**

| Env var | Default |
|---|---|
| `RECSYS_RANDOMIZATION_POOL` | `5` |
