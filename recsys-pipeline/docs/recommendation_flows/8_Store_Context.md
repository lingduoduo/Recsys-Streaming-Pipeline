# Store Context

**Flow:** [Previous](7_Shuffling.md) · **Current: Store context** · [Next](9_Track_Metrics.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

> **Where this runs:** this stage executes in the retrieval service, now in
> [lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service) — see
> the [repository boundary](../../../README.md#repository-boundary). The Redis key contract below is
> owned by this repository and is authoritative. The class and bean names are as of the split and
> may have been renamed there; nothing in either repository detects that.

For the complete local startup sequence, follow the [canonical finite local workflow](../../README.md#canonical-finite-local-workflow).

After scoring and final selection, the retrieval service writes pending recommendation context for
each served user-item pair, so a later `/feedback` call can join it into a labeled replay example.

The context is stored at `replay:pending:{user}:{item}`; the `/feedback` write path reads it back and pushes the rewarded event to the `replay:recommendations` buffer (see the feedback and Replay Buffer Export sections in the main README).

## Required state

A successful `GET /api/v1/retrieval/recommend/{user}` with at least one selected item writes:

- `replay:pending:{user}:{item}` strings containing request, state, candidate snapshot, selected
  action, policy, prediction, and timestamp context, with the configured pending TTL;
- `recommendation:request:{requestId}` plus `user:{id}:served_history` and
  `user:{id}:impressions` hashes; and
- `bandit:item:{item}:impressions`, `bandit:last_served:{item}`, and the exposed-item set used by
  metrics.

`POST /api/v1/retrieval/feedback` depends on the matching pending key to reconstruct the full labeled event, then
deletes it and appends the reward to `replay:recommendations`. If the pending key is absent or has
expired, feedback still writes a minimal replay event and reward/click aggregates, but the original
state, candidate slate, propensity, and model predictions are unavailable; a tabular Q update also
cannot be derived without the stored state.

### Configuration

**Replay buffer property**

| Property | Default |
|---|---|
| `recsys.replay-buffer.max-size` | `10000` |
| `recsys.replay-buffer.candidate-snapshot-size` | `20` |

**Runtime override**

| Env var | Default |
|---|---|
| `RECSYS_REPLAY_BUFFER_MAX_SIZE` | `10000` |
| `RECSYS_REPLAY_CANDIDATE_SNAPSHOT_SIZE` | `20` |
| `RECSYS_REPLAY_PENDING_TTL` | `1h` |
