# Store Context

After scoring and randomization, the retrieval service writes the pending recommendation context for each served user-item pair to the replay buffer, so a later `/feedback` call can join it into a labeled training example.

The context is stored at `replay:pending:{user}:{item}`; the `/feedback` write path reads it back and pushes the rewarded event to the `replay:recommendations` buffer (see the feedback and Replay Buffer Export sections in the main README).

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
