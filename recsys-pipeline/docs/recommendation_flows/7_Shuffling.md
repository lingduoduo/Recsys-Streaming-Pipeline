# Randomization

After scoring, the retrieval service shuffles the top scoring pool slightly to avoid deterministic repetition across requests. The size of the pool that gets shuffled is controlled by the top-N randomization pool.

### Configuration

**Candidate generation property**

| Property | Default |
|---|---|
| `recsys.candidate-generation.top-n-randomization-pool` | `5` |

**Runtime override**

| Env var | Default |
|---|---|
| `RECSYS_RANDOMIZATION_POOL` | `5` |
