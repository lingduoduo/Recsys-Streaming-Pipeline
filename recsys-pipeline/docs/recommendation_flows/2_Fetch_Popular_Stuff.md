# Fetch Popular Candidates

After query hydration, the retrieval service seeds the candidate pool by pulling the top items from `global:item_popularity`.

`global:item_popularity` is a Redis sorted set of global click counts, kept fresh by `UserEventStreamingJob` (one `ZINCRBY` per unique clicked item each micro-batch). The number of popular candidates fetched is controlled by the popularity-fetch multiplier.

### Configuration

**Candidate generation property**

| Property | Default |
|---|---|
| `recsys.candidate-generation.popularity-fetch-multiplier` | `5` |

**Runtime override**

| Env var | Default |
|---|---|
| `RECSYS_POPULARITY_FETCH_MULTIPLIER` | `5` |
