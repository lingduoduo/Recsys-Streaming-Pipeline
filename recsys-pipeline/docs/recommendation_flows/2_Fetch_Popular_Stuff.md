# Fetch Popular Candidates

**Flow:** [Previous](1_Query_Hydration.md) · **Current: Fetch popular candidates** · [Next](3_Cold_Start.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

For the complete local startup sequence, follow the [root quick start](../../../README.md#recsys-pipeline).

After query hydration, the retrieval service seeds the candidate pool by pulling the top items from `global:item_popularity`.

`global:item_popularity` is a Redis sorted set of global click counts, kept fresh by `UserEventStreamingJob` (one `ZINCRBY` per unique clicked item each micro-batch). The number of popular candidates fetched is controlled by the popularity-fetch multiplier.

## Required state

`UserEventStreamingJob` writes click counts to the Redis sorted set `global:item_popularity`.
The retrieval service reads up to
`max(limit × recsys.candidate-generation.popularity-fetch-multiplier, limit)` members in descending
score order. If the key is absent or the sorted set is empty, this stage contributes no popularity
candidates; catalog-backed cold-start candidates may still be added by the next stage.

### Configuration

**Candidate generation property**

| Property | Default |
|---|---|
| `recsys.candidate-generation.popularity-fetch-multiplier` | `5` |

**Runtime override**

| Env var | Default |
|---|---|
| `RECSYS_POPULARITY_FETCH_MULTIPLIER` | `5` |
