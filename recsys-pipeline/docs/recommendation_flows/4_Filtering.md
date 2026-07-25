# Candidate Filters

**Flow:** [Previous](3_Cold_Start.md) · **Current: Filtering** · [Next](5_Candidate_Hydration.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

For the complete local startup sequence, follow the [root quick start](../../../README.md#recsys-pipeline).

After candidate generation, `ContentCandidateRetriever` removes known-history items and applies
catalog-based expiry and muted-value checks before scoring.

## Required state

- Filter switches and muted-value lists come from `recsys.filtering.*`
  (`RECSYS_FILTERING_ENABLED`, `RECSYS_MUTED_PRODUCT_TYPES`, `RECSYS_MUTED_GENRES`, and
  `RECSYS_MUTED_KEYWORDS`), and item eligibility fields come from the configured catalog.
- Query hydration supplies watched/rated history, recently rated/action/cached IDs, impressed IDs,
  and served IDs from the stage 1 keys. `ContentCandidateRetriever` also passes its combined
  excluded-item set through the final eligibility check.

When configuration lists are absent, they default to empty and remove nothing on that dimension;
when hydrated history is absent, the corresponding history filter has no IDs to exclude. A
candidate with no catalog profile bypasses catalog metadata checks, while a profile with an expired
`expiresAtEpochMillis` is always removed. Setting `recsys.filtering.enabled=false` disables the
muted product-type, genre, and keyword checks; history and expiry filtering still run.

| Implemented check | Removes |
|---|---|
| `PreviouslySeenMoviesFilter` | Watched, rated, recently rated, action-sequence, and cached movie IDs |
| `PreviouslySeenMoviesBackupFilter` | Movies in `impressedMovieIds` |
| `PreviouslyServedMoviesFilter` | Recently served movies (`servedMovieIds`) |
| Catalog expiry check | Profiles whose positive `expiresAtEpochMillis` is in the past |
| Configured muted-value checks | Profiles matching a muted product type, genre, tag/keyword, or title substring |

Creator block/mute/follow, age, self-item, media, reshare, requested-genre, visibility, and safety
filters are not implemented in the current Java serving path. Adding them would require new query
state, catalog fields or upstream clients, and `CandidateFilter` wiring.

### Configuration

| Property | Default |
|---|---|
| `recsys.filtering.enabled` | `true` |
| `recsys.filtering.muted-product-types` | *(empty)* |
| `recsys.filtering.muted-genres` | *(empty)* |
| `recsys.filtering.muted-keywords` | *(empty)* |
