# Candidate Filters

**Flow:** [Previous](3_Cold_Start.md) · **Current: Filtering** · [Next](5_Candidate_Hydration.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

For the complete local startup sequence, follow the [root quick start](../../../README.md#recsys-pipeline).

After candidate generation, candidates pass through the `CandidateFilter` pipeline, which drops seen, blocked, muted, and otherwise ineligible candidates before scoring.

## Required state

- Filter switches and muted-value lists come from `recsys.filtering.*`
  (`RECSYS_FILTERING_ENABLED`, `RECSYS_MUTED_PRODUCT_TYPES`, `RECSYS_MUTED_GENRES`, and
  `RECSYS_MUTED_KEYWORDS`), and item eligibility fields come from the configured catalog.
- Query hydration supplies user history and preferences from keys such as `user:{id}:recent`,
  `user:{id}:rated`, `user:{id}:impressions`, `user:{id}:served_history`, and
  `user:{id}:features`. Filters that use creator relationships or preferences likewise depend on
  their hydrated block, mute, follow, genre, and age fields being present.

When configuration lists are absent, they default to empty and remove nothing on that dimension;
when hydrated user state is absent, the corresponding history/relationship filter has no IDs to
exclude. Filtering still runs, but missing state can allow previously seen or otherwise
user-specific ineligible items through. Setting `recsys.filtering.enabled=false` disables the
configured muted-value checks; history filters still run in the current retrieval pipeline.

| Filter | Removes |
|---|---|
| `PreviouslySeenMoviesFilter` | Movies the user has already watched (via bloom filter) |
| `PreviouslySeenMoviesBackupFilter` | Watched movies using `impressedMovieIds`; used when bloom filter is unavailable |
| `PreviouslyServedMoviesFilter` | Recently served movies (`servedMovieIds`) |
| `SelfMovieFilter` | Movies created by the requesting user (`userId == ownerId`) |
| `CreatorBlocklistFilter` | Movies from blocked creators |
| `MutedKeywordFilter` | Movies whose title or tags match muted keywords |
| `AgeFilter` | Movies outside the user's age-appropriate range |
| `VideoFilter` | Non-video content (configurable via filter settings) |
| `ReshareDeduplicationFilter` | Duplicate reshares of the same source movie |
| `GenreIdsFilter` | Candidates not matching the requested genre IDs |
| `NewUserGenreFilter` | Candidates outside the genre allowlist for new users |

### Configuration

| Property | Default |
|---|---|
| `recsys.filtering.enabled` | `true` |
| `recsys.filtering.blocked-users` | *(empty)* |
| `recsys.filtering.muted-product-types` | *(empty)* |
| `recsys.filtering.muted-genres` | *(empty)* |
| `recsys.filtering.muted-keywords` | *(empty)* |
