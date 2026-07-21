# Candidate Filters

After candidate generation, candidates pass through the `CandidateFilter` pipeline, which drops seen, blocked, muted, and otherwise ineligible candidates before scoring.

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
