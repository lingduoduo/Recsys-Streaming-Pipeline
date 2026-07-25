# Candidate Hydrators

**Flow:** [Previous](4_Filtering.md) · **Current: Candidate hydration** · [Next](6_Predicting_Scoring.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

For the complete local startup sequence, follow the [root quick start](../../../README.md#recsys-pipeline).

After filtering (see [4_Filtering.md](4_Filtering.md)), `CandidateHydrator` implementations enrich the surviving candidates with additional signals:

## Required state

- Candidate content features come from the configured catalog and, for data-pipeline enrichment,
  the `movie:{id}:features` hashes written by `MovieLensContextCollectorStreamingJob`.
- Candidate vector signals use Redis item embedding keys
  `{recsys.embeddings.item-prefix}:{item}` (default `i2vEmb:{item}`), written by the offline
  Item2Vec job. User-vector scoring can additionally use
  `{recsys.embeddings.user-prefix}:{user}` (default `uEmb:{user}`).
- Social, visibility, engagement, and safety hydrators require their corresponding item feature
  hashes or upstream service responses.

When an item feature hash is absent, that enrichment is empty/default rather than fabricated. When
an item embedding is absent, the service can still rank from catalog, popularity, reward, and
bandit signals, but the embedding relevance contribution for that item is zero. An absent user
embedding falls back to an average of available watched/rated item embeddings; if those are also
missing, embedding relevance is zero.

| Hydrator | Adds |
|---|---|
| `CoreDataCandidateHydrator` | Title, genres, and release year from the movie feature store |
| `InNetworkCandidateHydrator` | In-network flag — whether the candidate is from a followed creator |
| `MutualFollowJaccardCandidateHydrator` | Jaccard similarity score via MinHash |
| `EngagementCountsCandidateHydrator` | Global rating count and average rating |
| `GenreMatchCandidateHydrator` | Genre overlap signal against user preferences |
| `SubscriptionCandidateHydrator` | Subscription-gated content flag |
| `LanguageCodeCandidateHydrator` | Language code |
| `HasMediaCandidateHydrator` | Media type flags |
| `BlockedByCandidateHydrator` | Blocked-by flag — whether the viewer is blocked by the candidate's creator |
| `VisibilityFilteringCandidateHydrator` | Visibility eligibility flag based on content policy |
| `FollowingRepliedUsersCandidateHydrator` | Social proximity signal from followed or replied-to creators |
| `QuoteCandidateHydrator` | Quote and reference metadata |
| `GizmoduckCandidateHydrator` | External content safety signal |
