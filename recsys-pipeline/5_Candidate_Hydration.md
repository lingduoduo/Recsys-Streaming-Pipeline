# Candidate Hydrators

After filtering (see [4_Filtering.md](4_Filtering.md)), `CandidateHydrator` implementations enrich the surviving candidates with additional signals:

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
