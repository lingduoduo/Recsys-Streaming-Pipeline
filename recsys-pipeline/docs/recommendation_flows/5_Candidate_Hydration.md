# Candidate Hydration

**Flow:** [Previous](4_Filtering.md) · **Current: Candidate hydration** · [Next](6_Predicting_Scoring.md)

**References:** [API](../recommendation_architecture/API.md) · [Data pipeline](../recommendation_architecture/Data_Pipeline.md)

> **Where this runs:** this stage executes in the retrieval service, now in
> [lingduoduo/Recsys-Backend-Service](https://github.com/lingduoduo/Recsys-Backend-Service) — see
> the [repository boundary](../../../README.md#repository-boundary). The Redis key contract below is
> owned by this repository and is authoritative. The class and bean names are as of the split and
> may have been renamed there; nothing in either repository detects that.

For the complete local startup sequence, follow the [canonical finite local workflow](../../README.md#canonical-finite-local-workflow).

The current Java serving path does not implement a `CandidateHydrator` interface or candidate
hydration pipeline. `ContentCandidateRetriever` passes `MovieCandidate` records containing an item
ID, popularity score, content score, and cold-start-source flag directly into stage 6.

## Required state

- Serving content metadata comes from `recsys.catalog` plus the optional
  `recsys.catalog-path` file. It is normalized by `CatalogContentScoring` during retrieval and used
  again during scoring; it is not fetched by a candidate-hydrator stage.
- `MovieLensContextCollectorStreamingJob` writes `movie:{id}:features`, but the current retrieval
  service does not read those hashes. They enrich derived training/relevance datasets in the data
  pipeline.
- Redis user/item embeddings are not stage 5 hydration state. Stage 6 reads them directly while
  calculating relevance.

Consequently, this stage has no independent required serving state today. If an item is absent from
the configured catalog, it can still flow from popularity when it is not excluded by history, but
catalog content signals are zero and catalog metadata checks cannot apply. Missing
`movie:{id}:features` has no effect on serving; missing embeddings have the stage 6 fallback
described on the next page.

Core item data, social proximity, engagement, subscription, media, visibility, and safety
hydrators are aspirational concepts in this guide. Implementing them requires actual
`CandidateHydrator` wiring plus defined state/upstream writers.
