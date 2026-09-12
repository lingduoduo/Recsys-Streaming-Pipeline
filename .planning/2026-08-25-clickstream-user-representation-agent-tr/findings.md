# Findings & Decisions

## Requirements
- Trace raw search/view/click clickstreams end to end.
- Explain derived user representations and how serving consumes them.
- Explain agent-training traces/replay and downstream policy training.
- Assess whether current infrastructure supports credible user-behavior simulation.
- Provide gaps and prioritized recommendations; do not modify product code.

## Research Findings
- Understand-domain lightweight scan found 71 source files and 40 file signatures, but 0 generic entry points. The pipeline's entry points are custom Scala jobs and shell/Python runners, so direct code tracing is required.
- No `.ua/knowledge-graph.json` existed; domain analysis is being derived from fresh lightweight context at `.ua/intermediate/domain-context.json`.
- Relevant Spark boundaries are explicit: Avro decode/gating; `OnlineJoinerStreamingJob`; sample jobs for recall/ranking/relevance; `ExperienceCollectorStreamingJob`; sequence preprocessing/sinks; behavioral-profile batch derivation/publishing; Item2Vec/UserEmbedding/ALS; and CTR training.
- Relevant Python boundaries include generic and segment-specific producers, feedback scheduling, replay export, OPE, Q/FQI/DPO post-training, and three end-to-end simulation runners.
- Relevant serving boundaries include history/profile/sequence hydrators, `HybridRecommendationService`, `OnlineLearningService`, and the `ReplayEvent` contract.
- Canonical Avro v2 carries identity/lineage (`event_id`, request/session/user/item), generic `event_type`, position, feature maps, model/policy/algorithm versions, explicit feedback fields, and typed surface/locale/timezone/device. It has no typed query/search text or view-duration field beyond generic dwell/completion.
- The generic producer emits `click` alone in clickstream mode, or `impression` + probabilistic `click` + `order` in behavior mode. It uses surfaces including `search_results` and `detail_page`, but emits no `search` or `view` event type. Repository producer/test enumeration likewise shows impression/click/order/rating/thumb/abandon, not search/view events.
- `OnlineJoinerStreamingJob` joins only impression/exposure with feedback types click/order/purchase/thumb/abandon, grouped by request+user+item. It creates pointwise labels 0/1/2, preserves feature/context/version fields, and publishes Kafka plus date-partitioned Parquet after late-feedback handling.
- `ExperienceCollectorStreamingJob` groups pointwise samples by request+user into ordered slate traces containing user/context maps, per-item outcomes and measurement fields, slate-level click/order/reward, Kafka output, and optional Parquet.
- Behavioral profiles are a batch transform over enriched training-sample Parquet: recency-decayed impression/click/order/rating weights produce genre/tag preferences, engagement/conversion, diversity/concentration, recent-release affinity, and deterministic personas, then publish versioned snapshots to Redis.
- Dense `uEmb` user vectors are not learned from clickstreams: `UserEmbeddingTrainingJob` averages Item2Vec vectors only for ratings above a threshold. This is a separate MovieLens/offline representation path.
- There are two distinct training-trace systems: (1) Spark `training_experiences` slates derived from raw impressions/feedback, and (2) Java `replay:recommendations` RL events created at serve time and completed at `/feedback`. DPO joins them by `(requestId,item)`; Q/FQI chain only the Java replay by user/time.
- Java replay captures state (recent/genres/tags), candidate snapshot/action space, chosen action/position, policy name and scores, candidate model predictions/counters, serve time, reward, feedback time, and next state. Pending context is per user+item with TTL; missing/expired context still produces a minimal feedback event, which post-training may accept with empty state/action space.
- The logged `propensity` is hard-coded as `1/slateSize`, even though selection is top-k/score-driven. The OPE implementation therefore avoids propensity estimators and uses a fitted Direct Method, but its feature set is narrow (coldStart, cumulative impressions/clicks, and model predictions) and does not consume the logged user state or typed request context.
- Offline Q/FQI forms episodes by user and a 30-minute inactivity gap, using the next logged request's candidate set as feasible actions at s'. This is a useful transition approximation but not a recorded environment transition/session identifier.
- Synthetic infrastructure has reproducible seeds, stable latent user tastes/demographics, category/surface/device effects, delayed multi-signal feedback, Kafka/Spark/Redis/Parquet end-to-end runners, and ground-truth measurement checks. However, most synthetic slates are sampled randomly and user preferences are static; there is no evolving user-state/environment model driven by agent actions.
- The movie-category sim briefly exercises the live Java recommend/feedback loop, but feedback is mechanically posted on alternating requests rather than sampled from the same latent response model used by the raw-event producer. Thus it validates plumbing, not closed-loop policy behavior.

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| Use code and tests as primary evidence | Documentation can drift; runtime paths and contracts are authoritative. |
| Generate a domain graph as a supporting artifact | The flow crosses Python, Kafka, Spark, Redis, Parquet, Java, and post-training modules. |

## Issues Encountered
| Issue | Resolution |
|-------|------------|

## Resources
-
## Serving integration gaps

- `UserEventStreamingJob` writes daily click sequences (`seq:{user}:click:{day}`), but the inspected serving hydrator, `RatingSequencesQueryHydrator`, reads rating sequences only. This suggests click histories are materialized without being consumed by the main serving representation path.
- `UserBehaviorProfileQueryHydrator` supplies positive genre/tag weights from the activated batch profile, while dense `uEmb` generation remains ratings-driven. The repository therefore has several user representations, but no single unified representation built from search, view, and click behavior.
- `MovieLensContextCollectorStreamingJob` is a separate ratings/demographics/movie-metadata ingestion path, rather than the canonical clickstream path.
- A production-source search for `KIND_CLICK`/`KindClick` found only constants and the Spark writer; no Java serving reader consumes click sequences. `RatingSequencesQueryHydrator` explicitly requests `KIND_RATING`.
- `run-data-pipeline.sh` launches only the clickstream producer and `UserEventStreamingJob`; documentation explicitly says `OnlineJoinerStreamingJob`, `ExperienceCollectorStreamingJob`, and derived sample jobs are standalone. The complete training pipeline is therefore not a single operational unit.
- Documentation describes ranking samples as joining `uEmb`/`i2vEmb`, but those embeddings are supplied from the separate ratings-derived model path rather than learned from the clickstream samples themselves.
