# Findings & Decisions

## Requirements
- Investigate adding measures for relevance, satisfaction/ratings, freshness, diversity, fairness, safety, and latency.
- Report current coverage, missing inputs, implementation points, and a practical rollout order.

## Research Findings
- No existing `.ua/` or legacy knowledge graph was found.
- Relevance: offline recall evaluation already reports recall@k and hit-rate@k using leave-one-out clicks; ranking evaluation reports AUC/log-loss/score coverage. Spark also reports a graded 0/1/2 engagement label, but calls genre combinations "queries", so it is not true query-intent relevance.
- Satisfaction: the online feedback API accepts `clicked` plus a generic reward constrained to [0,1]. Historical MovieLens ratings are collected separately and segment reports expose weighted average ratings, but the serving feedback contract has no rating, dislike, reason, dwell, completion, or request ID.
- Freshness: catalog profiles contain a boolean `newRelease`; new releases enter cold-start logic and candidate probing. There is no publication timestamp, age-decay formula, freshness-at-k metric, or freshness outcome comparison.
- Diversity: serving applies a repeated-primary-genre decay and exposes a per-item diversity multiplier. It does not aggregate intra-list diversity, unique genre share, entropy, or long-tail exposure.
- Fairness: demographic data (age band, gender, occupation, geo) is collected and segment CTR/order rate is reported. There is no exposure parity, quality parity, calibration, or confidence/support gating by group.
- Safety: candidate eligibility supports muted product types, genres, keywords/title matches, and expiry. This is a configurable content filter, not a measured safety taxonomy; no violations, false positives, audit reason, or policy-version metrics are emitted.
- Latency: the Java service has no Actuator/Micrometer dependency and no endpoint/stage timers. Streaming jobs expose processing cadence and batch monitoring but not user-visible serve latency. Impression and feedback timestamps enable response-delay measures only after joining events.
- Online serving already aggregates requests, served items, clicks, CTR, observed/estimated reward, pseudo-regret, novelty, cold-start impressions, exploratory impressions, and catalog coverage by algorithm.
- Training samples preserve request ID, user/item, rank position, impression timestamp, feedback timestamp, click/order labels, and feature maps; slate experiences preserve ordered items and user/context/item features. This is a strong base for offline metric expansion.

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| Treat "possibly to add" as feasibility/gap analysis | No implementation scope or metric definitions were specified |
| Reuse the slate experience as the canonical offline evaluation grain | It preserves rank order, outcomes, user/context features, and item features |
| Add latency in the serving layer, not infer it from Spark | User-visible latency must cover the synchronous Java request path; Spark timestamps can measure delayed feedback and pipeline lag separately |
| Keep guardrails separate from objective metrics | Safety/fairness thresholds should not be traded away inside one blended ranking score |

## Proposed Minimum Metric Set
- Relevance: NDCG@K, MAP@K or MRR, Recall@K, HitRate@K; retain AUC/log-loss for pointwise diagnostics.
- Satisfaction: CTR, order/conversion rate, mean explicit rating, negative-feedback rate, dwell/completion where available; all joined by request ID and position.
- Freshness: fresh-item share@K, mean/median content age, freshness-weighted exposure, CTR/reward for fresh vs established items.
- Diversity: unique genres@K, genre entropy, intra-list diversity (embedding or genre Jaccard distance), long-tail exposure share.
- Fairness: exposure share and NDCG/CTR/reward per group; report max-min/ratio gaps, support counts, and confidence intervals.
- Safety: unsafe exposure rate, filter-trigger rate by reason/policy version, false-positive/appeal rate when labels exist, and fail-open/fail-closed counts.
- Latency: endpoint and stage p50/p95/p99, error rate, timeout rate, Redis/model timing, Kafka event lag, and impression-to-feedback delay.

## Recommended Rollout
1. Instrument latency plus schema/version fields (`requestId`, algorithm/model/policy versions) because every later comparison needs trustworthy attribution.
2. Add slate-level offline relevance and diversity metrics from existing experiences.
3. Extend feedback with optional explicit satisfaction/negative signals, preserving backward compatibility.
4. Replace boolean freshness with timestamps and compute freshness/quality tradeoffs.
5. Add fairness slices and safety audit counters only after minimum support/privacy rules and taxonomies are defined.

## Issues Encountered
| Issue | Resolution |
|-------|------------|

## Resources
- `services/java-retrieval-service/.../HybridRecommendationService.java`
- `services/java-retrieval-service/.../MovieLensServingSideEffects.java`
- `services/spark-streaming-job/.../OnlineJoinerStreamingJob.scala`
- `services/spark-streaming-job/.../ExperienceCollectorStreamingJob.scala`
- `services/python-modeling/recall_eval_report.py`
- `services/python-modeling/ranking_eval_report.py`
