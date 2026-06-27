# Documentation

Development docs for the Recsys Streaming Pipeline — specs (what & why), plans
(how, task-by-task), and research notes.

## Specs — [`specs/`](specs/)

| Doc | What it covers |
|-----|----------------|
| [training-consolidation.md](specs/training-consolidation.md) | Offline/online training consolidation: gaps identified and a phased plan to unify the two training paths. |
| [2026-06-27-consolidation-phase2-phase5.md](specs/2026-06-27-consolidation-phase2-phase5.md) | Scoped follow-up: close training-consolidation Phase 2 (unified data contract) and Phase 5 (online→offline feedback) gaps. |
| [2026-06-26-data-pipeline-optimization.md](specs/2026-06-26-data-pipeline-optimization.md) | Streaming data-pipeline optimization across five dimensions (throughput, cost, correctness, code quality, operability). Phase 1 + Phase 2. |
| [2026-06-27-movielens-segment-simulation.md](specs/2026-06-27-movielens-segment-simulation.md) | Segment-comparison sim aligned to canonical UserEvent demographics via the movielens_context→Redis path; report joins Parquet engagement with Redis demographics. |
| [2026-06-27-movie-category-simulation.md](specs/2026-06-27-movie-category-simulation.md) | Item-side parallel: compares engagement across a 3-level movie category (l1 family / l2 genre / l3 genre×decade) via the movie metadata → Redis path. |
| [2026-06-27-session-id-tracking.md](specs/2026-06-27-session-id-tracking.md) | Thread session_id (emitted but currently dropped at from_json) through OnlineJoiner into training_samples for session-level engagement analysis — additive. |
| [2026-06-27-recall-samples-job.md](specs/2026-06-27-recall-samples-job.md) | Derive per-impression recall data (user/session/time/recommended+click movie/label) from training_samples → new recall_samples Kafka topic. |
| [2026-06-27-ranking-samples-job.md](specs/2026-06-27-ranking-samples-job.md) | Derive per-impression ranking data (user/item features + Redis embeddings, is_click, rating) from training_samples → new ranking_samples Kafka topic. |
| [2026-06-27-relevance-samples-job.md](specs/2026-06-27-relevance-samples-job.md) | Derive per-impression relevance data (query=user:session, movie title/genres/year from Redis, score=label) from training_samples → new relevance_samples Kafka topic. |
| [2026-06-27-keyword-analysis.md](specs/2026-06-27-keyword-analysis.md) | Keyword analysis: first keyword/subkeyword (genre) distribution + per-category (l1/l2/l3) top keywords for movies vs queries. |
| [2026-06-27-query-analysis.md](specs/2026-06-27-query-analysis.md) | Query analysis: most-common queries (genre-combo intent) + short (≤10 chars) vs long query engagement (CTR/CVR/avg rating). |

## Plans — [`plans/`](plans/)

Implementation plans (bite-sized TDD tasks) derived from the specs.

| Doc | Status |
|-----|--------|
| [2026-06-14-phase1-quick-wins.md](plans/2026-06-14-phase1-quick-wins.md) | Training consolidation — Phase 1 |
| [2026-06-14-phase2-data-contract-and-two-tower.md](plans/2026-06-14-phase2-data-contract-and-two-tower.md) | Training consolidation — Phase 2 |
| [2026-06-14-phase3-automation-and-feedback.md](plans/2026-06-14-phase3-automation-and-feedback.md) | Training consolidation — Phase 3 |
| [2026-06-26-data-pipeline-optimization.md](plans/2026-06-26-data-pipeline-optimization.md) | Data-pipeline optimization Phase 1 — shipped (PR #88) |
| [2026-06-26-data-pipeline-phase2.md](plans/2026-06-26-data-pipeline-phase2.md) | Data-pipeline optimization Phase 2 (event dedup + corrupt accounting) — shipped (PR #89) |
| [2026-06-27-consolidation-phase2-phase5.md](plans/2026-06-27-consolidation-phase2-phase5.md) | Training consolidation — Phase 2 + Phase 5 gap closure — shipped (PR #91) |
| [2026-06-27-movielens-segment-simulation.md](plans/2026-06-27-movielens-segment-simulation.md) | MovieLens-aligned user-segment simulation — shipped (PR #94) |
| [2026-06-27-movie-category-simulation.md](plans/2026-06-27-movie-category-simulation.md) | Movie-category (l1/l2/l3) engagement simulation — shipped (PR #96) |
| [2026-06-27-session-id-tracking.md](plans/2026-06-27-session-id-tracking.md) | Track session_id through the data stream into training_samples + training_experiences — implemented (PR #97) |
| [2026-06-27-recall-samples-job.md](plans/2026-06-27-recall-samples-job.md) | Recall-samples streaming job (training_samples → recall_samples topic) — shipped (PR #98) |
| [2026-06-27-ranking-samples-job.md](plans/2026-06-27-ranking-samples-job.md) | Ranking-samples streaming job (training_samples + Redis embeddings → ranking_samples topic) — shipped (PR #99) |
| [2026-06-27-relevance-samples-job.md](plans/2026-06-27-relevance-samples-job.md) | Relevance-samples streaming job (training_samples + Redis movie metadata → relevance_samples topic) — shipped (PR #100) |
| [2026-06-27-keyword-analysis.md](plans/2026-06-27-keyword-analysis.md) | Keyword analysis report (distribution + category top keywords) — shipped (PR #102) |
| [2026-06-27-query-analysis.md](plans/2026-06-27-query-analysis.md) | Query analysis report (most-common + short-vs-long engagement) — shipped (PR #103) |

## Notes — [`notes/`](notes/)

| Doc | What it covers |
|-----|----------------|
| [training-consolidation-findings.md](notes/training-consolidation-findings.md) | Research findings backing the consolidation spec. |

---

> New specs/plans authored via the brainstorming / writing-plans skills default
> to `docs/superpowers/{specs,plans}` — move them into `docs/specs` / `docs/plans`
> and add a row above to keep this index current.
