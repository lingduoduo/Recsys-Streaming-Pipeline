# Findings & Decisions

## Requirements
- Dashboard must show real recall AND ranking metrics (user: "populate all data, generate all metrics and reports, including recall and ranking metrics").

## Research Findings

### Redis contracts the dashboard metrics depend on
| Metric | Redis key(s) | Producer (Spark job) | Runner |
|--------|--------------|----------------------|--------|
| Recall corpus (BM25) | `movie:*:features` | `MovieLensContextCollectorStreamingJob` | movie-category-sim |
| Recall embedding/hybrid | `i2vEmb:{itemId}` | `Item2VecTrainingJob` | `run-offline-pipeline.sh` |
| Ranking embedding signal | `uEmb:{userId}` + `i2vEmb:*` | `UserEmbeddingTrainingJob` | `run-user-embedding-pipeline.sh` |
| Ranking popularity signal | `global:item_popularity` (ZSET) | `UserEventStreamingJob` | (was unrun) |
| Ranking position signal | (from parquet `position` col) | n/a | n/a |

### Why the old dashboard was degenerate (evidence from committed index.html)
- Recall: `embedding`=0.0 at all k; `hybrid`==`bm25` exactly → no `i2vEmb:*` present.
- Ranking: `popularity` n=0/coverage 0/auc nan; `embedding` n=0/coverage 0/auc nan; only `position` scored, AUC 0.495 (random).
- Root cause: only `movie:*:features` was ever populated. Embeddings + popularity never generated for this sim.

### The half-done design
`docs/specs/2026-07-06-movie-category-embedding-integration-design.md` addresses this. Commit `dade1a3` did step 2
(producer writes matching `ratings.csv` with `movie_*`/`user_*` ids, click→4.0, order→5.0, order supersedes click
per slate). Steps 3–6 (sim wiring + dashboard) were NOT implemented — this task completes them.

### ID alignment
Sim emits `movie_*` items and `user_*` users. The generated `ratings.csv` uses the SAME ids, so Item2Vec/user
embeddings key on `i2vEmb:movie_*` / `uEmb:user_*`, matching the parquet `item_id`/`user_id`. This is what makes
recall/ranking evaluable (the bundled `item_*` ratings never could).

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| Reuse existing Spark Item2Vec + user-embedding jobs | Design boundary: no second vector impl |
| Wire `UserEventStreamingJob` for popularity too | Otherwise ranking's popularity signal stays empty |
| `docker compose down -v` at sim start | Stale ZK broker node caused Kafka `NodeExistsException` |

## Issues Encountered
| Issue | Resolution |
|-------|------------|
| Docker "Docker.app" not found | Runtime is Colima; `colima start` |
| Kafka not healthy (NodeExists, stale ZK) | `down -v` reset volumes |

## Resources
- Design: docs/specs/2026-07-06-movie-category-embedding-integration-design.md
- Sim script: recsys-pipeline/run-movie-category-sim.sh
- Dashboard: recsys-pipeline/services/python-modeling/analysis_dashboard_report.py
- Run log: scratchpad/sim-run2.log
