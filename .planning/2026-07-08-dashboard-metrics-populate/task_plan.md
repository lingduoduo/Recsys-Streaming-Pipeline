# task_plan.md — Analysis Dashboard: populate all data + generate all metrics (recall + ranking)

**Goal**: Make the consolidated analysis dashboard (`analysis_dashboard_report.py`) render *real* recall and
ranking metrics — not degenerate placeholders — by generating and populating every Redis contract the
metrics depend on, then regenerating `report-dashboard/index.html`.

**Success Criteria**:
- Redis populated with all four contracts: `movie:*:features`, `i2vEmb:movie_*`, `uEmb:user_*`, `global:item_popularity`.
- Dashboard recall section: `embedding` recall > 0 and `hybrid` ≠ `bm25` (embeddings actually contribute).
- Dashboard ranking section: `popularity` and `embedding` signals have coverage > 0 and real AUC (not nan / not only `position`).
- `report-dashboard/index.html` regenerated from the fresh run.

## Current Phase
Phase 4 complete — all success criteria met.

## Phases

### Phase 1: Investigate current dashboard + data state
- [x] Read `analysis_dashboard_report.py` + recall/ranking eval modules; map Redis key contracts
- [x] Inspect existing `report-dashboard/index.html` — found degenerate recall/ranking
- [x] Confirm environment cold: Redis down, no parquet, Docker daemon down
- [x] Map producers of each Redis contract (4 Spark jobs)
- [x] Find the partially-implemented embedding-integration design doc
- **Status:** complete

### Phase 2: Wire the simulation to produce all data
- [x] Producer already writes matching `ratings.csv` (commit dade1a3) — set `RATINGS_OUTPUT_PATH` in sim
- [x] Add Item2Vec job (`i2vEmb`) via `run-offline-pipeline.sh`
- [x] Add user-embedding job (`uEmb`) via `run-user-embedding-pipeline.sh`
- [x] Add `UserEventStreamingJob` drain → `global:item_popularity` (ranking popularity signal)
- [x] Run consolidated dashboard at the end of the sim
- [x] Add `docker compose down -v` to avoid stale ZooKeeper broker registration
- **Status:** complete

### Phase 3: Run end-to-end
- [x] Start Colima (Docker runtime) + confirm Java 17
- [x] Run 1 failed: Kafka NodeExists (stale ZK) → fixed with `down -v`
- [x] Run 2: sbt assembly → producer → 3 drains → embeddings → dashboard (all OK)
- **Status:** complete

### Phase 4: Verify
- [x] Redis counts nonzero for all four contracts (400 features / 400 i2vEmb:movie_* / 200 uEmb:user_* / 400 popularity)
- [x] Dashboard recall: embedding@10=0.0337 > 0, hybrid (0.0309) ≠ bm25 (0.0308)
- [x] Dashboard ranking: popularity AUC 0.563, embedding AUC 0.547, position 0.503 — all coverage 1.0
- [x] Copied fresh dashboard to recsys-pipeline/report-dashboard/index.html; focused test suite 10/10
- [x] Report results to user
- **Status:** complete

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Follow design doc `2026-07-06-movie-category-embedding-integration-design.md`; reuse existing Spark jobs | Don't build a second vector implementation |
| Also wire `UserEventStreamingJob` for popularity (beyond the doc) | Ranking's 3rd signal (popularity) must be real too |
| Keep ratings/embedding artifacts under `$SIM_ROOT` | Not committed; matches design boundary |
| Defer dashboard N/A fallback semantics | Only matters when embeddings absent; after this run they're present |

## Errors Encountered
| Error | Resolution |
|-------|------------|
| Kafka `NodeExistsException` at registerBroker (stale ZK volume) | `docker compose down -v` before `up -d`; added to sim script |
