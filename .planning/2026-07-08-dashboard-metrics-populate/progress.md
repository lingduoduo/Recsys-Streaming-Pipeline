# Progress Log

## Session: 2026-07-08

### Current Status
- **Phase:** 4 - COMPLETE. Dashboard shows real recall + ranking metrics; delivered to repo.

### Actions Taken
- Investigated dashboard + eval modules; mapped 4 Redis contracts and their Spark producers.
- Diagnosed degenerate metrics from committed index.html (recall embedding=0, hybrid==bm25; ranking only position@0.495).
- Found half-done design doc; producer ratings export already landed (commit dade1a3).
- Wired `run-movie-category-sim.sh`: RATINGS_OUTPUT_PATH on producer; added UserEventStreamingJob popularity drain;
  added Item2Vec (i2vEmb) + user-embedding (uEmb) jobs; added final dashboard step; added `down -v` infra reset.
- Started Colima; confirmed Java 17.
- Run 1 failed (Kafka NodeExists / stale ZK). Added `down -v`; re-ran.
- Run 2 in progress: build OK, producer OK (20000 slates / 123780 events), collector wrote 400 movie:*:features,
  joiner producing parquet.

### Test Results
| Test | Expected | Actual | Status |
|------|----------|--------|--------|
| movie:*:features count | 400 | 400 | pass |
| i2vEmb:movie_* count | >0 | 400 | pass |
| uEmb:user_* count | >0 | 200 | pass |
| global:item_popularity ZCARD | >0 | 400 | pass |
| dashboard recall embedding>0 & hybrid≠bm25 | yes | emb@10=0.0337, hybrid 0.0309 vs bm25 0.0308 | pass |
| dashboard ranking popularity+embedding coverage>0 | yes | pop AUC 0.563, emb AUC 0.547, both cov 1.0 | pass |
| focused python suite | pass | 10 passed | pass |

### Errors
| Error | Resolution |
|-------|------------|
| Kafka NodeExistsException (stale ZK) | `docker compose down -v` before `up -d` |
| Docker.app not found | Colima runtime; `colima start` |
