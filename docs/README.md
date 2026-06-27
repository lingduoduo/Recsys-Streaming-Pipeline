# Documentation

Development docs for the Recsys Streaming Pipeline — specs (what & why), plans
(how, task-by-task), and research notes.

## Specs — [`specs/`](specs/)

| Doc | What it covers |
|-----|----------------|
| [training-consolidation.md](specs/training-consolidation.md) | Offline/online training consolidation: gaps identified and a phased plan to unify the two training paths. |
| [2026-06-27-consolidation-phase2-phase5.md](specs/2026-06-27-consolidation-phase2-phase5.md) | Scoped follow-up: close training-consolidation Phase 2 (unified data contract) and Phase 5 (online→offline feedback) gaps. |
| [2026-06-26-data-pipeline-optimization.md](specs/2026-06-26-data-pipeline-optimization.md) | Streaming data-pipeline optimization across five dimensions (throughput, cost, correctness, code quality, operability). Phase 1 + Phase 2. |
| [2026-06-27-user-segment-simulation.md](specs/2026-06-27-user-segment-simulation.md) | Synthetic Kafka→Spark stream for comparing engagement across user segments (cohort, age, sex, education, geo, platform) via the existing feature maps. |

## Plans — [`plans/`](plans/)

Implementation plans (bite-sized TDD tasks) derived from the specs.

| Doc | Status |
|-----|--------|
| [2026-06-14-phase1-quick-wins.md](plans/2026-06-14-phase1-quick-wins.md) | Training consolidation — Phase 1 |
| [2026-06-14-phase2-data-contract-and-two-tower.md](plans/2026-06-14-phase2-data-contract-and-two-tower.md) | Training consolidation — Phase 2 |
| [2026-06-14-phase3-automation-and-feedback.md](plans/2026-06-14-phase3-automation-and-feedback.md) | Training consolidation — Phase 3 |
| [2026-06-26-data-pipeline-optimization.md](plans/2026-06-26-data-pipeline-optimization.md) | Data-pipeline optimization Phase 1 — shipped (PR #88) |
| [2026-06-26-data-pipeline-phase2.md](plans/2026-06-26-data-pipeline-phase2.md) | Data-pipeline optimization Phase 2 (event dedup + corrupt accounting) — shipped (PR #89) |
| [2026-06-27-user-segment-simulation.md](plans/2026-06-27-user-segment-simulation.md) | User-segment engagement simulation — shipped (PR #93) |
| [2026-06-27-consolidation-phase2-phase5.md](plans/2026-06-27-consolidation-phase2-phase5.md) | Training consolidation — Phase 2 + Phase 5 gap closure — shipped (PR #91) |

## Notes — [`notes/`](notes/)

| Doc | What it covers |
|-----|----------------|
| [training-consolidation-findings.md](notes/training-consolidation-findings.md) | Research findings backing the consolidation spec. |

---

> New specs/plans authored via the brainstorming / writing-plans skills default
> to `docs/superpowers/{specs,plans}` — move them into `docs/specs` / `docs/plans`
> and add a row above to keep this index current.
