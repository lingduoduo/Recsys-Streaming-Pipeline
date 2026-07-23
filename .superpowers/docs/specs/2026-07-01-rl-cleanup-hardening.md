# Spec: RL Cleanup + Hardening

> Final cleanup pass after the scoring/RL audit fixes
> ([#110](2026-07-01-scoring-cutoff-centralization.md),
> [#111](2026-07-01-qlearning-state-recurrence.md),
> [#112](2026-07-01-rl-feedback-correctness.md)). Bundles three remaining
> low-risk threads into one PR: a config-default decision, two small code
> cleanups, and real (Redis-backed) tests for the already-merged B1/B3 fixes.

## Objective

Close out the audit follow-ups: make the deep-learning term opt-in by default,
remove dead code and a scale-mixing fusion smell, and replace the structural-only
coverage of the pending-key idempotency and Lua Q-update with real Redis-backed
integration tests.

## Scope

- **In:** `application.yml` + both READMEs (DL-weight default); `HybridRecommendationService`
  (dead `readCount`, two-tower/DL fusion); `pom.xml` (Testcontainers test dep); a new
  Docker-gated integration test.
- **Out:** any change to the scoring math, the RL update rules, or the two-tower
  serving contract; broader feedback-counter idempotency (still deferred).

## Threads & changes

### T1 — deep-learning weight defaults to 0.0

`ln.onnx` (the served MLP) has an unverified output range. Even though `dlScore` is
now clamped to `[0,1]`, the safe default is to **not** let it influence ranking
unless an operator opts in.

**Change:** `recsys.bandit.deep-learning-weight` default `0.15 → 0.0` in
`application.yml`; update the root `README.md` architecture line and the
`recsys-pipeline/README.md` config/env tables to `0.0`. Operators enable it with
`RECSYS_DEEP_LEARNING_WEIGHT`. Code and docs now agree at `0.0`.

### T2a — remove dead `readCount(Long)`

`HybridRecommendationService.readCount(Long)` (~line 1311) is declared but never
called (an orphan from a prior refactor; the counter path uses `readLong(Object)`).

**Change:** delete the method.

### T2b — clamp before two-tower/DL fusion

The two-tower/DL merge (~lines 212-214) takes `Math::max` of `dlScoresRaw` (raw ONNX,
unbounded) and `twoTowerScores` (a weighted sigmoid blend, ~`[0,1]`) — a max across
mismatched scales, so the larger-magnitude producer wins regardless of confidence.
Only reachable on the opt-in two-tower path (`ONNX_*_TOWER_PATH` set).

**Change:** clamp both to `[0,1]` (`RecommendationConstants.clamp`) before `max`, so
the fusion compares like scales. Behavior unchanged when two-tower is disabled.

### T3 — Redis-backed tests for B1 & B3

B1 (pending-key delete-on-consume) and B3 (atomic Lua Q-update) are currently only
verified structurally (mock interactions). Add a real Redis to prove the behavior.

**Change:** add `org.testcontainers:junit-jupiter` (test scope) and a `redis:7-alpine`
container. A new integration test builds a real `StringRedisTemplate` against the
container and a `q-learning`-configured `HybridRecommendationService`, and asserts:
- **B1:** after `recordFeedback`, `replay:pending:{user}:{item}` is deleted; a replayed
  feedback does **not** change the Q-value again; the pending key was written with a TTL.
- **B3:** two sequential feedbacks accumulate Q exactly per
  `q ← q + α·(r + γ·nextValue − q)` through the real Lua `EVAL`, proving the script
  persists atomically and is the single writer.

The test is **Docker-gated**: `@BeforeAll` runs `assumeTrue(DockerClientFactory.instance().isDockerAvailable())`, so environments without Docker skip it and the build stays green.

## Work items & acceptance

- **T1.** *Accept:* a default-config boot has `deep-learning-weight = 0.0`; both READMEs
  show `0.0`; setting `RECSYS_DEEP_LEARNING_WEIGHT=0.15` restores the contribution.
- **T2a.** *Accept:* `readCount` is gone; project compiles; no reference remains.
- **T2b.** *Accept:* fusion clamps both operands to `[0,1]` before `max`; two-tower-disabled
  behavior byte-identical.
- **T3.** *Accept:* with Docker present the IT passes (B1 idempotency + TTL, B3 arithmetic);
  without Docker it is skipped, not failed; the existing 50 unit tests stay green.

## Non-goals / risks

- Three unrelated threads share one PR (explicit request); commits are split per thread
  for traceability.
- The B3 test asserts sequential correctness and single-writer-by-construction, not a
  non-deterministic parallel race.
- Testcontainers requires a Docker daemon to actually exercise T3; CI without Docker will
  skip it (coverage gap surfaced by a skipped test, not hidden).
