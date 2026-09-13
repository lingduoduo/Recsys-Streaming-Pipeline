# DPO prerequisite met

**Date:** 2026-09-12
**Status:** Implemented and verified; [PR #223](https://github.com/lingduoduo/Recsys-Streaming-Pipeline/pull/223) open

## Problem and scope

The offline DPO arm (`post-training/post_train_dpo.py`) joins the slate log to the replay buffer on
request id. When it was built (2026-08-20) that join matched nothing: the slate log's
`request_id` came from the Python producer and the replay's `requestId` from the Java serving
path, two independent generators. Three places still say so, in the present tense:

- the CLI docstring and its total-join-failure error text in `post_train_dpo.py`
- the "Unmet prerequisite" paragraph in the README's DPO block
- the DPO paragraph in `docs/recommendation_architecture/Analysis_Report.md`

Since then the serving path gained its own Kafka emission: PR #208 publishes per-slate impression
events and PRs #210 and #216 publish feedback events, all through `GrpoEventPublisher`, all
carrying `request.requestId()`, and all on the topic `OnlineJoinerStreamingJob` consumes. The Redis
replay context written by `MovieLensServingSideEffects.serializeReplayContext` uses the same
`request.requestId()`. So with `RECSYS_GRPO_EMIT_EVENTS=true`, slates built from serving traffic
carry the replay's id and the DPO join is reachable. The flag defaults to `false`, so a default
deployment still produces nothing for DPO; that is now an operator setting, not a structural gap.

Nothing pins the value-level identity. `GrpoImpressionEventsTest` checks the event's `request_id`
and `MovieLensServingSideEffectsTest` checks the replay payload, but no test asserts that one
served request puts the same id in both. This change adds that test and brings the three documents
and the CLI message up to date. It does not enable the flag by default, change the join, or touch
the DPO loss.

## Global constraints

- Java 17 with the existing Maven build; Python 3 with numpy and pandas; add no dependencies.
- Keep `RECSYS_GRPO_EMIT_EVENTS` defaulting to `false`.
- Keep `slate_pairs`, `dpo`, `replay_dataset`, and the DPO CLI's arguments, outputs, and printed
  matched-count line unchanged.
- Keep the CLI's `SystemExit` on zero pairs, including the dropped-pair and matched-count figures
  that `test_main_names_the_request_id_mismatch_when_nothing_joins` asserts.
- Keep the accuracy caveats in the README and Analysis_Report DPO sections unchanged.

## Alternatives and decision

| Approach | Tradeoff |
|---|---|
| Update the docs only | Cheapest, but leaves the id identity unpinned; a future refactor could reintroduce two generators with a green suite. |
| Docs plus a serving-side test asserting one id in both sinks | Selected: the identity is a property of `MovieLensServingSideEffects.recordServed`, which is where both writes happen, so that is where the test belongs. |
| Live end-to-end run through Docker Compose | Strongest evidence, but the compose stack has been unreliable on this machine and the identity is provable from code plus the new test. Not done. |
| Flip the emit flag default to `true` | Operational decision outside this change. |

## Design

### Serving-side contract test

`MovieLensServingSideEffectsTest` gains a test that builds the side effects with an enabled
`GrpoEventPublisher` and a recording `GrpoSender`, calls `recordServed` once for a request with id
`req-served-42`, captures the pending replay payload from `valueOps.set`, parses it as JSON, decodes
the single sent Avro payload the way `GrpoEventPublisherTest` does, and asserts the replay's
`requestId` equals the event's `request_id` and both equal the request's id.

### CLI text

The docstring's "UNMET PREREQUISITE" paragraph becomes a "PREREQUISITE" paragraph stating that
the join needs slates built from serving-emitted events, which requires `RECSYS_GRPO_EMIT_EVENTS=true`
on the retrieval service; slates from the Python producers alone still cannot join. The
total-join-failure error names that flag and the producer-only case as the two causes of zero
matches, keeping the dropped-pair and matched-count figures first.

### Documentation

The README paragraph is retitled "Prerequisite" and rewritten to the same content. The
Analysis_Report sentence "It does not run on today's data" becomes a sentence saying it runs on
slates built from serving-emitted events and points at the flag.

### Test assertions

`test_main_names_the_request_id_mismatch_when_nothing_joins` keeps its figure assertions and
replaces the `movie_segment_producer` / `HybridRecommendationService` substring checks with checks
for `RECSYS_GRPO_EMIT_EVENTS` and "serving". The `_mismatched_fixture` docstring is updated to say
it reproduces producer-only data.

## Files

Paths below are relative to the repository root.

| File | Responsibility |
|---|---|
| `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java` | Contract test: one served request, one id in the replay context and the emitted event. |
| `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py` | Docstring and zero-pair error text. |
| `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py` | Error-text assertions and fixture docstring. |
| `recsys-pipeline/README.md` | DPO prerequisite paragraph. |
| `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md` | DPO paragraph. |
| `.superpowers/docs/plans/2026-09-12-dpo-prerequisite-met.md` | Reproducible implementation and verification steps. |

## Acceptance criteria

1. The new Java test passes and fails if either sink stops using `request.requestId()` (checked by
   inspection of the assertions, not by mutating production code).
2. `MovieLensServingSideEffectsTest` reports 3 tests, 0 failures under JDK 17.
3. `test_post_training_dpo.py` passes with the updated assertions; the zero-pair `SystemExit`
   message still contains the dropped-pair and matched-count figures.
4. No file in the repository still says the DPO arm "produces nothing on today's data" or that the
   prerequisite is unmet.
5. The full Python modeling suite and the retrieval service's Maven suite pass, with unrelated
   failures reported against a baseline rather than claimed green.

## Risks and limits

The test proves both sinks share one id inside serving; it does not prove the joiner, the
experience collector, and the slate export preserve it end to end. Those stages are covered by
their own suites and by the `n_slate_request_ids_matched` line the CLI prints, which remains the
runtime check. Rollback is a revert; no data changes.

## Verification record

- `mvn test -Dtest=MovieLensServingSideEffectsTest` under JDK 17: 2 tests before, 3 after, 0 failures.
  The new test passed on the unchanged production code, as expected for a characterization test.
- Retrieval service full Maven suite: 329 tests, 0 failures, 0 errors, 1 skipped
  (`UserProfileIntegrationTest`, Testcontainers blocked on Docker 29), 37 s, 2026-09-12.
- `python3 -m pytest integration-tests/python_modeling -q`: 501 passed. `test_post_training_dpo.py`
  alone: 34 passed, after the mismatch-message assertion first failed on the old text as intended.
- Stale-text grep for "UNMET PREREQUISITE", "Unmet prerequisite", "does not run on today's data",
  and "produces nothing on today" across `*.py` and `*.md`: no matches.
