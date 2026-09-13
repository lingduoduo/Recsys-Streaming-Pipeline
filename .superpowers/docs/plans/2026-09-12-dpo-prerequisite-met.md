# DPO Prerequisite Met Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pin that serving writes one request id to both the replay context and its emitted Kafka events, and bring the DPO arm's docs and error text up to date now that the join is reachable behind `RECSYS_GRPO_EMIT_EVENTS`.

**Architecture:** One Java contract test at the point where both writes happen (`MovieLensServingSideEffects.recordServed`). Text-only updates to the CLI docstring and error, its test assertions, the README, and the analysis report. No behavior change.

**Tech Stack:** Java 17 with Maven and JUnit 5 plus Mockito; Python 3.12 with pytest.

**Spec:** [DPO prerequisite met](../specs/2026-09-12-dpo-prerequisite-met-design.md)

## Global Constraints

- Java 17 with the existing Maven build; Python 3 with numpy and pandas; add no dependencies.
- Keep `RECSYS_GRPO_EMIT_EVENTS` defaulting to `false`.
- Keep `slate_pairs`, `dpo`, `replay_dataset`, and the DPO CLI's arguments, outputs, and printed matched-count line unchanged.
- Keep the CLI's `SystemExit` on zero pairs, including the dropped-pair and matched-count figures that `test_main_names_the_request_id_mismatch_when_nothing_joins` asserts.
- Keep the accuracy caveats in the README and Analysis_Report DPO sections unchanged.

## Execution notes

All paths are repository-relative. Run Maven from `recsys-pipeline/services/java-retrieval-service/`
with `JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home`.
Run pytest from `recsys-pipeline/`. The work is on `fix/dpo-prerequisite-met`, based on
`origin/master`. Publish a PR against `master`; never commit to `master`.

Baselines (2026-09-12): `mvn test -Dtest=MovieLensServingSideEffectsTest` → 2 tests, 0 failures.
`python3 -m pytest integration-tests/python_modeling -q` → 501 passed.

---

### Task 1: Serving-side contract test for one request id in both sinks

**Files:**
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java`

**Interfaces:**
- Consumes: `MovieLensServingSideEffects(StringRedisTemplate, ObjectMapper, Duration, GrpoEventPublisher, GrpoPolicyScorer)`, `GrpoEventPublisher(RecsysEventAvroCodec, GrpoSender, boolean)`, `MovieLensServingSideEffects.pendingReplayKey(String, String)`, `ReplayEvent.REQUEST_ID` (`"requestId"`), and the existing `servedMovie` and `offScorer` helpers in the test.
- Produces: a test that pins the identity; nothing else depends on it.

- [ ] **Step 1: Add the imports**

Add to the import block of `MovieLensServingSideEffectsTest.java`:

```java
import com.demo.retrieval.service.replay.ReplayEvent;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
```

- [ ] **Step 2: Add the test**

Insert after `recordServedSkipsEmptySelections` and before the `servedMovie` helper:

```java
    @Test
    void replayContextAndEmittedImpressionShareTheServingRequestId() throws Exception {
        // The offline DPO arm joins the slate log (built from these Kafka events) to the replay
        // buffer on request id. Both sinks must carry the SAME value from one served request. Two
        // independent generators is exactly the defect that left that join matching nothing until
        // serving emitted its own events, so the identity is pinned here, where both writes happen.
        List<byte[]> sent = new ArrayList<>();
        RecsysEventAvroCodec codec = new RecsysEventAvroCodec();
        sideEffects = new MovieLensServingSideEffects(redis, new ObjectMapper(), Duration.ofHours(1),
            new GrpoEventPublisher(codec, (key, payload) -> sent.add(payload), true),
            offScorer());
        ServedMovie top = servedMovie("m1", false);

        sideEffects.recordServed(new ServingSideEffectRequest(
            "req-served-42", "u1", "ucb", Map.of(), List.of(top), List.of(top),
            List.of(), List.of(), 1, 0, 1, 0.8, 0.1, 0.5));

        ArgumentCaptor<String> pendingPayload = ArgumentCaptor.forClass(String.class);
        verify(valueOps).set(eq(MovieLensServingSideEffects.pendingReplayKey("u1", "m1")),
            pendingPayload.capture(), eq(Duration.ofHours(1)));
        Map<?, ?> replay = new ObjectMapper().readValue(pendingPayload.getValue(), Map.class);

        assertEquals(1, sent.size());
        byte[] payload = sent.get(0);
        var decoder = DecoderFactory.get().binaryDecoder(payload, 10, payload.length - 10, null);
        GenericRecord event = new GenericDatumReader<GenericRecord>(codec.schema()).read(null, decoder);

        assertEquals("req-served-42", replay.get(ReplayEvent.REQUEST_ID));
        assertEquals(replay.get(ReplayEvent.REQUEST_ID), event.get("request_id").toString());
    }
```

- [ ] **Step 3: Run the suite**

Run: `cd recsys-pipeline/services/java-retrieval-service && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn test -Dtest=MovieLensServingSideEffectsTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 3, Failures: 0, Errors: 0`. This is a characterization test; it passes on the current code because both writes already read `request.requestId()`.

- [ ] **Step 4: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java
git commit -m "test: pin one serving request id across the replay context and emitted events"
```

---

### Task 2: CLI docstring, error text, and test assertions

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py:13-19` and `:143-160`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py:339-350` and `:380-394`

**Interfaces:**
- Consumes: `slate_pairs.build_pairs_with_diagnostics` (unchanged) and its `n_slate_request_ids_matched` / `n_slate_request_ids` fields.
- Produces: the same `SystemExit` with new wording; `main`'s return value and stdout are unchanged.

- [ ] **Step 1: Update the test assertions first**

In `test_main_names_the_request_id_mismatch_when_nothing_joins`, replace

```python
    assert "movie_segment_producer" in message
    assert "HybridRecommendationService" in message
    assert "serving path" in message.lower()
```

with

```python
    assert "RECSYS_GRPO_EMIT_EVENTS" in message
    assert "serving" in message.lower()
```

Replace the `_mismatched_fixture` docstring with:

```python
    """Producer-only slates against a serving-written replay: the ids can never coincide.

    The Python producer mints the slate log's request_id as f"req_{uuid4().hex[:12]}"; the replay's
    requestId is serving's UUID.randomUUID().toString(). Only slates built from serving-emitted
    events (RECSYS_GRPO_EMIT_EVENTS=true) carry the replay's id.
    """
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py::test_main_names_the_request_id_mismatch_when_nothing_joins -q`
Expected: FAIL with `assert 'RECSYS_GRPO_EMIT_EVENTS' in message`.

- [ ] **Step 3: Rewrite the docstring paragraph**

Replace the paragraph beginning `UNMET PREREQUISITE:` through `in.` with:

```
PREREQUISITE: the join needs slates built from serving-emitted events. With
RECSYS_GRPO_EMIT_EVENTS=true the retrieval service publishes its own impressions and feedback to
Kafka carrying the same requestId it writes to the replay buffer, so the slate log and the replay
share one id. Slates produced only by the Python producers mint their own
`f"req_{uuid4().hex[:12]}"` ids and can never join. The printed "slate request_ids matched to a
replay requestId" count is the number that says which case you are in.
```

- [ ] **Step 4: Rewrite the zero-pair error text**

Replace the lines from `"A TOTAL join failure (0 matched) means the two sides mint their ids independently: "` through `"serving-path change, outside this component.\n"` with:

```python
            "A TOTAL join failure (0 matched) means the slates were not built from serving-emitted "
            "events. The retrieval service publishes impressions and feedback carrying the replay's "
            "requestId only when RECSYS_GRPO_EMIT_EVENTS=true (default false); slates from the "
            "Python producers alone mint their own `req_...` ids and can never join.\n"
```

Keep the preceding figures and the trailing `"If ids DID match, ..."` sentence unchanged.

- [ ] **Step 5: Run the DPO test file**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -q`
Expected: all pass (35 tests).

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py
git commit -m "docs(dpo): state the emit-flag prerequisite instead of an unmet one"
```

---

### Task 3: README and analysis report

**Files:**
- Modify: `recsys-pipeline/README.md:1075-1083`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md:160-166`

**Interfaces:**
- Consumes: nothing.
- Produces: documentation only.

- [ ] **Step 1: Replace the README paragraph**

Replace the paragraph beginning `**Unmet prerequisite — this arm produces nothing on today's data.**` through `synthetic fixtures only.` with:

```
**Prerequisite — slates must come from serving-emitted events.** The join is on request id, and only
the retrieval service writes the same id to both sides: with `RECSYS_GRPO_EMIT_EVENTS=true` it
publishes its own impressions and feedback to Kafka carrying the `requestId` it also writes to the
replay buffer. The flag defaults to `false`, and slates produced only by the Python producers mint
their own `req_...` ids, so on a default deployment the join matches 0 rows and the CLI exits naming
the flag. The `slate request_ids matched to a replay requestId` line is the check.
```

- [ ] **Step 2: Replace the Analysis_Report sentences**

Replace

```
preference pairs drawn from the slate log. It does not run on today's data: the slate log's
`request_id` and the replay's `requestId` are minted by different generators (Python producer
versus Java serving path), so the join matches nothing and the CLI exits naming the mismatch —
see the README's DPO block for what would unblock it. When it does run, read its held-out pairwise
```

with

```
preference pairs drawn from the slate log. It needs slates built from serving-emitted events: with
`RECSYS_GRPO_EMIT_EVENTS=true` the retrieval service writes the same `requestId` to Kafka and to the
replay buffer, so the two sides join; producer-only slates never do, and the CLI exits naming the
flag. Read its held-out pairwise
```

- [ ] **Step 3: Confirm nothing still claims the prerequisite is unmet**

Run: `grep -rn "UNMET PREREQUISITE\|Unmet prerequisite\|does not run on today's data\|produces nothing on today" recsys-pipeline --include='*.py' --include='*.md' | grep -v '/target/'`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add recsys-pipeline/README.md recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md
git commit -m "docs: DPO prerequisite is the emit flag, not a missing serving change"
```

---

### Task 4: Full suites, records, memory, and PR

**Files:**
- Modify: `.superpowers/docs/specs/2026-09-12-dpo-prerequisite-met-design.md` (status and verification record)
- Modify: this plan (check boxes, record observations)
- Modify: `/Users/linghuang/.claude/projects/-Users-linghuang-Git-Recsys-Streaming-Pipeline/memory/project_requestid_namespaces_disjoint.md` (outside the repository)

**Interfaces:**
- Consumes: the finished work from Tasks 1-3.
- Produces: a PR against `master` from `fix/dpo-prerequisite-met`.

- [ ] **Step 1: Run the Python modeling suite and the retrieval service's Maven suite**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling -q`
Expected: 501 passed.

Run: `cd recsys-pipeline/services/java-retrieval-service && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn test 2>&1 | grep -E "Tests run:.*Fail|BUILD"` in the background if it exceeds a few minutes.
Expected: `BUILD SUCCESS`; `UserProfileIntegrationTest` skips as always (Testcontainers blocked on Docker 29). If unrelated tests fail, confirm they fail on `origin/master` too before reporting.

- [ ] **Step 2: Update the spec status and verification record, tick this plan, and commit**

Set the spec's status line to `Implemented and verified; PR pending` and add a `## Verification record` section with the Maven counts, the pytest count, and the grep result.

```bash
git add .superpowers/docs/specs/2026-09-12-dpo-prerequisite-met-design.md .superpowers/docs/plans/2026-09-12-dpo-prerequisite-met.md
git commit -m "docs: record DPO prerequisite verification"
```

- [ ] **Step 3: Update the memory note**

In the memory file named above, replace the final paragraph beginning `**Fix now designed (2026-08-30):**` with a paragraph stating that PR #208 (impressions) and PRs #210/#216 (feedback) shipped the emission behind `RECSYS_GRPO_EMIT_EVENTS` (default false), that the identity is pinned by `replayContextAndEmittedImpressionShareTheServingRequestId`, and that the DPO join is reachable on emitted data as of this change's PR.

- [ ] **Step 4: Open the PR**

```bash
git push -u origin fix/dpo-prerequisite-met
gh pr create --base master --title "docs(dpo): prerequisite is met behind the emit flag; pin the shared request id" --body "$(cat <<'EOF'
## Summary
- The DPO arm's docs and CLI still said it "produces nothing on today's data" because the slate log and replay buffer minted request ids independently. That has not been true since PR #208 (serving emits impressions) and PRs #210/#216 (serving emits feedback): both sinks now carry `request.requestId()` when `RECSYS_GRPO_EMIT_EVENTS=true`.
- Add a serving-side contract test asserting one served request writes the same id to the replay context and the emitted Avro event.
- Rewrite the CLI docstring and zero-pair error, the README paragraph, and the analysis report to name the flag as the prerequisite. Flag default stays `false`.

## Test plan
- [x] `MovieLensServingSideEffectsTest`: 3 tests pass under JDK 17.
- [x] `test_post_training_dpo.py` passes with the updated error-text assertions.
- [x] Full Python modeling suite: <fill from Step 1>.
- [x] Retrieval service Maven suite: <fill from Step 1>.

Spec: `.superpowers/docs/specs/2026-09-12-dpo-prerequisite-met-design.md`
Plan: `.superpowers/docs/plans/2026-09-12-dpo-prerequisite-met.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Then record the PR link in the spec's status line and commit it with `docs: link DPO prerequisite PR`.

---

## Self-review

- Spec coverage: contract test (Task 1), CLI text and assertions (Task 2), README and report (Task 3), suites, records, memory, PR (Task 4). Acceptance criteria 1-5 map to Task 1, Task 1 Step 3, Task 2 Step 5, Task 3 Step 3, Task 4 Step 1.
- Placeholders: the PR body has two `<fill from Step 1>` slots supplied by Task 4 Step 1.
- Names used across tasks: `replayContextAndEmittedImpressionShareTheServingRequestId`, `RECSYS_GRPO_EMIT_EVENTS`, `n_slate_request_ids_matched` are consistent with the code.
