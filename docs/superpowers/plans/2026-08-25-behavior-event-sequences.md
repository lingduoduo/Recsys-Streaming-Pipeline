# Behavioral Event Sequences Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist validated search, result-view, detail-view, and click actions as one ordered behavior sequence and optionally hydrate item-bearing behavior into serving history.

**Architecture:** Introduce a versioned Avro v3 writer while retaining v1/v2 readers, then generalize the existing Spark click pipeline into action-aware behavior validation and dual-write migration. Add a focused Java hydrator that reads the behavior sequence in off/shadow/on modes and merges item-bearing actions into watched history without changing public query types.

**Tech Stack:** Avro single-object encoding, Python 3/pytest/fastavro, Scala 2.12/Spark 3.5/ScalaTest, Java 17/Spring Boot/JUnit 5, Redis columnar sequences.

**Spec:** `docs/superpowers/specs/2026-08-25-behavior-event-sequences-design.md`

## Global Constraints

- Preserve v1 and v2 decoding; all new canonical writes use v3.
- Keep existing recommendation-feedback events valid and ignored by `UserEventStreamingJob`.
- Continue writing legacy `click` sequences while adding `behavior` sequences.
- Serving defaults to `off`; missing or failed behavior reads retain legacy history.
- Do not log query text or add a new Redis data structure.
- Use Java 17 explicitly: `JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home`.

## File Structure

- `recsys-pipeline/schemas/recsys-event-v3.avsc`: new canonical writer contract.
- `recsys-pipeline/services/python-modeling/event_avro.py`: v3 default, v1/v2 catalog, action-aware writer validation.
- `recsys-pipeline/services/python-modeling/producer.py`: deterministic search-to-click trace generator.
- `recsys-pipeline/services/spark-streaming-job/.../EventAvroCodec.scala`: v3 default and v1/v2/v3 reader catalog.
- `recsys-pipeline/services/spark-streaming-job/.../UserEventStreamingJob.scala`: behavioral gate, dedupe, projection, and dual writes.
- `recsys-pipeline/services/{spark-streaming-job,java-retrieval-service}/.../sequence-schema.json`: cross-language `behavior` kind contract.
- `recsys-pipeline/services/java-retrieval-service/.../BehaviorSequencesQueryHydrator.java`: behavior-to-history adapter.
- `recsys-pipeline/services/java-retrieval-service/.../SequenceConfig.java`: hydrator bean wiring.
- Existing module tests: contract and regression coverage adjacent to each implementation.

---

### Task 1: Version the canonical event contract

**Files:**
- Create: `recsys-pipeline/schemas/recsys-event-v3.avsc`
- Modify: `recsys-pipeline/services/python-modeling/event_avro.py`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventAvroCodec.scala`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_event_avro.py`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/event/EventAvroCodecSpec.scala`

**Interfaces:**
- Produces: v3 fields `query_id`, `query_text`, `result_set_id`, `referrer`, `view_kind`, `view_duration_ms`; nullable `item_id`.
- Produces: Python `validate_required(event: Mapping[str, object]) -> None` with action-aware item requirements.
- Produces: Scala `EventAvroCodec.schema` as v3 and `catalog` containing v1/v2/v3 fingerprints.

- [ ] **Step 1: Write failing Python contract tests**

Add tests that assert the default schema is v3, a search with `item_id=None` round-trips, a click without an item is rejected, v2 resolves into the v3 shape, and all six context fields round-trip:

```python
def test_search_without_item_round_trips_in_v3():
    event = {
        "event_id": "search-1", "user_id": "u1", "item_id": None,
        "event_type": "search", "timestamp_ms": 1000,
        "query_id": "q1", "query_text": "space opera",
    }
    assert event_avro.decode_event(event_avro.encode_event(event))["query_id"] == "q1"

def test_item_bearing_behavior_requires_item():
    event = {"event_id": "click-1", "user_id": "u1", "item_id": None,
             "event_type": "click", "timestamp_ms": 1000}
    with pytest.raises(event_avro.EventValidationError, match="item_id"):
        event_avro.encode_event(event)
```

- [ ] **Step 2: Run Python tests and verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_event_avro.py`

Expected: FAIL because v3 does not exist and v2 rejects null `item_id`.

- [ ] **Step 3: Write failing Scala codec tests**

Extend `EventAvroCodecSpec` to assert v3 is the writer, all three schemas are cataloged, itemless search decodes, and itemless click fails required-field validation.

- [ ] **Step 4: Run Scala codec tests and verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt 'testOnly com.demo.event.EventAvroCodecSpec'`

Expected: FAIL because the codec still loads v2 and globally requires `item_id`.

- [ ] **Step 5: Implement Avro v3 and action-aware codecs**

Copy v2 to v3, make `item_id` nullable with default null, append the six nullable fields, switch both default schema paths to v3, retain v1/v2 in catalogs, and require `item_id` only for `result_view`, `detail_view`, `click`, and existing item-bearing feedback actions. Keep `event_id`, `user_id`, `event_type`, and `timestamp_ms` globally required.

```python
REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "timestamp_ms")
ITEM_REQUIRED_ACTIONS = frozenset({
    "impression", "exposure", "result_view", "detail_view", "click",
    "order", "purchase", "rating", "thumb_up", "thumb_down", "abandon",
})
```

- [ ] **Step 6: Verify GREEN**

Run both commands from Steps 2 and 4. Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/schemas recsys-pipeline/services/python-modeling/event_avro.py \
  recsys-pipeline/integration-tests/python_modeling/test_event_avro.py \
  recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventAvroCodec.scala \
  recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/event/EventAvroCodecSpec.scala
git commit -m "feat: add behavioral event schema v3"
```

### Task 2: Generate deterministic behavioral traces

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/producer.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_producer.py`

**Interfaces:**
- Consumes: v3 behavioral fields from Task 1.
- Produces: `make_search_journey(users, items) -> list[dict[str, object]]` ordered as search, result views, detail view, click.

- [ ] **Step 1: Write the failing journey test**

```python
def test_search_journey_preserves_shared_identity_and_order(monkeypatch):
    mod = load_producer_module()
    events = mod.make_search_journey(["user_1"], ["movie_1", "movie_2"])
    assert [e["event_type"] for e in events] == [
        "search", "result_view", "result_view", "detail_view", "click"
    ]
    assert events[0]["item_id"] is None
    assert {e["query_id"] for e in events} == {events[0]["query_id"]}
    assert {e["result_set_id"] for e in events[1:]} == {events[1]["result_set_id"]}
    assert [e["timestamp_ms"] for e in events] == sorted(e["timestamp_ms"] for e in events)
```

- [ ] **Step 2: Run test and verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_producer.py -k search_journey`

Expected: FAIL with missing `make_search_journey`.

- [ ] **Step 3: Implement the minimal journey generator**

Use one selected user, one session, one query ID, one result-set ID, stable positions, and monotonically increasing timestamps. Choose the first result as detail/click target so tests do not depend on randomness. Add `PRODUCER_MODE=search` routing in `main`.

- [ ] **Step 4: Verify GREEN and compatibility**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_producer.py integration-tests/python_modeling/test_event_avro.py`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/producer.py \
  recsys-pipeline/integration-tests/python_modeling/test_producer.py
git commit -m "feat: generate search behavior journeys"
```

### Task 3: Generalize Spark ingestion and dual-write sequences

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventSchemas.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceSchema.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/UserEventStreamingJob.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/RedisSink.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/resources/sequence-schema.json`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/UserEventStreamingJobSpec.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceSchemaSpec.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine/RedisSinkSpec.scala`

**Interfaces:**
- Produces: `SequenceSchema.KindBehavior = "behavior"`.
- Produces: `behavioralEvents(raw, watermarkDelay, batchId, reporter): DataFrame`.
- Produces: `buildBehaviorSequenceEvents(batch): DataFrame` and legacy `buildClickSequenceEvents(batch): DataFrame`.
- Search sequence sentinel: empty string in `item_id`; serving omits it.

- [ ] **Step 1: Write failing action-gate tests**

Cover valid itemless search, missing query text, missing result-set ID, valid detail view, itemless click, unknown feedback action ignored, and event-id dedupe. Assert rejection reason names exactly: `missing_search_query`, `missing_result_identity`, and `missing_behavior_item`. Add a pure popularity-count test proving search/view rows do not increment popularity and clicks do.

```scala
val kept = UserEventStreamingJob.behavioralEvents(decoded, "10 minutes").select("event_id").as[String].collect()
kept should contain theSameElementsAs Seq("search-ok", "detail-ok", "click-ok")
```

- [ ] **Step 2: Run gate tests and verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt 'testOnly com.demo.task.UserEventStreamingJobSpec'`

Expected: FAIL because `behavioralEvents` is absent and the global null-item gate drops searches.

- [ ] **Step 3: Implement behavioral filtering**

Extend `EventSchemas.baseFields` with the six v3 fields. Make `normalize` gate globally required user/event/timestamp fields, then filter to the four behavioral actions and apply action-specific `FieldGate` conditions before watermark dedupe.

- [ ] **Step 4: Write failing projection and shared-contract tests**

Assert a search becomes `kind=behavior`, `item_id=""`, `action=search`; item-bearing rows preserve IDs/actions; click rows appear once in behavior projection and once in legacy click projection; encoder ordering is timestamp ascending. Update both expected kinds to `rating`, `click`, `behavior`.

- [ ] **Step 5: Run projection tests and verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt 'testOnly com.demo.task.UserEventStreamingJobSpec com.demo.sequence.SequenceSchemaSpec'`

Expected: FAIL because `KindBehavior` and dual projections are absent.

- [ ] **Step 6: Implement dual-write projections and sinks**

Keep popularity counts click-only by making `RedisPopularitySink.counts` package-visible and filtering `event_type == "click"` with non-null `item_id` before grouping. Configure three durable sinks: popularity, `sequence:user-behavior`, and `sequence:user-click-legacy`. Use separate `SequenceBusinessSink` instances so their commit ledgers and identities remain independent.

- [ ] **Step 7: Verify GREEN**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt 'testOnly com.demo.task.UserEventStreamingJobSpec com.demo.sequence.SequenceSchemaSpec com.demo.sequence.SequenceEncoderSpec com.demo.engine.RedisSinkSpec'`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job
git commit -m "feat: persist unified behavior sequences"
```

### Task 4: Hydrate behavior into serving history

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/query_hydrators/BehaviorSequencesQueryHydrator.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/query_hydrators/BehaviorSequencesQueryHydratorTest.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/SequenceSchemaConstants.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/SequenceConfig.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/test/resources/sequence-schema.json`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/sequence/SequenceCodecTest.java`

**Interfaces:**
- Consumes: `SequenceClient.read(userId, "behavior", {item_id,ts,action}, 100, lookback)`.
- Produces: `BehaviorSequencesQueryHydrator(SequenceClient, String mode, int lookbackDays, int maxItems)`.
- On mode output: `watchedMovieIds = behaviorItemIds ++ legacyWatched`, stable de-duplicated and truncated.

- [ ] **Step 1: Write failing hydrator tests**

Use an in-memory recording `SequenceClient` returning aligned `item_id`, `ts`, and `action` columns. Test:

```java
assertEquals(List.of("m2", "m1", "legacy"),
    hydrator.hydrate(queryWithWatched("legacy")).watchedMovieIds());
```

The source rows should include an empty search item and duplicate clicks. Also assert off does not read, shadow reads but serves legacy, on omits empty items, on preserves newest-first order, read failure retains legacy, and max length is honored.

- [ ] **Step 2: Run test and verify RED**

Run: `cd recsys-pipeline && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn -pl services/java-retrieval-service -Dtest=BehaviorSequencesQueryHydratorTest test`

Expected: FAIL because the class and behavior constant do not exist.

- [ ] **Step 3: Implement the hydrator**

Follow `RatingSequencesQueryHydrator` mode parsing and failure handling. Request `item_id`, `ts`, and `action`; discard blank item IDs; retain only `result_view`, `detail_view`, and `click`; stable-dedupe behavior first, append existing watched history, and truncate to `maxItems`. `update` must change only `watchedMovieIds`.

- [ ] **Step 4: Wire the bean and shared schema constant**

Add `KIND_BEHAVIOR`, update both cross-language fixtures, and create the hydrator bean in `SequenceConfig` using existing sequence mode/lookback plus a constant `100` maximum for this first slice.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cd recsys-pipeline
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
  mvn -pl services/java-retrieval-service \
  -Dtest=BehaviorSequencesQueryHydratorTest,SequenceCodecTest,RatingSequencesQueryHydratorSequenceStoreTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service
git commit -m "feat: hydrate behavior sequence history"
```

### Task 5: Document rollout and verify the vertical slice

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`
- Modify: `recsys-pipeline/README.md`
- Test: relevant Python, Scala, and Java suites.

**Interfaces:**
- Documents: v3 fields/actions, `seq:{user}:behavior:{day}`, dual-write migration, `RECSYS_SEQUENCE_MODE` rollout, and privacy warning for query text.

- [ ] **Step 1: Update architecture and operator documentation**

Replace v2-as-current references with v3, list v1/v2 as accepted legacy writers, add the behavior sequence key and event validation table, and document `PRODUCER_MODE=search`. State that query text must not be logged and that unified replay/simulation are follow-ups.

- [ ] **Step 2: Run focused verification**

```bash
cd recsys-pipeline
pytest -q integration-tests/python_modeling/test_event_avro.py integration-tests/python_modeling/test_producer.py
(cd services/spark-streaming-job && sbt test)
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home \
  PATH=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home/bin:$PATH \
  mvn test
```

Expected: all commands exit 0. Docker-backed Java integration tests may report skipped when Docker is unavailable; no unit-test failures or errors are permitted.

- [ ] **Step 3: Check contracts and diff hygiene**

Run: `git diff --check && git status --short && git diff --stat origin/master...HEAD`

Expected: no whitespace errors; only planned source, test, spec, plan, and documentation files changed.

- [ ] **Step 4: Commit documentation and plan completion**

```bash
git add recsys-pipeline/README.md recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
git commit -m "docs: describe behavioral sequence rollout"
```

- [ ] **Step 5: Request code review and prepare PR**

Use `superpowers:requesting-code-review` against `origin/master`; fix all Critical and Important findings, rerun the full verification commands, then use `superpowers:finishing-a-development-branch` option 2 to push and create the PR.
