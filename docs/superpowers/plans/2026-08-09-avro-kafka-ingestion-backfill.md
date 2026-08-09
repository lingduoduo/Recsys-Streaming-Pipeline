# Avro Kafka Ingestion and Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a complete `recsys_events` vertical slice using Avro single-object encoding, explicit topic retention policies, lineage-preserving Parquet archival, dead-letter handling, and bounded replay to `recsys_events.backfill`.

**Architecture:** A checked-in Avro schema and matching Python/Scala codecs define the Kafka boundary. Spark decodes each Kafka record into either a valid canonical event or a structured failure, archives both outcomes before business transformations, and continues existing valid-record processing. A JSON topic catalog drives policy validation/provisioning, while a separate Python command replays bounded Parquet ranges through the same Avro codec.

**Tech Stack:** Apache Avro single-object encoding, Python 3 + `fastavro`, Kafka Python client, Scala 2.12, Spark 3.5.1 Structured Streaming, Apache Avro Java, ScalaTest, pytest, Parquet, Docker Compose Kafka.

## Global Constraints

- Implement only the `recsys_events` vertical slice; do not migrate derived Kafka topics.
- Use standard Avro single-object encoding: marker `0xC3 0x01`, little-endian 64-bit parsing-canonical-form fingerprint, then binary Avro datum.
- Do not add a vendor-specific schema-registry protocol or wire envelope.
- Treat date-partitioned Parquet as the local warehouse-table analogue.
- Publish replayed records only to `recsys_events.backfill`.
- Preserve `event_id` and `timestamp_ms` across archive and replay.
- Make missing Kafka offsets fail fast on the primary path.
- Do not claim one-million-message-per-second capacity without representative load testing.
- Preserve existing untracked `.ua/` directories and `recsys-pipeline/kafka.png`; they are outside this implementation.

## File Structure

- `recsys-pipeline/schemas/recsys-event-v1.avsc`: canonical Kafka event contract.
- `recsys-pipeline/services/python-modeling/event_avro.py`: schema loading, fingerprinting, validation, and Avro single-object encoding/decoding for Python tools.
- `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventAvroCodec.scala`: matching JVM codec and structured decode failures.
- `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/DecodedEventBatch.scala`: separates valid events from dead-letter rows while preserving Kafka metadata.
- `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/RawArchiveSink.scala`: idempotent raw archive and dead-letter Parquet writes.
- `recsys-pipeline/config/kafka-topics.json`: declarative live/backfill topic policies and capacity inputs.
- `recsys-pipeline/services/python-modeling/topic_policy.py`: policy parsing, storage calculation, and Kafka CLI argument generation.
- `recsys-pipeline/scripts/provision-kafka-topics.py`: validates the catalog and invokes `kafka-topics`/`kafka-configs`.
- `recsys-pipeline/services/python-modeling/archive_replay.py`: bounded Parquet selection, rate-limited Avro publication, and manifest creation.
- Existing producer, Spark source/engine/jobs, Compose, launch scripts, requirements, tests, and documentation are modified only where the vertical slice requires it.

---

### Task 1: Canonical Avro Schema and Python Single-Object Codec

**Files:**
- Create: `recsys-pipeline/schemas/recsys-event-v1.avsc`
- Create: `recsys-pipeline/services/python-modeling/event_avro.py`
- Modify: `recsys-pipeline/services/python-modeling/requirements.txt`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_event_avro.py`

**Interfaces:**
- Produces: `load_schema(path: Path | None = None) -> dict`
- Produces: `schema_fingerprint(schema: dict) -> int`
- Produces: `encode_event(event: Mapping[str, object], schema: dict | None = None) -> bytes`
- Produces: `decode_event(payload: bytes, catalog: Mapping[int, dict] | None = None) -> dict`
- Produces: `SchemaFingerprintError` and `EventValidationError`
- Encoding output is consumed by Task 2's Scala compatibility test and Task 3's producer.

- [ ] **Step 1: Write schema/codec tests that fail before the files exist**

```python
def test_single_object_round_trip():
    event = {
        "event_id": "e-1", "request_id": None, "session_id": None,
        "user_id": "u-1", "item_id": "i-1", "event_type": "click",
        "timestamp_ms": 1718400000000,
    }
    encoded = event_avro.encode_event(event)
    assert encoded[:2] == b"\xc3\x01"
    assert event_avro.decode_event(encoded)["event_id"] == "e-1"

def test_missing_required_field_is_rejected():
    with pytest.raises(event_avro.EventValidationError, match="event_id"):
        event_avro.encode_event({"user_id": "u", "item_id": "i", "event_type": "click", "timestamp_ms": 1})

def test_unknown_fingerprint_is_rejected():
    payload = b"\xc3\x01" + (7).to_bytes(8, "little") + b"bad"
    with pytest.raises(event_avro.SchemaFingerprintError, match="7"):
        event_avro.decode_event(payload)
```

- [ ] **Step 2: Run the focused test to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_event_avro.py`

Expected: FAIL during import because `event_avro.py` and the schema do not exist.

- [ ] **Step 3: Add the canonical schema and minimal codec**

The schema must use the record fullname `com.demo.event.RecSysEvent`, require `event_id`, `user_id`, `item_id`, `event_type`, and `timestamp_ms`, and give every optional field a `null`-first union with a `null` default. Feature maps use `{"type":"map","values":"string"}`.

```python
MAGIC = b"\xc3\x01"

def schema_fingerprint(schema: dict) -> int:
    canonical = fastavro.schema.to_parsing_canonical_form(schema)
    return int(fastavro.schema.fingerprint(canonical, "CRC-64-AVRO"), 16)

def encode_event(event, schema=None):
    writer_schema = schema or load_schema()
    validate_required(event)
    out = io.BytesIO()
    out.write(MAGIC)
    out.write(schema_fingerprint(writer_schema).to_bytes(8, "little"))
    fastavro.schemaless_writer(out, writer_schema, dict(event), strict=True)
    return out.getvalue()

def decode_event(payload, catalog=None):
    if len(payload) < 10 or payload[:2] != MAGIC:
        raise EventValidationError("invalid Avro single-object marker")
    fingerprint = int.from_bytes(payload[2:10], "little")
    schemas = catalog or {schema_fingerprint(load_schema()): load_schema()}
    if fingerprint not in schemas:
        raise SchemaFingerprintError(f"unknown schema fingerprint {fingerprint}")
    return fastavro.schemaless_reader(io.BytesIO(payload[10:]), schemas[fingerprint])
```

Add `fastavro>=1.9,<2` to `requirements.txt`.

- [ ] **Step 4: Run Python codec and existing producer tests**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_event_avro.py integration-tests/python_modeling/test_producer.py`

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```bash
git add recsys-pipeline/schemas/recsys-event-v1.avsc \
  recsys-pipeline/services/python-modeling/event_avro.py \
  recsys-pipeline/services/python-modeling/requirements.txt \
  recsys-pipeline/integration-tests/python_modeling/test_event_avro.py
git commit -m "feat: define recsys event Avro contract"
```

---

### Task 2: Scala Decoder and Cross-Language Compatibility Fixture

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/build.sbt`
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/resources/schemas/recsys-event-v1.avsc`
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventAvroCodec.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/resources/avro/python-recsys-event-v1.bin`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/event/EventAvroCodecSpec.scala`

**Interfaces:**
- Consumes: Task 1's Avro schema and exact wire contract.
- Produces: `EventAvroCodec.fingerprint: Long`
- Produces: `EventAvroCodec.decode(bytes: Array[Byte]): Either[DecodeFailure, GenericRecord]`
- Produces: `EventAvroCodec.encode(record: GenericRecord): Array[Byte]`
- Produces: sealed error codes `invalid_marker`, `unknown_fingerprint`, `corrupt_payload`, and `required_field`.

- [ ] **Step 1: Generate a deterministic Python fixture and write the failing Scala test**

Generate the fixture with a one-shot command that calls Task 1's `encode_event` for fixed event `e-cross-language` and writes the bytes to the test-resource path. The test must assert the committed fixture begins with `c3 01`, its fingerprint equals `SchemaNormalization.parsingFingerprint64(schema)`, and Scala decodes all required fields.

```scala
"EventAvroCodec" should "decode the Python single-object fixture" in {
  val bytes = Files.readAllBytes(Paths.get(getClass.getResource("/avro/python-recsys-event-v1.bin").toURI))
  val decoded = EventAvroCodec.decode(bytes).toOption.value
  decoded.get("event_id").toString shouldBe "e-cross-language"
  decoded.get("timestamp_ms") shouldBe 1718400000000L
}
```

- [ ] **Step 2: Run the Scala test to verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.event.EventAvroCodecSpec"`

Expected: FAIL because `EventAvroCodec` does not exist.

- [ ] **Step 3: Add Avro dependencies/resources and implement the codec**

Add Apache Avro `1.11.3` to `build.sbt`. Copy the canonical schema into the JVM resources and add a test that compares both checked-in schema files byte-for-byte after whitespace-normalized JSON parsing, preventing drift.

```scala
object EventAvroCodec {
  val Magic: Array[Byte] = Array(0xc3.toByte, 0x01.toByte)
  lazy val schema: Schema = new Schema.Parser().parse(
    getClass.getResourceAsStream("/schemas/recsys-event-v1.avsc"))
  lazy val fingerprint: Long = SchemaNormalization.parsingFingerprint64(schema)

  def decode(bytes: Array[Byte]): Either[DecodeFailure, GenericRecord] = {
    if (bytes == null || bytes.length < 10 || !bytes.take(2).sameElements(Magic))
      Left(DecodeFailure("invalid_marker", "invalid Avro single-object marker"))
    else if (ByteBuffer.wrap(bytes, 2, 8).order(ByteOrder.LITTLE_ENDIAN).getLong != fingerprint)
      Left(DecodeFailure("unknown_fingerprint", "writer schema is not in the local catalog"))
    else Try {
      val decoder = DecoderFactory.get.binaryDecoder(bytes, 10, bytes.length - 10, null)
      new GenericDatumReader[GenericRecord](schema).read(null, decoder)
    }.toEither.left.map(e => DecodeFailure("corrupt_payload", e.getMessage))
  }
}
```

- [ ] **Step 4: Run compatibility tests**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.event.EventAvroCodecSpec"`

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add recsys-pipeline/services/spark-streaming-job/build.sbt \
  recsys-pipeline/services/spark-streaming-job/src/main/resources/schemas/recsys-event-v1.avsc \
  recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventAvroCodec.scala \
  recsys-pipeline/services/spark-streaming-job/src/test/resources/avro/python-recsys-event-v1.bin \
  recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/event/EventAvroCodecSpec.scala
git commit -m "feat: decode Avro events in Spark jobs"
```

---

### Task 3: Migrate the Python Producers to Avro

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/producer.py`
- Modify: `recsys-pipeline/services/python-modeling/backfill_producer.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_producer.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_engagement_sim.py`

**Interfaces:**
- Consumes: `event_avro.encode_event(event) -> bytes` from Task 1.
- Produces: Kafka values that Task 2 decodes.
- Keeps existing string key serialization and event-generation functions unchanged.

- [ ] **Step 1: Add failing serializer tests**

```python
def test_kafka_value_serializer_emits_avro_single_object():
    mod = load_producer_module()
    payload = mod.serialize_event(mod.make_click_event(["u"], ["i"]))
    assert payload[:2] == b"\xc3\x01"
    assert event_avro.decode_event(payload)["event_type"] == "click"
```

Update the KafkaProducer stub to capture `value_serializer`, and assert `make_producer()` installs `serialize_event` rather than a JSON lambda.

- [ ] **Step 2: Run producer tests to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_producer.py integration-tests/python_modeling/test_engagement_sim.py`

Expected: FAIL because `serialize_event` does not exist and producer values are JSON.

- [ ] **Step 3: Replace JSON value serialization with the shared codec**

```python
from event_avro import encode_event

def serialize_event(event):
    return encode_event(event)

def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize_event,
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        # preserve existing reliability, batching, compression, and timeout options
    )
```

Remove the unused JSON import from `producer.py`. `backfill_producer.py` continues to reuse `make_producer`, so it automatically emits the same Avro contract.

- [ ] **Step 4: Run producer/backfill tests**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_event_avro.py integration-tests/python_modeling/test_producer.py integration-tests/python_modeling/test_engagement_sim.py`

Expected: PASS.

- [ ] **Step 5: Commit Task 3**

```bash
git add recsys-pipeline/services/python-modeling/producer.py \
  recsys-pipeline/services/python-modeling/backfill_producer.py \
  recsys-pipeline/integration-tests/python_modeling/test_producer.py \
  recsys-pipeline/integration-tests/python_modeling/test_engagement_sim.py
git commit -m "feat: publish recsys events as Avro"
```

---

### Task 4: Decode Kafka Records, Archive Valid Events, and Dead-Letter Failures

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/DecodedEventBatch.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/RawArchiveSink.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/Source.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/ExecutionEngine.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventParsing.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/UserEventStreamingJob.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine/SourceSpec.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/event/DecodedEventBatchSpec.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine/RawArchiveSinkSpec.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/UserEventStreamingJobSpec.scala`

**Interfaces:**
- Produces: `DecodedEventBatch.decode(rawKafka: DataFrame): DecodedEventFrames`
- Produces: `DecodedEventFrames(valid: DataFrame, deadLetters: DataFrame)`
- Produces: `RawArchiveSink.writeValid(df, batchId)` and `writeDeadLetters(df, batchId)`.
- `valid` contains canonical event columns plus Kafka lineage; existing jobs consume canonical columns and ignore lineage after archive.

- [ ] **Step 1: Write failing decode-split and archive tests**

Build a DataFrame with Kafka-shaped columns (`value`, `topic`, `partition`, `offset`, `timestamp`) containing one Task 2-encoded event, one unknown fingerprint, and one corrupt payload. Assert one valid row and two dead-letter rows with stable error codes.

```scala
val frames = DecodedEventBatch.decode(rawKafka)
frames.valid.select("event_id").as[String].collect() should contain only "e1"
frames.deadLetters.select("error_code").as[String].collect().toSet shouldBe
  Set("unknown_fingerprint", "corrupt_payload")
```

Archive tests write to `java.nio.file.Files.createTempDirectory`, assert valid data is stored below `date=2024-06-15`, and assert columns include Kafka coordinates, `schema_fingerprint`, and `archived_at`.

- [ ] **Step 2: Run focused Scala tests to verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.event.DecodedEventBatchSpec com.demo.engine.RawArchiveSinkSpec"`

Expected: FAIL because both classes are missing.

- [ ] **Step 3: Make the Kafka source preserve headers and fail on data loss**

Update `KafkaSource.read`:

```scala
.option("includeHeaders", "true")
.option("failOnDataLoss", "true")
```

Update `SourceSpec` to assert both options through the existing source-plan/config inspection pattern.

- [ ] **Step 4: Implement safe decoding and structured side outputs**

Use a serializable UDF that calls `EventAvroCodec.decode` and returns a struct containing all canonical fields, `schema_fingerprint`, `error_code`, and `error_detail`. Never allow a codec exception to escape the UDF. Preserve raw bytes and Kafka metadata until valid/error frames split.

```scala
final case class DecodedEventFrames(valid: DataFrame, deadLetters: DataFrame)

def decode(raw: DataFrame): DecodedEventFrames = {
  val decoded = raw.withColumn("decoded", decodeUdf(col("value")))
  val valid = decoded.filter(col("decoded.error_code").isNull)
    .select(col("decoded.event.*"), col("topic").as("kafka_topic"),
      col("partition").as("kafka_partition"), col("offset").as("kafka_offset"),
      col("timestamp").as("kafka_timestamp"), col("decoded.schema_fingerprint"))
  val errors = decoded.filter(col("decoded.error_code").isNotNull)
    .select(col("topic").as("kafka_topic"), col("partition").as("kafka_partition"),
      col("offset").as("kafka_offset"), col("timestamp").as("kafka_timestamp"),
      col("value").as("raw_value"), col("decoded.error_code"), col("decoded.error_detail"))
  DecodedEventFrames(valid, errors)
}
```

- [ ] **Step 5: Implement idempotent raw and dead-letter archive writes**

`RawArchiveSink` derives UTC `date` from `timestamp_ms`, adds `archived_at`, and writes each Spark `batchId` to a deterministic `_batches/<batchId>` staging/final directory. If that batch directory already exists, it treats the retry as complete instead of appending duplicates. Dead letters use ingestion date and a separate root.

- [ ] **Step 6: Integrate decoding and pre-transform archival into the engine**

Extend `ExecutionEngine.run` with a `decode: DataFrame => DecodedEventFrames` boundary and raw/dead-letter sinks. In `foreachBatch`, persist decoded valid/error frames, write archive outputs first, then invoke existing stages and sinks only on valid events; unpersist both frames in `finally`.

Keep a compatibility overload for existing non-Avro jobs so derived-topic consumers remain unchanged.

- [ ] **Step 7: Migrate only `recsys_events` consumers**

Configure `OnlineJoinerStreamingJob` and `UserEventStreamingJob` to use the Avro decode path and environment variables:

```text
RECSYS_EVENT_ARCHIVE_PATH=/tmp/spark-recsys/recsys-events-archive
RECSYS_EVENT_DEAD_LETTER_PATH=/tmp/spark-recsys/recsys-events-dead-letter
```

Replace `EventParsing.fromJson` only in these two jobs. Do not change consumers of derived JSON topics.

- [ ] **Step 8: Run engine and migrated-job tests**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.event.*Spec com.demo.engine.*Spec com.demo.process.OnlineJoinerStreamingJobSpec com.demo.task.UserEventStreamingJobSpec"`

Expected: PASS.

- [ ] **Step 9: Commit Task 4**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event \
  recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine \
  recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala \
  recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/UserEventStreamingJob.scala \
  recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/event \
  recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine \
  recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala \
  recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/UserEventStreamingJobSpec.scala
git commit -m "feat: archive and validate Avro Kafka events"
```

---

### Task 5: Declarative Topic Policies and Provisioning

**Files:**
- Create: `recsys-pipeline/config/kafka-topics.json`
- Create: `recsys-pipeline/services/python-modeling/topic_policy.py`
- Create: `recsys-pipeline/scripts/provision-kafka-topics.py`
- Modify: `recsys-pipeline/docker-compose.yml`
- Modify: `recsys-pipeline/scripts/run-streaming-job.sh`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_topic_policy.py`
- Create: `recsys-pipeline/integration-tests/test_provision_kafka_topics.py`
- Modify: `recsys-pipeline/integration-tests/test_run_streaming_job_topic_bootstrap.sh`

**Interfaces:**
- Produces: `TopicPolicy` dataclass.
- Produces: `required_storage_bytes(policy: TopicPolicy) -> int`.
- Produces: `validate_policy(policy: TopicPolicy) -> None`.
- Produces: `create_args(policy) -> list[str]` and `config_args(policy) -> list[str]`.

- [ ] **Step 1: Write failing policy-calculation tests**

```python
def test_required_storage_includes_replication_and_overhead():
    policy = TopicPolicy("events", 12, 3, 1_000_000, 500, 86_400_000,
                         200_000_000_000_000, 1.10, "delete", "recsys-event", "abc")
    assert required_storage_bytes(policy) == 1_000_000 * 500 * 86_400 * 3 * 1.10

def test_budget_overrun_is_rejected():
    with pytest.raises(ValueError, match="storage budget"):
        validate_policy(dataclasses.replace(policy, retention_bytes=1))
```

Also test exact `kafka-topics --create` and `kafka-configs --alter` argument arrays for both topics.

- [ ] **Step 2: Run policy tests to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_topic_policy.py integration-tests/test_provision_kafka_topics.py`

Expected: FAIL because the policy module/script/catalog do not exist.

- [ ] **Step 3: Add catalog and policy implementation**

The catalog has exactly two entries: `recsys_events` and `recsys_events.backfill`. Values are environment-overridable by the provisioning script, but committed defaults must be safe for the single-broker development stack and must not pretend to support production scale.

```python
def required_storage_bytes(p):
    days = p.retention_ms / 86_400_000
    return math.ceil(p.messages_per_second * p.average_record_bytes * 86_400
                     * days * p.replication_factor * p.overhead_factor)

def validate_policy(p):
    if min(p.partitions, p.replication_factor, p.messages_per_second,
           p.average_record_bytes, p.retention_ms, p.retention_bytes) <= 0:
        raise ValueError(f"{p.name}: numeric policy values must be positive")
    if required_storage_bytes(p) > p.retention_bytes:
        raise ValueError(f"{p.name}: required storage exceeds storage budget")
```

- [ ] **Step 4: Implement idempotent provisioning and disable auto-creation**

The script validates every policy before invoking Kafka. For each topic it runs `kafka-topics --create --if-not-exists`, then `kafka-configs --alter --add-config cleanup.policy=...,retention.ms=...,retention.bytes=...`. It exits before running commands if any policy is invalid.

Set `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` in Compose. Replace the ad hoc single-topic creation in `run-streaming-job.sh` with the provisioning command. Update shell tests to assert provisioning precedes `spark-submit`.

- [ ] **Step 5: Run policy, script, and Compose tests**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_topic_policy.py integration-tests/test_provision_kafka_topics.py integration-tests/test_docker_compose_resilience.py && bash integration-tests/test_run_streaming_job_topic_bootstrap.sh`

Expected: PASS.

- [ ] **Step 6: Commit Task 5**

```bash
git add recsys-pipeline/config/kafka-topics.json \
  recsys-pipeline/services/python-modeling/topic_policy.py \
  recsys-pipeline/scripts/provision-kafka-topics.py \
  recsys-pipeline/docker-compose.yml recsys-pipeline/scripts/run-streaming-job.sh \
  recsys-pipeline/integration-tests/python_modeling/test_topic_policy.py \
  recsys-pipeline/integration-tests/test_provision_kafka_topics.py \
  recsys-pipeline/integration-tests/test_run_streaming_job_topic_bootstrap.sh \
  recsys-pipeline/integration-tests/test_docker_compose_resilience.py
git commit -m "feat: provision Kafka topics from retention policy"
```

---

### Task 6: Bounded Parquet-to-Kafka Replay

**Files:**
- Create: `recsys-pipeline/services/python-modeling/archive_replay.py`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_archive_replay.py`
- Create: `recsys-pipeline/scripts/run-archive-replay.sh`
- Modify: `recsys-pipeline/integration-tests/test_service_scripts.py`

**Interfaces:**
- Produces: `ReplayConfig` with archive path, inclusive start date, exclusive end date, max rows, override flag, records/sec, bootstrap servers, and fixed target topic.
- Produces: `select_archive(config) -> Iterable[dict]`.
- Produces: `run_replay(config, producer_factory, clock, sleeper) -> ReplayManifest`.
- Consumes: Task 1's `encode_event` and Task 4's archive columns.

- [ ] **Step 1: Write replay safety tests**

```python
def test_target_topic_is_fixed():
    assert ReplayConfig.target_topic == "recsys_events.backfill"

def test_row_limit_blocks_publish(tmp_path):
    write_archive(tmp_path, events=3)
    with pytest.raises(ReplayLimitError, match="3 rows exceeds max_rows=2"):
        run_replay(config(tmp_path, max_rows=2), fake_producer, fake_clock, fake_sleep)

def test_replay_preserves_identity_and_writes_manifest(tmp_path):
    result = run_replay(config(tmp_path), fake_producer, fake_clock, fake_sleep)
    decoded = event_avro.decode_event(fake_producer.sent[0].value)
    assert decoded["event_id"] == "e-1"
    assert decoded["timestamp_ms"] == 1718400000000
    assert result.status == "completed"
    assert result.target_topic == "recsys_events.backfill"
```

Test inclusive start/exclusive end partition selection and injected-clock rate limiting without real sleeps.

- [ ] **Step 2: Run replay tests to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_archive_replay.py`

Expected: FAIL because `archive_replay.py` does not exist.

- [ ] **Step 3: Implement bounded selection, publishing, and manifests**

Use `pyarrow.dataset` to filter Parquet partitions and project only canonical event fields plus `schema_fingerprint`. Validate date order and positive `max_rows`/rate before reading. Count the bounded selection before creating the producer. Re-encode each event with Task 1's codec, key by `request_id` or `user_id`, and use a monotonic deadline limiter.

Write the manifest atomically as `<manifest-dir>/<run-id>.json.tmp` followed by `os.replace` to `<run-id>.json`, updating status to `completed` or `failed` in a `finally` path.

- [ ] **Step 4: Add the operator wrapper and script tests**

`run-archive-replay.sh` changes to the pipeline root, checks required environment variables, and invokes `archive_replay.py` with explicit flags. Extend `test_service_scripts.py` to assert missing bounds fail before Python starts and that configured bounds/rate/max rows become exact arguments.

- [ ] **Step 5: Run replay and script tests**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_archive_replay.py integration-tests/test_service_scripts.py`

Expected: PASS.

- [ ] **Step 6: Commit Task 6**

```bash
git add recsys-pipeline/services/python-modeling/archive_replay.py \
  recsys-pipeline/integration-tests/python_modeling/test_archive_replay.py \
  recsys-pipeline/scripts/run-archive-replay.sh \
  recsys-pipeline/integration-tests/test_service_scripts.py
git commit -m "feat: replay archived events with safety bounds"
```

---

### Task 7: Integration Coverage and Operations Documentation

**Files:**
- Create: `recsys-pipeline/integration-tests/test_avro_kafka_round_trip.py`
- Modify: `recsys-pipeline/README.md`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`
- Modify: `recsys-pipeline/scripts/run-data-pipeline.sh`

**Interfaces:**
- Consumes all prior task interfaces.
- Produces an opt-in integration test selected by `RUN_KAFKA_INTEGRATION=1`.

- [ ] **Step 1: Write the opt-in round-trip test**

The test skips unless `RUN_KAFKA_INTEGRATION=1`. When enabled, it provisions both topics, publishes a fixed Avro record, runs the bounded Spark ingestion fixture, verifies the archived Parquet lineage, invokes replay, and consumes the same `event_id` from `recsys_events.backfill`.

```python
@pytest.mark.skipif(os.getenv("RUN_KAFKA_INTEGRATION") != "1", reason="opt-in Kafka integration")
def test_avro_archive_and_replay_round_trip(tmp_path):
    event_id = publish_fixed_avro_event("recsys_events")
    run_bounded_ingestion(tmp_path)
    assert archived_event(tmp_path, event_id)["kafka_topic"] == "recsys_events"
    run_archive_replay(tmp_path)
    assert consume_event_id("recsys_events.backfill") == event_id
```

- [ ] **Step 2: Run once to verify it is safely skipped by default**

Run: `cd recsys-pipeline && pytest -q integration-tests/test_avro_kafka_round_trip.py`

Expected: one SKIPPED test, no broker required.

- [ ] **Step 3: Document schema, provisioning, archive, dead letters, and replay**

Document exact commands and environment variables, including:

```bash
python scripts/provision-kafka-topics.py --bootstrap-server localhost:9092
RECSYS_EVENT_ARCHIVE_PATH=/data/recsys-events \
RECSYS_EVENT_DEAD_LETTER_PATH=/data/recsys-events-dead-letter \
  ./scripts/run-streaming-job.sh
REPLAY_START_DATE=2026-08-01 REPLAY_END_DATE=2026-08-02 \
REPLAY_MAX_ROWS=100000 REPLAY_RECORDS_PER_SECOND=5000 \
  ./scripts/run-archive-replay.sh
```

State that the committed development policy is not a 1M msg/s capacity claim. Include the storage formula, schema-evolution rules, dead-letter error codes, replay manifest location, and how a consumer opts into the backfill topic.

- [ ] **Step 4: Update the local pipeline wrapper**

Make `run-data-pipeline.sh` run topic provisioning after Kafka health succeeds and before starting producers/consumers. Preserve its existing cleanup and service behavior.

- [ ] **Step 5: Run the complete relevant suites**

Run:

```bash
cd recsys-pipeline
pytest -q integration-tests/python_modeling/test_event_avro.py \
  integration-tests/python_modeling/test_producer.py \
  integration-tests/python_modeling/test_topic_policy.py \
  integration-tests/python_modeling/test_archive_replay.py \
  integration-tests/test_provision_kafka_topics.py \
  integration-tests/test_service_scripts.py \
  integration-tests/test_docker_compose_resilience.py \
  integration-tests/test_avro_kafka_round_trip.py
bash integration-tests/test_run_streaming_job_topic_bootstrap.sh
cd services/spark-streaming-job
sbt test
```

Expected: Python and shell tests PASS, the opt-in integration test SKIPS unless enabled, and all Scala tests PASS.

- [ ] **Step 6: Run static repository checks**

Run:

```bash
git diff --check
rg -n 'fromJson\(rawKafka|failOnDataLoss.*false|AUTO_CREATE_TOPICS_ENABLE.*true' \
  recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala \
  recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/UserEventStreamingJob.scala \
  recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/Source.scala \
  recsys-pipeline/docker-compose.yml
```

Expected: `git diff --check` produces no output; the forbidden-pattern search produces no matches.

- [ ] **Step 7: Commit Task 7**

```bash
git add recsys-pipeline/integration-tests/test_avro_kafka_round_trip.py \
  recsys-pipeline/README.md \
  recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md \
  recsys-pipeline/scripts/run-data-pipeline.sh
git commit -m "docs: operate Avro ingestion and archive replay"
```

---

### Task 8: Final Verification and Review Gate

**Files:**
- Verify only; modify files solely to fix failures attributable to Tasks 1–7.

**Interfaces:**
- Confirms the complete vertical slice and all success criteria from the approved design.

- [ ] **Step 1: Run clean focused verification**

Run the complete commands from Task 7 Step 5 again from a fresh shell. Record exact pass/skip counts.

- [ ] **Step 2: Inspect scope and generated artifacts**

Run:

```bash
git status --short
git diff HEAD~7..HEAD --stat
git log -8 --oneline
```

Confirm no `.ua/`, `kafka.png`, build output, Parquet data, replay manifests, credentials, or local environment files were committed.

- [ ] **Step 3: Request code review**

Invoke `superpowers:requesting-code-review` against the implementation range. Require review of wire compatibility, retry/idempotency behavior, replay safety, policy arithmetic, and whether derived JSON topics remained untouched.

- [ ] **Step 4: Address accepted findings and re-run affected plus full tests**

Use `superpowers:receiving-code-review` before applying reviewer feedback. For every accepted correction, add or tighten a failing test first, implement the minimal fix, then repeat Task 7 Step 5.

- [ ] **Step 5: Finalize the branch**

Invoke `superpowers:verification-before-completion`, then `superpowers:finishing-a-development-branch`. Do not claim completion unless the fresh verification output supports it.
