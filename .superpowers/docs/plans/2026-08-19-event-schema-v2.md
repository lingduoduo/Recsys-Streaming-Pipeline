# Event Schema v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The canonical event declares `surface`, `locale`, `timezone`, and `device` as typed fields and carries `thumb_up` / `thumb_down` / `abandon` as event types, all landing in `training_samples`, with v1 events still decoding.

**Architecture:** Add a second Avro schema file rather than editing the first, and teach both codecs to resolve the writer schema by fingerprint so v1 and v2 records coexist. Fix the joiner's feedback attribution to be per-field before adding event types that would otherwise erase earlier signals. Move `country` to `user_features` (a user attribute) while `device` becomes a typed field (request context), and update the two consumers that read those keys by their old names.

**Tech Stack:** Avro single-object encoding (fastavro on Python, `org.apache.avro` on Scala), Spark Structured Streaming (Scala 2.12 / Spark 3.5), pytest, ScalaTest FlatSpec, Maven.

**Spec:** [.superpowers/docs/specs/2026-08-19-event-schema-v2-design.md](../specs/2026-08-19-event-schema-v2-design.md)

## Global Constraints

- Java build commands must use JDK 17: `JAVA_HOME=$(/usr/libexec/java_home -v 17)`. The default JDK 25 aborts every Spark-session test with a misleading `getSubject` error.
- Scala tests run from `recsys-pipeline/services/spark-streaming-job`, which is an **sbt** module (`build.sbt`, no `pom.xml`): `JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch test`. Python tests run from the repository root. (The `mvn` commands elsewhere in this plan were wrong; only `services/java-retrieval-service` is Maven, and this plan does not touch it.)
- The v2 fingerprint is **`0xAF86ABE880FE4BB3`** (`af86abe880fe4bb3` as the lowercase hex string stored in config). This value is fixed by the exact field list and order in Task 1. If you change that list, recompute with:
  `python3 -c "import json,fastavro;s=json.load(open('recsys-pipeline/schemas/recsys-event-v2.avsc'));print(format(int.from_bytes(bytes.fromhex(fastavro.schema.fingerprint(fastavro.schema.to_parsing_canonical_form(s),'CRC-64-AVRO')),'little'),'016x'))"`
- The Avro schema file is duplicated: `recsys-pipeline/schemas/` (read by Python) and `recsys-pipeline/services/spark-streaming-job/src/main/resources/schemas/` (read by Scala). Both copies must be written and kept byte-identical.
- All new schema fields are `["null", "string"]` with `"default": null`. Never make one required.
- Vocabularies (`surface`, `locale`, `timezone`, `device`) are producer conventions, not Avro enums. An unrecognized value must flow through as data, never fail a decode.
- Boundary rule from the spec: request context is a typed field; user attributes stay in `user_features`; `context_features` is for ad-hoc experiment keys only.

---

### Task 1: Schema v2 and the Python codec catalog

**Files:**
- Create: `recsys-pipeline/schemas/recsys-event-v2.avsc`
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/resources/schemas/recsys-event-v2.avsc` (byte-identical copy)
- Modify: `recsys-pipeline/services/python-modeling/event_avro.py`
- Modify: `recsys-pipeline/services/python-modeling/archive_replay.py:438-461`
- Modify: `recsys-pipeline/config/kafka-topics.json:15,29`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_event_avro.py`, `test_archive_replay.py`

**Interfaces:**
- Produces: `event_avro.DEFAULT_SCHEMA_PATH` (now v2), `event_avro.LEGACY_SCHEMA_PATHS: tuple[Path, ...]`, `event_avro.load_catalog() -> dict[int, dict]`. `archive_replay._validate_archive_fingerprints` accepts every fingerprint in that catalog. Task 7's producers encode through the unchanged `encode_event(event)` signature; Task 2 mirrors this catalog in Scala.

- [ ] **Step 1: Write the failing test**

Add to `recsys-pipeline/integration-tests/python_modeling/test_event_avro.py`:

```python
def test_v2_is_the_default_writer_schema():
    """Fails if producers still encode v1 after the bump."""
    schema = event_avro.load_schema()
    names = [field["name"] for field in schema["fields"]]

    assert names[-4:] == ["surface", "locale", "timezone", "device"]
    assert event_avro.schema_fingerprint(schema) == 0xAF86ABE880FE4BB3


def test_v1_payload_decodes_into_the_v2_shape():
    """Fails if a v1 record dead-letters instead of resolving through the catalog."""
    v1 = event_avro.load_schema(event_avro.LEGACY_SCHEMA_PATHS[0])
    payload = event_avro.encode_event(
        {
            "event_id": "e-legacy",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "click",
            "timestamp_ms": 1718400000000,
        },
        v1,
    )

    decoded = event_avro.decode_event(payload)

    assert decoded["event_id"] == "e-legacy"
    assert decoded["surface"] is None
    assert decoded["locale"] is None
    assert decoded["timezone"] is None
    assert decoded["device"] is None


def test_v2_round_trips_the_new_context_fields():
    payload = event_avro.encode_event(
        {
            "event_id": "e-v2",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "impression",
            "timestamp_ms": 1718400000000,
            "surface": "home_feed",
            "locale": "en-US",
            "timezone": "America/New_York",
            "device": "ios",
        }
    )

    decoded = event_avro.decode_event(payload)

    assert decoded["surface"] == "home_feed"
    assert decoded["locale"] == "en-US"
    assert decoded["timezone"] == "America/New_York"
    assert decoded["device"] == "ios"


def test_unknown_fingerprint_is_still_rejected():
    """Fails if widening the catalog accidentally accepts any fingerprint."""
    payload = event_avro.encode_event(
        {
            "event_id": "e-1",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "click",
            "timestamp_ms": 1718400000000,
        }
    )
    corrupted = payload[:2] + (0xDEADBEEF).to_bytes(8, "little") + payload[10:]

    with pytest.raises(event_avro.SchemaFingerprintError):
        event_avro.decode_event(corrupted)
```

Add to `recsys-pipeline/integration-tests/python_modeling/test_archive_replay.py`:

```python
def test_archive_accepts_every_catalogued_fingerprint(tmp_path):
    """Fails while the gate compares against one schema: a v1 archive must stay replayable."""
    import event_avro
    from archive_replay import _validate_archive_fingerprints

    catalog = event_avro.load_catalog()
    assert len(catalog) == 2, "the catalog should hold v1 and v2"

    v1_fingerprint = event_avro.schema_fingerprint(
        event_avro.load_schema(event_avro.LEGACY_SCHEMA_PATHS[0]))
    archive = _archive_with_fingerprints(tmp_path, [v1_fingerprint])

    assert _validate_archive_fingerprints(archive, None) == (v1_fingerprint,)
```

Build `_archive_with_fingerprints` with the same Parquet-writing helper the existing tests in
that file already use for `schema_fingerprint` columns (see the fixture around line 148 that
writes `"schema_fingerprint": SCHEMA_FINGERPRINT`); reuse it rather than writing a second one.

Also update the existing `test_standard_single_object_encoding_layout` (around line 30), which pins the v1 fingerprint. It must now pin v1 explicitly rather than relying on the default:

```python
    schema = event_avro.load_schema(event_avro.LEGACY_SCHEMA_PATHS[0])
    expected_fingerprint = 0x225B275F487979AB
```

The rest of that test (the `payload[:10]` byte assertion and the explicit `{expected_fingerprint: schema}` catalog) already passes `schema` explicitly and needs no other change.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_event_avro.py -q`
Expected: FAIL — `recsys-event-v2.avsc` does not exist, so `load_schema()` raises `FileNotFoundError`, and `LEGACY_SCHEMA_PATHS` is undefined.

- [ ] **Step 3: Write minimal implementation**

Create `recsys-pipeline/schemas/recsys-event-v2.avsc` as a copy of v1 with four fields appended. Field order is what fixes the fingerprint — append, never insert:

```json
{
  "type": "record",
  "name": "RecSysEvent",
  "namespace": "com.demo.event",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "request_id", "type": ["null", "string"], "default": null},
    {"name": "session_id", "type": ["null", "string"], "default": null},
    {"name": "user_id", "type": "string"},
    {"name": "item_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "timestamp_ms", "type": "long"},
    {"name": "position", "type": ["null", "int"], "default": null},
    {"name": "user_features", "type": ["null", {"type": "map", "values": "string"}], "default": null},
    {"name": "item_features", "type": ["null", {"type": "map", "values": "string"}], "default": null},
    {"name": "context_features", "type": ["null", {"type": "map", "values": "string"}], "default": null},
    {"name": "model_version", "type": ["null", "string"], "default": null},
    {"name": "policy_version", "type": ["null", "string"], "default": null},
    {"name": "algorithm_version", "type": ["null", "string"], "default": null},
    {"name": "rating", "type": ["null", "double"], "default": null},
    {"name": "negative_feedback_reason", "type": ["null", "string"], "default": null},
    {"name": "dwell_millis", "type": ["null", "long"], "default": null},
    {"name": "completion_rate", "type": ["null", "double"], "default": null},
    {"name": "published_at", "type": ["null", "long"], "default": null},
    {"name": "new_release", "type": ["null", "boolean"], "default": null},
    {"name": "filter_reason", "type": ["null", "string"], "default": null},
    {"name": "unsafe_label", "type": ["null", "boolean"], "default": null},
    {"name": "surface", "type": ["null", "string"], "default": null},
    {"name": "locale", "type": ["null", "string"], "default": null},
    {"name": "timezone", "type": ["null", "string"], "default": null},
    {"name": "device", "type": ["null", "string"], "default": null}
  ]
}
```

Copy it to the Scala resource path:

```bash
cp recsys-pipeline/schemas/recsys-event-v2.avsc \
   recsys-pipeline/services/spark-streaming-job/src/main/resources/schemas/recsys-event-v2.avsc
```

In `event_avro.py`, replace the `DEFAULT_SCHEMA_PATH` assignment with:

```python
_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
DEFAULT_SCHEMA_PATH = _SCHEMA_DIR / "recsys-event-v2.avsc"
LEGACY_SCHEMA_PATHS: tuple[Path, ...] = (_SCHEMA_DIR / "recsys-event-v1.avsc",)
```

Add the catalog builder after `schema_fingerprint`:

```python
def load_catalog() -> dict[int, dict]:
    """Every writer schema this decoder accepts, keyed by fingerprint.

    A record written before a schema bump is still a valid record; keeping the older
    writer schemas here is what lets it resolve into the current reader shape instead
    of dead-lettering as an unknown fingerprint.
    """
    catalog: dict[int, dict] = {}
    for path in (*LEGACY_SCHEMA_PATHS, DEFAULT_SCHEMA_PATH):
        schema = load_schema(path)
        catalog[schema_fingerprint(schema)] = schema
    return catalog
```

In `decode_event`, use the catalog and read through the current schema as the reader schema:

```python
    schemas = catalog if catalog is not None else load_catalog()
    if fingerprint not in schemas:
        raise SchemaFingerprintError(f"unknown schema fingerprint {fingerprint}")
    encoded_record = io.BytesIO(payload[10:])
    try:
        decoded = fastavro.schemaless_reader(
            encoded_record, schemas[fingerprint], load_schema())
    except (TypeError, ValueError, EOFError) as exc:
        raise EventValidationError(str(exc)) from exc
```

In `archive_replay.py`, widen the fingerprint gate from one schema to the catalog. Replace
the `expected = schema_fingerprint(load_schema())` line and the mismatch check inside
`_validate_archive_fingerprints`:

```python
    accepted = set(load_catalog())
```

```python
                fingerprint = int(raw_fingerprint)
                observed.add(fingerprint)
                if fingerprint not in accepted:
                    raise ReplaySchemaFingerprintError(
                        "archive schema_fingerprint "
                        f"{fingerprint} is not in the local schema catalog {sorted(accepted)}",
                        observed,
                    )
```

Change the import on line 20 to `from event_avro import encode_event, load_catalog, load_schema, schema_fingerprint`, keeping the names the rest of the module still uses.

Update both `schema_fingerprint` values in `recsys-pipeline/config/kafka-topics.json` (lines 15 and 29) from `"225b275f487979ab"` to `"af86abe880fe4bb3"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q`
Expected: PASS, whole Python suite — `test_avro_kafka_round_trip.py` and `test_archive_replay.py` also encode through this module.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/schemas/recsys-event-v2.avsc \
        recsys-pipeline/services/spark-streaming-job/src/main/resources/schemas/recsys-event-v2.avsc \
        recsys-pipeline/services/python-modeling/event_avro.py \
        recsys-pipeline/services/python-modeling/archive_replay.py \
        recsys-pipeline/config/kafka-topics.json \
        recsys-pipeline/integration-tests/python_modeling/test_event_avro.py \
        recsys-pipeline/integration-tests/python_modeling/test_archive_replay.py
git commit -m "feat: add event schema v2 with typed context fields"
```

---

### Task 2: Scala codec fingerprint catalog

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventAvroCodec.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/event/EventAvroCodecSpec.scala`

**Interfaces:**
- Consumes: the two schema resources created in Task 1.
- Produces: `EventAvroCodec.schema` (now v2, unchanged name and type `Schema`), `EventAvroCodec.fingerprint: Long` (now the v2 value), `EventAvroCodec.catalog: Map[Long, Schema]`. `decode` and `encode` keep their current signatures, so `ExecutionEngine` and `EventParsing` need no change.

- [ ] **Step 1: Write the failing test**

Add to `EventAvroCodecSpec.scala`:

```scala
  it should "decode a v1 payload into the v2 reader shape" in {
    val v1Schema = {
      val input = getClass.getResourceAsStream("/schemas/recsys-event-v1.avsc")
      try new Schema.Parser().parse(input) finally input.close()
    }
    val v1Record = new GenericData.Record(v1Schema)
    v1Record.put("event_id", "e-legacy")
    v1Record.put("user_id", "u-1")
    v1Record.put("item_id", "i-1")
    v1Record.put("event_type", "click")
    v1Record.put("timestamp_ms", 1718400000000L)

    val output = new ByteArrayOutputStream()
    output.write(EventAvroCodec.Magic)
    output.write(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      .putLong(SchemaNormalization.parsingFingerprint64(v1Schema)).array())
    val encoder = EncoderFactory.get.binaryEncoder(output, null)
    new GenericDatumWriter[GenericRecord](v1Schema).write(v1Record, encoder)
    encoder.flush()

    EventAvroCodec.decode(output.toByteArray) match {
      case Right(record) =>
        record.get("event_id").toString shouldBe "e-legacy"
        record.get("surface") shouldBe null
        record.get("device") shouldBe null
      case Left(failure) => fail(s"expected a decoded record, got ${failure.code}")
    }
  }

  it should "expose v2 as the writer schema" in {
    val names = EventAvroCodec.schema.getFields.asScala.map(_.name).toSeq
    names.takeRight(4) shouldBe Seq("surface", "locale", "timezone", "device")
    EventAvroCodec.fingerprint shouldBe 0xAF86ABE880FE4BB3L
  }
```

Add the imports this needs at the top of the spec, if absent: `org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}`, `org.apache.avro.io.EncoderFactory`, `org.apache.avro.{Schema, SchemaNormalization}`, `java.io.ByteArrayOutputStream`, `java.nio.{ByteBuffer, ByteOrder}`, and `scala.collection.JavaConverters._`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch "testOnly com.demo.event.EventAvroCodecSpec"`
Expected: FAIL — the codec still loads v1, so the field names assertion fails and the v1 payload's fingerprint matches, returning a record without the new fields rather than exercising the catalog.

- [ ] **Step 3: Write minimal implementation**

In `EventAvroCodec.scala`, replace the `schema` and `fingerprint` definitions:

```scala
  private def parse(resource: String): Schema = {
    val input = Option(getClass.getResourceAsStream(resource))
      .getOrElse(throw new IllegalStateException(s"missing Avro schema resource $resource"))
    try new Schema.Parser().parse(input)
    finally input.close()
  }

  /** The schema new records are written with. */
  lazy val schema: Schema = parse("/schemas/recsys-event-v2.avsc")

  /** Every writer schema this decoder accepts, keyed by fingerprint. A record written
    * before a bump is still valid; resolving it against `schema` is what keeps it out
    * of the dead-letter archive. */
  lazy val catalog: Map[Long, Schema] =
    Seq("/schemas/recsys-event-v1.avsc", "/schemas/recsys-event-v2.avsc")
      .map(parse)
      .map(parsed => SchemaNormalization.parsingFingerprint64(parsed) -> parsed)
      .toMap

  lazy val fingerprint: Long = SchemaNormalization.parsingFingerprint64(schema)
```

Replace the fingerprint check and reader construction inside `decode`:

```scala
  def decode(bytes: Array[Byte]): Either[DecodeFailure, GenericRecord] = {
    if (bytes == null || bytes.length < 10 || !bytes.take(2).sameElements(Magic)) {
      Left(DecodeFailure.InvalidMarker("invalid Avro single-object marker"))
    } else {
      val writerFingerprint = ByteBuffer.wrap(bytes, 2, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
      catalog.get(writerFingerprint) match {
        case None =>
          Left(DecodeFailure.UnknownFingerprint("writer schema is not in the local catalog"))
        case Some(writerSchema) =>
          Try {
            val decoder = DecoderFactory.get.binaryDecoder(bytes, 10, bytes.length - 10, null)
            val record = new GenericDatumReader[GenericRecord](writerSchema, schema).read(null, decoder)
            if (!decoder.isEnd) throw new IllegalArgumentException("unexpected trailing Avro payload bytes")
            record
          } match {
            case Success(record) => missingRequiredField(record) match {
                case Some(field) => Left(DecodeFailure.RequiredField(s"missing required field $field"))
                case None        => Right(record)
              }
            case Failure(error) => Left(DecodeFailure.CorruptPayload(errorDetail(error)))
          }
      }
    }
  }
```

`encode` is unchanged: it already writes `schema` and `fingerprint`, which now mean v2.

The existing spec assertion "keep its checked-in schema semantically identical to the canonical schema" compares the resource against `recsys-pipeline/schemas/`. Extend it to assert the same for both v1 and v2 resources, so a future edit cannot update one copy alone.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch test`
Expected: PASS, whole Scala suite.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventAvroCodec.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/event/EventAvroCodecSpec.scala
git commit -m "feat: resolve event writer schemas through a fingerprint catalog"
```

---

### Task 3: Per-signal feedback attribution

This task changes no schema and adds no field. It fixes the attribution bug on its own so that Task 4's new event types cannot inherit it.

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala:201-208`
- Modify: `recsys-pipeline/services/python-modeling/movie_segment_producer.py:178-183`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala:227`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `training_samples` keeps the same column names and types; only their attribution changes. Task 4 adds columns to the same `agg` block.

- [ ] **Step 1: Write the failing test**

The existing test at line 227, `"keep only the latest feedback event's measurement fields when they are disjoint"`, asserts the buggy behavior — it expects the click's fields to be `null` after a rating-only order. **Replace that whole test** (its name, comment, and body) with:

```scala
  it should "attribute each feedback field to the latest event that set it" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // The click carries engagement signals and the order carries only a rating: exactly the
    // shape a producer emits when it splits measurement fields across two feedback events.
    // Each field is attributed independently, so the later rating does not blank the earlier
    // engagement signals and producers need not repeat them.
    val impression =
      """{"event_id":"impression","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"impression","timestamp":100,"position":0}"""
    val click =
      """{"event_id":"click","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"click","timestamp":105,"negative_feedback_reason":"not_interested","dwell_millis":9000,"completion_rate":0.08}"""
    val ratingOnlyOrder =
      """{"event_id":"order","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"order","timestamp":110,"rating":4.5}"""

    def sample(events: Seq[String]) =
      OnlineJoinerStreamingJob.buildTrainingSamples(
        OnlineJoinerStreamingJob.parseEvents(decodedJson(events.toDF("value"))).kept).first()

    val row = sample(Seq(impression, click, ratingOnlyOrder))

    row.getAs[Double]("rating") shouldBe 4.5
    row.getAs[String]("negative_feedback_reason") shouldBe "not_interested"
    row.getAs[Long]("dwell_millis") shouldBe 9000L
    row.getAs[Double]("completion_rate") shouldBe 0.08
    row.getAs[Int]("clicked") shouldBe 1
    row.getAs[Int]("ordered") shouldBe 1
  }

  it should "let a later feedback event override a field it actually sets" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val impression =
      """{"event_id":"impression","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"impression","timestamp":100,"position":0}"""
    val firstClick =
      """{"event_id":"c1","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"click","timestamp":105,"completion_rate":0.10}"""
    val secondClick =
      """{"event_id":"c2","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"click","timestamp":115,"completion_rate":0.90}"""

    val row = OnlineJoinerStreamingJob.buildTrainingSamples(
      OnlineJoinerStreamingJob.parseEvents(
        decodedJson(Seq(impression, firstClick, secondClick).toDF("value"))).kept).first()

    row.getAs[Double]("completion_rate") shouldBe 0.90
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"`
Expected: FAIL — `negative_feedback_reason` is `null` rather than `"not_interested"`, because the rating-only order supersedes the whole struct.

- [ ] **Step 3: Write minimal implementation**

In `OnlineJoinerStreamingJob.scala`, replace the single `feedback_measurement` aggregate with one aggregate per field. Add this helper above `buildTrainingSamples`:

```scala
  /** The value of `field` from the latest feedback event that actually set it.
    *
    * A single max_by over one struct would attribute every field to one event, so a later
    * event carrying only a rating blanks an earlier click's engagement signals. Ordering on
    * (timestamp, event_id) matches the impression aggregate's tie-break. */
  private def latestFeedback(field: String, isFeedback: Column): Column =
    max_by(
      when(isFeedback && col(field).isNotNull, col(field)),
      when(isFeedback && col(field).isNotNull,
        struct(col("timestamp"), coalesce(col("event_id"), lit("")))))
```

Replace the `max_by(... .as("feedback_measurement"))` entry in the `.agg(...)` list with:

```scala
        latestFeedback("timestamp", isFeedback).as("last_feedback_ts"),
        latestFeedback("timestamp_ms", isFeedback).as("last_feedback_ts_ms"),
        latestFeedback("rating", isFeedback).as("rating"),
        latestFeedback("negative_feedback_reason", isFeedback).as("negative_feedback_reason"),
        latestFeedback("dwell_millis", isFeedback).as("dwell_millis"),
        latestFeedback("completion_rate", isFeedback).as("completion_rate"),
```

`last_feedback_ts` and `last_feedback_ts_ms` use `latestFeedback` on the timestamp columns themselves, which is the latest feedback event overall — the same value the struct produced.

In the final `.select(...)`, replace the six `col("feedback_measurement.X").as("X")` projections with bare `col("X")` for `rating`, `negative_feedback_reason`, `dwell_millis`, `completion_rate`, and `last_feedback_ts`; and change the `feedback_delay_ms` expression to read the flat column:

```scala
        when(col("last_feedback_ts_ms").isNotNull,
          (col("last_feedback_ts_ms") - col("impression_ts_ms")).cast(LongType)
        ).as("feedback_delay_ms"),
```

Add `max_by` and `Column` to the existing imports if the file does not already have them (`org.apache.spark.sql.functions._` covers `max_by`; `org.apache.spark.sql.Column` needs an explicit import).

Then delete the now-unnecessary copy-forward in `movie_segment_producer.py`:

```python
            if rng.random() < order_prob(meta):
                order = base(item, "order", now_ms + rng.randint(21, 120) * 1000, position)
                order["rating"] = round(min(5.0, 3.0 + 2.0 * completion), 1)
                events.append(order)
```

(Remove the three-line `for field in (...)` loop and the comment above it explaining the workaround.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch test`
Then: `cd ../../.. && python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py -q`
Expected: PASS both. `LateFeedbackJoinSpec` exercises the same builder and must stay green.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala \
        recsys-pipeline/services/python-modeling/movie_segment_producer.py
git commit -m "fix: attribute each feedback field to the latest event that set it"
```

---

### Task 4: Thumb, abandon, and context columns in the joiner

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala:14-26,160-161,185-191,214-250`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventSchemas.scala:29-50`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/LateFeedbackJoin.scala:187-190`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`

**Interfaces:**
- Consumes: the `isFeedback` predicate and the per-field attribution established in Task 3; this task widens that predicate rather than calling `latestFeedback` directly.
- Produces: `training_samples` gains `thumb` (`IntegerType`, nullable), `abandoned` (`IntegerType`, 0/1), and `surface`, `locale`, `timezone`, `device` (`StringType`, nullable). Task 5 reads `device`; Task 6 reads `device`; Task 7's producers write all of them.

- [ ] **Step 1: Write the failing test**

Add to `OnlineJoinerStreamingJobSpec.scala`:

```scala
  it should "record thumbs and abandons without erasing click engagement" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val impression =
      """{"event_id":"impression","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"impression","timestamp":100,"position":0,"surface":"home_feed","locale":"en-US","timezone":"America/New_York","device":"ios"}"""
    val click =
      """{"event_id":"click","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"click","timestamp":105,"dwell_millis":9000,"completion_rate":0.80}"""
    val thumbUp =
      """{"event_id":"thumb","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"thumb_up","timestamp":200}"""

    val row = OnlineJoinerStreamingJob.buildTrainingSamples(
      OnlineJoinerStreamingJob.parseEvents(
        decodedJson(Seq(impression, click, thumbUp).toDF("value"))).kept).first()

    row.getAs[Int]("thumb") shouldBe 1
    row.getAs[Int]("abandoned") shouldBe 0
    row.getAs[Long]("dwell_millis") shouldBe 9000L
    row.getAs[Double]("completion_rate") shouldBe 0.80
    row.getAs[String]("surface") shouldBe "home_feed"
    row.getAs[String]("locale") shouldBe "en-US"
    row.getAs[String]("timezone") shouldBe "America/New_York"
    row.getAs[String]("device") shouldBe "ios"
    row.getAs[Double]("label") shouldBe 1.0
  }

  it should "take the latest thumb and flag an abandon" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val impression =
      """{"event_id":"impression","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"impression","timestamp":100,"position":0}"""
    val thumbUp =
      """{"event_id":"t1","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"thumb_up","timestamp":150}"""
    val thumbDown =
      """{"event_id":"t2","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"thumb_down","timestamp":250}"""
    val abandon =
      """{"event_id":"a1","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"abandon","timestamp":300,"completion_rate":0.05}"""

    val row = OnlineJoinerStreamingJob.buildTrainingSamples(
      OnlineJoinerStreamingJob.parseEvents(
        decodedJson(Seq(impression, thumbUp, thumbDown, abandon).toDF("value"))).kept).first()

    row.getAs[Int]("thumb") shouldBe -1
    row.getAs[Int]("abandoned") shouldBe 1
    row.getAs[Double]("completion_rate") shouldBe 0.05
    // A thumb is not a click: the label is unchanged by valence signals.
    row.getAs[Double]("label") shouldBe 0.0
  }

  it should "carry the typed context fields through a late-feedback snapshot" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // SnapshotColumns is a fixed list; a context field missing from it is silently dropped
    // when a slate waits for late feedback, which no other test would catch.
    LateFeedbackJoin.SnapshotColumns should contain allOf ("surface", "locale", "timezone", "device")
  }

  it should "leave thumb null when the user never thumbed" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val impression =
      """{"event_id":"impression","session_id":"s","request_id":"req","user_id":"user","item_id":"item","event_type":"impression","timestamp":100,"position":0}"""

    val row = OnlineJoinerStreamingJob.buildTrainingSamples(
      OnlineJoinerStreamingJob.parseEvents(
        decodedJson(Seq(impression).toDF("value"))).kept).first()

    row.getAs[AnyRef]("thumb") shouldBe null
    row.getAs[Int]("abandoned") shouldBe 0
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"`
Expected: FAIL — `org.apache.spark.sql.AnalysisException` for the missing `thumb` column.

- [ ] **Step 3: Write minimal implementation**

In `EventSchemas.scala`, add the four context fields to the `joiner` StructType's trailing `Seq(...)`:

```scala
      StructField("surface", StringType, nullable = true),
      StructField("locale", StringType, nullable = true),
      StructField("timezone", StringType, nullable = true),
      StructField("device", StringType, nullable = true)
```

In `OnlineJoinerStreamingJob.scala`, extend `MeasurementFields` with the four context fields so a DataFrame lacking them still gets typed nulls:

```scala
    "unsafe_label" -> BooleanType,
    "surface" -> StringType,
    "locale" -> StringType,
    "timezone" -> StringType,
    "device" -> StringType
```

Widen the feedback predicate (around line 161):

```scala
    val isFeedback = col("etype").isin(
      "click", "order", "purchase", "thumb_up", "thumb_down", "abandon")
```

Add the four context aggregates next to `user_features`, and the two valence aggregates, inside `.agg(...)`:

```scala
        first(when(isImpression, col("surface")),  ignoreNulls = true).as("surface"),
        first(when(isImpression, col("locale")),   ignoreNulls = true).as("locale"),
        first(when(isImpression, col("timezone")), ignoreNulls = true).as("timezone"),
        first(when(isImpression, col("device")),   ignoreNulls = true).as("device"),
        max_by(
          when(col("etype") === "thumb_up", lit(1)).when(col("etype") === "thumb_down", lit(-1)),
          when(col("etype").isin("thumb_up", "thumb_down"),
            struct(col("timestamp"), coalesce(col("event_id"), lit(""))))
        ).as("thumb"),
        max(when(col("etype") === "abandon", lit(1)).otherwise(lit(0))).as("abandoned"),
```

Add the six new columns to the final `.select(...)`, after `session_id`:

```scala
        col("thumb"),
        coalesce(col("abandoned"), lit(0)).as("abandoned"),
        col("surface"),
        col("locale"),
        col("timezone"),
        col("device"),
```

In `LateFeedbackJoin.scala`, `SnapshotColumns` already appends `MeasurementFields.map(_._1)`, so the four context fields arrive automatically — but `thumb` and `abandoned` are derived in the aggregate, not carried on the event, so they must **not** be added there. Leave `SnapshotColumns` unchanged and confirm by running `LateFeedbackJoinSpec`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch test`
Expected: PASS, whole Scala suite including `LateFeedbackJoinSpec` and `ExperienceCollectorStreamingJobSpec`. Add `import com.demo.process.LateFeedbackJoin` to the spec if it is not already imported.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventSchemas.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala
git commit -m "feat: carry typed context, thumb, and abandon into training samples"
```

---

### Task 5: CTR trainer reads the moved keys

Lands before Task 7 so no window exists where the trainer silently loses features.

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala:25-32`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala`

**Interfaces:**
- Consumes: the `device` column from Task 4; `user_features` map (unchanged shape).
- Produces: no new public names. `assembleFeatures(df, numFeatures)` keeps its signature.

- [ ] **Step 1: Write the failing test**

Add to `CtrRankingModelTrainingJobSpec.scala`:

```scala
  it should "read device and country from their new homes, falling back to the legacy map" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // genres and tags must be present: assembleFeatures reads them with coalesce, which
    // raises AnalysisException on a missing column rather than yielding null.
    val typed = Seq(
      ("u1", "i1", Map("country" -> "us"), Map.empty[String, String],
        Map.empty[String, String], "ios", 0, 1, Seq("drama"), Seq("classic"))
    ).toDF("user_id", "item_id", "user_features", "item_features",
      "context_features", "device", "position", "clicked", "genres", "tags")

    val typedRow = CtrRankingModelTrainingJob.assembleFeatures(
      CtrRankingModelTrainingJob.labelColumn(typed, "click"), 64).first()

    typedRow.getAs[String]("cf_device") shouldBe "ios"
    typedRow.getAs[String]("cf_country") shouldBe "us"

    val legacy = Seq(
      ("u1", "i1", Map.empty[String, String], Map.empty[String, String],
        Map("device" -> "ios", "country" -> "us"), 0, 1, Seq("drama"), Seq("classic"))
    ).toDF("user_id", "item_id", "user_features", "item_features",
      "context_features", "position", "clicked", "genres", "tags")

    val legacyRow = CtrRankingModelTrainingJob.assembleFeatures(
      CtrRankingModelTrainingJob.labelColumn(legacy, "click"), 64).first()

    legacyRow.getAs[String]("cf_device") shouldBe "ios"
    legacyRow.getAs[String]("cf_country") shouldBe "us"
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch "testOnly com.demo.task.CtrRankingModelTrainingJobSpec"`
Expected: FAIL — the typed case yields `"NA"` for both, because the trainer only reads `context_features`.

- [ ] **Step 3: Write minimal implementation**

In `CtrRankingModelTrainingJob.scala`, add a helper above `assembleFeatures`:

```scala
  /** The first source that has the value: the typed column, then the named map key.
    *
    * `device` moved from `context_features` to a typed field and `country` moved to
    * `user_features` in schema v2. Reading both keeps Parquet written before that bump
    * trainable, and keeps a producer that has not been updated yet from silently
    * degrading the model to the "NA" default. */
  private def firstAvailable(df: DataFrame, column: String, map: String, key: String): Column = {
    val fromColumn = if (df.columns.contains(column)) col(column) else lit(null).cast("string")
    val fromMap =
      if (df.columns.contains(map)) element_at(col(map), key) else lit(null).cast("string")
    coalesce(fromColumn, fromMap, lit("NA"))
  }
```

Replace the two `cf_*` lines in `assembleFeatures`:

```scala
      .withColumn("cf_device",  firstAvailable(df, "device", "context_features", "device"))
      .withColumn("cf_country", firstAvailable(df, "country", "user_features", "country"))
```

Keep `uf_tier` and `if_bucket` exactly as they are — `tier` is unresolved between producers and is sub-project B's problem.

Add `org.apache.spark.sql.Column` to the imports if absent.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch test`
Expected: PASS, whole Scala suite.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala
git commit -m "feat: read device and country from their v2 homes in the CTR trainer"
```

---

### Task 6: Rename the platform governance dimension to device

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/governance_measurements.py:18-26`
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py:391-409`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_governance_measurements.py`

**Interfaces:**
- Consumes: the `device` column from Task 4.
- Produces: `DEFAULT_DIMENSIONS` with `"device"` in place of `"platform"`; `_with_demographic_columns(samples)` keeps its signature and now also hoists from a typed column.

- [ ] **Step 1: Write the failing test**

Add to `recsys-pipeline/integration-tests/python_modeling/test_governance_measurements.py`:

```python
def test_device_is_a_declared_dimension_not_platform():
    """Fails while the event says device and the dimension list says platform."""
    from governance_measurements import DEFAULT_DIMENSIONS

    assert "device" in DEFAULT_DIMENSIONS
    assert "platform" not in DEFAULT_DIMENSIONS


def test_device_resolves_from_a_typed_column_and_from_user_features():
    """Fails if hoisting only looks in user_features, which is why platform never resolved."""
    import pandas as pd
    from analysis_dashboard_report import _with_demographic_columns

    typed = pd.DataFrame({"user_id": ["u1"], "device": ["ios"], "user_features": [{}]})
    assert _with_demographic_columns(typed)["device"].tolist() == ["ios"]

    legacy = pd.DataFrame({"user_id": ["u1"], "user_features": [{"device": "web"}]})
    assert _with_demographic_columns(legacy)["device"].tolist() == ["web"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_governance_measurements.py -q`
Expected: FAIL — `"device" in DEFAULT_DIMENSIONS` is False.

- [ ] **Step 3: Write minimal implementation**

In `governance_measurements.py`, rename the entry:

```python
DEFAULT_DIMENSIONS: tuple[str, ...] = (
    "age_band",
    "gender",
    "occupation",
    "geo",
    "device",
    "country",
    "subscription",
)
```

In `analysis_dashboard_report.py`, no logic change is required: the existing
`dimension not in samples.columns` guard already leaves a typed `device` column alone, and
the `user_features` path already hoists a legacy one. Update only the docstring so the next
reader knows both sources are intended:

```python
def _with_demographic_columns(samples):
    """Hoist allowlisted demographics into fairness columns.

    A dimension already present as a typed column is left alone. Otherwise it is read
    from user_features, which is where user attributes live until they are typed. The
    allowlist is the cardinality guard: a key outside DEFAULT_DIMENSIONS is never
    promoted, so no arbitrary user attribute can become a published group.
    """
    from governance_measurements import DEFAULT_DIMENSIONS
    if "user_features" not in samples.columns:
        return samples
    features = [_feature_map(value) for value in samples["user_features"]]
    missing = [dimension for dimension in DEFAULT_DIMENSIONS
               if dimension not in samples.columns and any(dimension in entry for entry in features)]
    if not missing:
        return samples
    hoisted = samples.copy()
    for dimension in missing:
        hoisted[dimension] = [entry.get(dimension) for entry in features]
    return hoisted
```

Confirm with the test rather than assuming it. If the typed assertion fails, the guard is not
doing what this step claims and the function — not the test — needs the fix.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q`
Expected: PASS, whole Python suite. If `test_analysis_dashboard.py` pins the string `"platform"` in a fairness warning, update that fixture to `"device"`.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/governance_measurements.py \
        recsys-pipeline/services/python-modeling/analysis_dashboard_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_governance_measurements.py
git commit -m "fix: name the platform fairness dimension device so it resolves"
```

---

### Task 7: Producers emit v2 context and valence events

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/feedback_schedule.py:18`
- Modify: `recsys-pipeline/services/python-modeling/producer.py:43-100`
- Modify: `recsys-pipeline/services/python-modeling/backfill_producer.py:88-150`
- Modify: `recsys-pipeline/services/python-modeling/movie_segment_producer.py:60-200`
- Modify: `recsys-pipeline/services/python-modeling/movielens_segment_producer.py:41-165`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py`, `test_engagement_sim.py`

**Interfaces:**
- Consumes: `encode_event` writing v2 (Task 1); the joiner columns from Task 4.
- Produces: events carrying `surface`, `locale`, `timezone`, `device` at top level; `country` inside `user_features`; `thumb_up` / `thumb_down` / `abandon` events.

- [ ] **Step 1: Write the failing test**

Add to `recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py`:

```python
def test_locale_and_timezone_are_stable_per_user():
    """Fails if context is drawn per slate: a user's locale must not change between slates."""
    rng = random.Random(17)
    movies = producer.assign_movies(20, rng)
    users = producer.assign_users(10, rng)
    items = list(movies)

    seen = {}
    for _ in range(40):
        user = rng.choice(list(users))
        for event in producer.make_slate(user, users[user], items, movies, rng):
            if event["event_type"] != "impression":
                continue
            previous = seen.setdefault(user, (event["locale"], event["timezone"]))
            assert previous == (event["locale"], event["timezone"])


def test_impressions_carry_typed_context_and_country_moves_to_user_features():
    rng = random.Random(17)
    movies = producer.assign_movies(20, rng)
    users = producer.assign_users(10, rng)
    user = next(iter(users))

    events = producer.make_slate(user, users[user], list(movies), movies, rng)
    impression = next(e for e in events if e["event_type"] == "impression")

    assert impression["surface"] in producer.SURFACES
    assert impression["device"] in ("ios", "android", "web")
    assert impression["locale"] in ("en-US", "en-CA", "fr-CA", "en-GB", "de-DE")
    assert impression["country"] not in impression.get("context_features", {})
    assert impression["user_features"]["country"] == users[user]["country"]
    assert impression["context_features"] == {}


def test_low_completion_clicks_produce_an_abandon_and_high_ones_a_thumb_up():
    rng = random.Random(3)
    movies = producer.assign_movies(40, rng)
    users = producer.assign_users(20, rng)
    items = list(movies)

    types = set()
    for _ in range(200):
        user = rng.choice(list(users))
        for event in producer.make_slate(user, users[user], items, movies, rng):
            types.add(event["event_type"])

    assert "thumb_up" in types
    assert "abandon" in types
```

Add to `recsys-pipeline/integration-tests/python_modeling/test_engagement_sim.py`:

```python
def test_valence_events_are_deferred_like_other_feedback():
    """Fails if split_slate sends a thumb at slate time instead of at its own timestamp."""
    from feedback_schedule import split_slate

    base_ms = 1_700_000_000_000
    events = [
        {"event_type": "impression", "timestamp_ms": base_ms},
        {"event_type": "thumb_up", "timestamp_ms": base_ms + 30_000},
        {"event_type": "abandon", "timestamp_ms": base_ms + 45_000},
    ]

    immediate, deferred = split_slate(events)

    assert [e["event_type"] for e in immediate] == ["impression"]
    assert sorted(delay for delay, _ in deferred) == [30.0, 45.0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py recsys-pipeline/integration-tests/python_modeling/test_engagement_sim.py -q`
Expected: FAIL — `producer.SURFACES` is undefined, impressions have no `surface` key, and `split_slate` puts the thumb in `immediate`.

- [ ] **Step 3: Write minimal implementation**

In `feedback_schedule.py`:

```python
FEEDBACK_TYPES = frozenset({"click", "order", "purchase", "thumb_up", "thumb_down", "abandon"})
```

In `movie_segment_producer.py`, add the vocabularies and derivations near the other ground-truth tables:

```python
SURFACES = ("home_feed", "search_results", "detail_page", "continue_watching")
# Additive per-slate click effect, so the surface is recoverable from the report.
SURFACE_EFF = {"home_feed": 0.02, "search_results": 0.04,
               "detail_page": 0.01, "continue_watching": 0.03}
# One locale and zone per country. Canada is split so a locale is not a country alias.
COUNTRY_LOCALE = {"us": "en-US", "ca": "en-CA", "gb": "en-GB", "de": "de-DE"}
COUNTRY_TIMEZONE = {"us": "America/New_York", "ca": "America/Toronto",
                    "gb": "Europe/London", "de": "Europe/Berlin"}
FRENCH_CANADIAN_SHARE = 4  # every Nth Canadian user gets fr-CA

THUMB_UP_COMPLETION = 0.70   # completion at or above this may thumb up
THUMB_UP_PROB = 0.30
THUMB_DOWN_PROB = 0.50       # applied to clicks already reporting not_interested
```

Extend `assign_users` so each user carries its derived context, keyed by index for the French-Canadian split:

```python
def assign_users(num_users: int, rng: random.Random) -> dict[str, dict]:
    """Per-user demographics plus the context derived from them.

    locale and timezone are a function of the user, not the slate: a value redrawn per
    event would be noise no report could attribute to anyone.
    """
    users = {}
    for i in range(1, num_users + 1):
        country = rng.choice(COUNTRIES)
        locale = COUNTRY_LOCALE[country]
        if country == "ca" and i % FRENCH_CANADIAN_SHARE == 0:
            locale = "fr-CA"
        users[f"user_{i}"] = {
            "gender": rng.choice(GENDERS),
            "age_band": rng.choice(AGE_BANDS),
            "country": country,
            "subscription": rng.choice(tuple(SUBSCRIPTION_EFF)),
            "locale": locale,
            "timezone": COUNTRY_TIMEZONE[country],
        }
    return users
```

`user_features` must stay inside the governance allowlist, so `locale` and `timezone` are removed from the map before it is attached. In `make_slate`, replace `base` and add the surface draw:

```python
def make_slate(user: str, user_meta: dict, items, movies: dict, rng: random.Random) -> list[dict]:
    now_ms = int(time.time() * 1000)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    slate_items = rng.sample(items, min(SLATE_SIZE, len(items)))
    surface = rng.choice(SURFACES)
    device = rng.choice(("ios", "android", "web"))
    # Only allowlisted user attributes travel in user_features; locale and timezone are
    # request context and ride as typed fields.
    user_features = {key: value for key, value in user_meta.items()
                     if key not in ("locale", "timezone")}

    def base(item: str, event_type: str, timestamp_ms: int, position: int) -> dict:
        return {
            "event_id": str(uuid.uuid4()), "request_id": request_id, "session_id": session_id,
            "user_id": user, "item_id": item, "event_type": event_type,
            "timestamp_ms": timestamp_ms, "position": position,
            "user_features": dict(user_features), "item_features": {}, "context_features": {},
            "surface": surface, "device": device,
            "locale": user_meta["locale"], "timezone": user_meta["timezone"],
        }
```

Include the surface effect in the click probability, next to `user_click_bias`:

```python
        click_prob = min(0.6, max(0.02, item_click_prob(meta)
                                  + user_click_bias(user_meta) + SURFACE_EFF[surface]))
```

Emit the valence events inside the existing `if rng.random() < click_prob:` block, after the click is appended:

```python
            if completion >= THUMB_UP_COMPLETION and rng.random() < THUMB_UP_PROB:
                events.append(base(item, "thumb_up", now_ms + rng.randint(30, 180) * 1000, position))
            elif click["negative_feedback_reason"] and rng.random() < THUMB_DOWN_PROB:
                thumb_down = base(item, "thumb_down", now_ms + rng.randint(30, 180) * 1000, position)
                thumb_down["negative_feedback_reason"] = click["negative_feedback_reason"]
                events.append(thumb_down)
            if completion < NEGATIVE_COMPLETION_CUTOFF:
                abandon = base(item, "abandon", now_ms + rng.randint(21, 90) * 1000, position)
                abandon["completion_rate"] = click["completion_rate"]
                events.append(abandon)
```

In `producer.py` (`make_behavior_slate`) and `backfill_producer.py` (`make_slate`), replace `"context_features": {"device": device, "country": country}` on the impression with typed fields and move `country` into `user_features`:

```python
            "user_features": {"tier": user_tier, "country": country},
            "item_features": {"bucket": f"b{int(item.split('_')[-1]) % 4}"},
            "context_features": {},
            "surface": surface,
            "device": device,
            "locale": COUNTRY_LOCALE[country],
            "timezone": COUNTRY_TIMEZONE[country],
```

Both files need the same `SURFACES`, `COUNTRY_LOCALE`, and `COUNTRY_TIMEZONE` tables and a `surface = random.choice(SURFACES)` alongside the existing `device` draw. Their `country` vocabulary is uppercase (`"US"`, `"CA"`, `"GB"`); lower-case it at the point of assignment (`country = random.choice(["us", "ca", "gb"])`) so one mapping serves every producer.

In `movielens_segment_producer.py`, replace `context_features = {"platform": platform}` with the typed fields, deriving locale and timezone from the user's ZIP region:

```python
ZIP_TIMEZONE = {"Northeast": "America/New_York", "Mid-Atlantic": "America/New_York",
                "Southeast": "America/New_York", "Midwest": "America/Chicago",
                "South-Central": "America/Chicago", "Mountain": "America/Denver",
                "West": "America/Los_Angeles", "unknown": None}
```

and in `make_slate`:

```python
    surface = rng.choice(SURFACES)
    region = derive_geo(demo["zip_code"])
    context = {
        "surface": surface,
        "device": platform,
        "locale": "en-US",              # MovieLens ZIPs are US-only
        "timezone": ZIP_TIMEZONE[region],
    }
```

Merge `context` into each impression dict and set `"context_features": {}`. Keep `PLATFORM_EFF` keyed on the same `platform` value — the variable name stays, only its destination changes.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q`
Then: `python3 -m pytest recsys-pipeline/integration-tests/test_avro_kafka_round_trip.py -q` (skips without a broker; that is expected)
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/feedback_schedule.py \
        recsys-pipeline/services/python-modeling/producer.py \
        recsys-pipeline/services/python-modeling/backfill_producer.py \
        recsys-pipeline/services/python-modeling/movie_segment_producer.py \
        recsys-pipeline/services/python-modeling/movielens_segment_producer.py \
        recsys-pipeline/integration-tests/python_modeling/test_movie_category_sim.py \
        recsys-pipeline/integration-tests/python_modeling/test_engagement_sim.py
git commit -m "feat: emit typed context and valence events from every producer"
```

---

### Task 8: Document the v2 contract

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md:522-535` (the `behavior` mode event schema example) and the Redis/topic tables that list event fields

**Interfaces:**
- Consumes: the shape produced by Tasks 1–7.
- Produces: nothing.

- [ ] **Step 1: Update the event contract documentation**

Replace the `behavior` mode event schema example with a v2 impression:

```json
{
  "request_id": "req_abc123",
  "user_id": "user_1",
  "item_id": "item_3",
  "event_type": "impression",
  "timestamp_ms": 1713600001000,
  "position": 0,
  "surface": "home_feed",
  "device": "ios",
  "locale": "en-US",
  "timezone": "America/New_York",
  "user_features": {"tier": "vip", "country": "us"},
  "item_features": {"bucket": "b1"},
  "context_features": {}
}
```

Add a short subsection after it:

```markdown
#### Schema versions

`recsys_events` carries Avro single-object payloads. Two writer schemas are valid:

| Schema | Fingerprint | Written by |
|---|---|---|
| `recsys-event-v1.avsc` | `225b275f487979ab` | Records produced before the v2 bump; still decodable |
| `recsys-event-v2.avsc` | `af86abe880fe4bb3` | Every producer today |

Both codecs resolve the writer schema by fingerprint and read it into the v2 shape, so a
v1 record arrives with `surface`, `locale`, `timezone`, and `device` set to `null`. A
fingerprint in neither schema is a dead letter with `error_code = unknown_fingerprint`.

**Where a field belongs.** Request context — what was true of the request — is a typed
field on the event. User attributes — what is true of the person regardless of request —
live in `user_features` until they are typed. `context_features` is for ad-hoc experiment
keys and is empty in every current producer.

#### Event types

| Type | Meaning |
|---|---|
| `impression` (alias `exposure`) | An item was shown in a slate |
| `click` | The item was opened; carries `dwell_millis` and `completion_rate` |
| `order` (alias `purchase`) | Conversion; carries `rating` |
| `thumb_up` / `thumb_down` | Explicit valence, arriving at its own time after the click |
| `abandon` | The user stopped early; carries the `completion_rate` reached |
| `rating` | Explicit 1-5 rating, on the `movielens_context` topic |

Feedback is attributed per field: each of `rating`, `negative_feedback_reason`,
`dwell_millis`, and `completion_rate` takes its value from the latest event that set it,
so a later thumb does not blank an earlier click's engagement signals.
```

- [ ] **Step 2: Verify the documented fields match the code**

Run: `grep -n '"name"' recsys-pipeline/schemas/recsys-event-v2.avsc | tail -4`
Expected: `surface`, `locale`, `timezone`, `device` — matching the documented example.

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q` and the Scala suite once more.
Expected: PASS both.

- [ ] **Step 3: Commit**

```bash
git add recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
git commit -m "docs: document the v2 event contract and event types"
```

---

## Verification

- [ ] `JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt -batch test` passes from `recsys-pipeline/services/spark-streaming-job`.
- [ ] `python3 -m pytest recsys-pipeline/integration-tests -q` passes from the repository root.
- [ ] `git grep -n '"platform"' recsys-pipeline/services/python-modeling/governance_measurements.py` returns nothing.
- [ ] `git grep -n 'context_features": {"device"' recsys-pipeline/services/python-modeling` returns nothing — no producer writes context to the map.
- [ ] `git grep -n '225b275f487979ab' recsys-pipeline/config` returns nothing; the topic config pins the v2 fingerprint.
- [ ] `git diff master --stat` shows no change under `services/java-retrieval-service/` — the serving path is untouched by this plan.

## Deferred, deliberately

The committed dashboard snapshot at `frontend/data/dashboard.json` still names `platform`
in its fairness warnings. Regenerating it needs a full sim run plus Redis
(`REDIS_HOST=localhost python frontend/export_dashboard_json.py --output frontend/data/dashboard.json`),
and the output is not byte-reproducible — the freshness age fields shift on every run. No
test asserts the snapshot's contents, so a stale snapshot breaks nothing. Regenerate it as
a follow-up when a sim is run for other reasons.
