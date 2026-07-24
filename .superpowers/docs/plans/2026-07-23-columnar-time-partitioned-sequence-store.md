# Columnar, Time-Partitioned Sequence Store — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace CSV-blob user sequences (hard-capped at 50 items) with a columnar, time-partitioned store that serves long sequences from Redis and trains from Parquet, both generated from one schema.

**Architecture:** A sequence is partitioned by `(userId, kind, dayBucket)` — the partition key — and each event attribute (`item_id`, `ts`, `action`, `rating`, `genres`, `release_year`) is a separate column. In Redis, one partition is one HASH whose fields are packed, positionally-aligned column strings; in Parquet, the same partition is `partitionBy("bucket","kind")` with one row per event. Writes touch only the current bucket; reads walk buckets newest-first and stop as soon as they have enough rows, fetching only the columns asked for.

**Tech Stack:** Scala 2.12.18 + Spark 3.5.1 + sbt + ScalaTest 3.2.18 (`services/spark-streaming-job`); Java 17 + Spring Boot + Maven + JUnit 5 + Mockito (`services/java-retrieval-service`); Jedis 5.1.5 (Spark side), Spring `StringRedisTemplate` (serving side).

**Spec:** `.superpowers/docs/specs/2026-07-23-columnar-time-partitioned-sequence-store-design.md`

## Global Constraints

- **Nothing legacy is deleted or modified in place.** `user:{id}:features` CSV fields, `user:{id}:served_history`, and their writers keep working unchanged.
- **Existing tests may be added to, never edited.** Only two existing test files are touched at all — `MovieLensContextCollectorStreamingJobSpec.scala` (Task 7) and `UserEventStreamingJobSpec.scala` (Task 8) — and both only gain new cases; no existing assertion changes. `RatingSequencesQueryHydratorTest` is **not** touched: Task 12 adds a separate `RatingSequencesQueryHydratorSequenceStoreTest`, and the untouched original passing is what proves criterion 2.
- **Redis key format:** `seq:{userId}:{kind}:{bucket}`. `kind ∈ {rating, click}`. `bucket` is UTC `yyyyMMdd` (or `yyyyMMddHH` when width is `hour`).
- **Row separator is `,`; within-row (genres) separator is `|`; a null value is an empty element.** Genre values are sanitized at encode time by removing both separators.
- **Always split with limit `-1`** (`str.split(",", -1)` in Scala and Java). The default drops trailing empty strings, which silently mis-aligns columns whose last value is null. This is the single most likely bug in this plan.
- **Reads must never be issued inside an open Redis pipeline.** Jedis `hget` inside an open pipeline reads buffered pipeline replies and corrupts the protocol. Follow the phase-1-read / phase-2-pipeline split used by `MovieLensContextCollectorStreamingJob`.
- **`n` is authoritative.** Decoders read `n` and truncate every column to `min(n, shortest column length)`, logging when they differ.
- **Config defaults:** `SEQ_BUCKET_WIDTH=day`, `SEQ_LOOKBACK_DAYS=90`, `SEQ_MAX_ROWS_PER_BUCKET=500`, `SEQ_BUCKET_FETCH_CHUNK=7`, `recsys.sequence.mode=off`.
- **Timestamps are epoch milliseconds everywhere in this store.** `MovieLensContextCollectorStreamingJob`'s `timestamp` field is in **seconds** and must be multiplied by 1000 at the producer boundary.
- **Build/test commands:**
  - Scala: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly <FQCN>"`, full suite `sbt test`
  - Java: `cd recsys-pipeline && mvn -pl services/java-retrieval-service test -Dtest=<ClassName>`, full suite `mvn -pl services/java-retrieval-service test`

---

## File Structure

**Scala — new package `com.demo.sequence`** (`services/spark-streaming-job/src/main/scala/com/demo/sequence/`)

| File | Responsibility |
|---|---|
| `SequenceSchema.scala` | Names and shapes only: kinds, column names, separators, `bucket(tsMillis, width)` scalar, `bucketColumn(tsCol, width)` Spark expression, `key(...)`. No I/O, no DataFrames. |
| `SequenceCodec.scala` | Pure packed-string manipulation: `pack`, `unpack`, `merge`. The seam every tricky rule is tested through. |
| `SequenceEncoder.scala` | Events DataFrame → column-chunk DataFrame. One `groupBy`. |
| `SequenceRedisSink.scala` | `Sink` impl. Append (read-merge-write) or Overwrite. Owns the two-phase Jedis dance. |
| `SequenceParquetSink.scala` | `Sink` impl. Explode chunks back to rows, `partitionBy("bucket","kind")`. |
| `SequenceJobConfig.scala` | The `SEQ_*` knobs read once, plus `SequenceSinks.write` — the persist / two-sink / unpersist fan-out shared by all three writers. |
| `SequenceBackfillJob.scala` | One-shot `main`. Ratings source → encoder → both sinks in Overwrite mode. |

**Scala — modified:** `process/MovieLensContextCollectorStreamingJob.scala` (add `kind=rating` producer), `task/UserEventStreamingJob.scala` (add `kind=click` producer).

**Java — new** (`services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/`)

| File | Responsibility |
|---|---|
| `SequenceSchemaConstants.java` | Mirror of the Scala constants. Tested against a shared fixture. |
| `SequenceCodec.java` | Unpack packed column strings → element lists. Read-side only; serving never writes. |
| `SequenceSlice.java` | Columnar result: `Map<String, List<String>>` + `size()` + typed accessors. |
| `SequenceClient.java` | Interface — `read(userId, kind, columns, maxRows, lookback)`. |
| `RedisSequenceClient.java` | Bucket walk with chunked pipelining and early exit. |

**Java — modified:** `config/RecommendationProperties.java` (nested `Sequence`), `resources/application.yml` (`recsys.sequence` block), `service/query_hydrators/RatingSequencesQueryHydrator.java` (off/shadow/on).

**Shared fixture:** `services/spark-streaming-job/src/test/resources/sequence-schema.json` and `services/java-retrieval-service/src/test/resources/sequence-schema.json` — byte-identical; both languages assert against it. Cross-language drift is this design's most exposed failure mode.

---

### Task 1: `SequenceSchema` — names, buckets, keys

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceSchema.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceSchemaSpec.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/resources/sequence-schema.json`

**Interfaces:**
- Consumes: nothing.
- Produces: `SequenceSchema.KindRating: String`, `KindClick: String`, `ColItemId/ColTs/ColAction/ColRating/ColGenres/ColReleaseYear/ColCount: String`, `Columns: Seq[String]`, `RowSeparator: String`, `ValueSeparator: String`, `bucket(tsMillis: Long, width: String): String`, `bucketColumn(tsCol: Column, width: String): Column`, `key(userId: String, kind: String, bucket: String): String`.

- [ ] **Step 1: Write the failing test**

Create `SequenceSchemaSpec.scala`:

```scala
package com.demo.sequence

import com.demo.SparkTestSupport
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceSchemaSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  // 2026-07-23T00:00:00.000Z and 2026-07-23T23:59:59.999Z
  private val dayStart = 1784764800000L
  private val dayEnd   = 1784851199999L

  "bucket" should "map an entire UTC day to one stamp" in {
    SequenceSchema.bucket(dayStart, "day") shouldBe "20260723"
    SequenceSchema.bucket(dayEnd, "day") shouldBe "20260723"
  }

  it should "put the next millisecond in the next bucket" in {
    SequenceSchema.bucket(dayEnd + 1L, "day") shouldBe "20260724"
  }

  it should "support hour width" in {
    SequenceSchema.bucket(dayStart, "hour") shouldBe "2026072300"
    SequenceSchema.bucket(dayEnd, "hour") shouldBe "2026072323"
  }

  "key" should "format the partition key" in {
    SequenceSchema.key("u1", SequenceSchema.KindRating, "20260723") shouldBe "seq:u1:rating:20260723"
  }

  "bucketColumn" should "agree with the scalar bucket function" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val timestamps = Seq(dayStart, dayEnd, dayEnd + 1L, 0L)
    val computed = timestamps.toDF("ts")
      .select(SequenceSchema.bucketColumn(col("ts"), "day").as("bucket"))
      .as[String]
      .collect()
      .toSeq

    computed shouldBe timestamps.map(SequenceSchema.bucket(_, "day"))
  }

  it should "agree with the scalar bucket function at hour width" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val timestamps = Seq(dayStart, dayStart + 3600000L, dayEnd)
    val computed = timestamps.toDF("ts")
      .select(SequenceSchema.bucketColumn(col("ts"), "hour").as("bucket"))
      .as[String]
      .collect()
      .toSeq

    computed shouldBe timestamps.map(SequenceSchema.bucket(_, "hour"))
  }

  "Columns" should "match the shared cross-language fixture" in {
    val fixture = scala.io.Source.fromInputStream(
      getClass.getResourceAsStream("/sequence-schema.json")
    ).mkString
    // Deliberately a substring assertion, not a JSON parse: the fixture exists to
    // detect drift, and adding a JSON library to this module for one test is not worth it.
    SequenceSchema.Columns.foreach(c => fixture should include(s""""$c""""))
    fixture should include(s""""rowSeparator": "${SequenceSchema.RowSeparator}"""")
    fixture should include(s""""valueSeparator": "${SequenceSchema.ValueSeparator}"""")
    fixture should include(s""""keyPrefix": "seq"""")
  }
}
```

Create `src/test/resources/sequence-schema.json`:

```json
{
  "keyPrefix": "seq",
  "kinds": ["rating", "click"],
  "columns": ["item_id", "ts", "action", "rating", "genres", "release_year"],
  "countField": "n",
  "rowSeparator": ",",
  "valueSeparator": "|"
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceSchemaSpec"`
Expected: FAIL — compilation error, `object SequenceSchema is not a member of package com.demo.sequence`.

- [ ] **Step 3: Write minimal implementation**

Create `SequenceSchema.scala`:

```scala
package com.demo.sequence

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

/** Names and shapes for the columnar sequence store. No I/O, no DataFrames beyond
  * `bucketColumn`, so every other component can depend on this without pulling in Redis. */
object SequenceSchema {
  val KindRating = "rating"
  val KindClick  = "click"

  val ColItemId      = "item_id"
  val ColTs          = "ts"
  val ColAction      = "action"
  val ColRating      = "rating"
  val ColGenres      = "genres"
  val ColReleaseYear = "release_year"
  val ColCount       = "n"

  val Columns: Seq[String] =
    Seq(ColItemId, ColTs, ColAction, ColRating, ColGenres, ColReleaseYear)

  val RowSeparator   = ","
  val ValueSeparator = "|"

  private val DayFormat  = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)
  private val HourFormat = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC)

  def bucket(tsMillis: Long, width: String): String = width match {
    case "hour" => HourFormat.format(Instant.ofEpochMilli(tsMillis))
    case _      => DayFormat.format(Instant.ofEpochMilli(tsMillis))
  }

  /** Spark expression equivalent of `bucket`. Uses DateType arithmetic from the epoch
    * rather than `from_unixtime`, so the result does not depend on the session time zone. */
  def bucketColumn(tsCol: Column, width: String): Column = {
    val day = date_format(
      date_add(to_date(lit("1970-01-01")), floor(tsCol / 86400000L).cast("int")),
      "yyyyMMdd"
    )
    width match {
      case "hour" =>
        concat(day, lpad((floor(tsCol / 3600000L) % 24L).cast("string"), 2, "0"))
      case _ => day
    }
  }

  def key(userId: String, kind: String, bucket: String): String =
    s"seq:$userId:$kind:$bucket"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceSchemaSpec"`
Expected: PASS — 6 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceSchema.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceSchemaSpec.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/resources/sequence-schema.json
git commit -m "feat(sequence): add SequenceSchema with UTC time bucketing"
```

---

### Task 2: `SequenceCodec` — pack, unpack, merge

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceCodec.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceCodecSpec.scala`

**Interfaces:**
- Consumes: `SequenceSchema.Columns`, `ColCount`, `RowSeparator`, `ValueSeparator`.
- Produces:
  - `SequenceCodec.pack(values: Seq[String]): String`
  - `SequenceCodec.unpack(packed: String, n: Int): Seq[String]`
  - `SequenceCodec.merge(existing: Map[String, String], fresh: Map[String, String], maxRows: Int): Map[String, String]` — both maps are field→packed-string including `"n"`; returns the same shape.

- [ ] **Step 1: Write the failing test**

Create `SequenceCodecSpec.scala`:

```scala
package com.demo.sequence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceCodecSpec extends AnyFlatSpec with Matchers {

  private def chunk(itemIds: String, ts: String, n: Int): Map[String, String] = Map(
    SequenceSchema.ColItemId -> itemIds,
    SequenceSchema.ColTs     -> ts,
    SequenceSchema.ColCount  -> n.toString
  )

  "pack/unpack" should "round-trip a simple chunk" in {
    val values = Seq("31", "1029", "1061")
    SequenceCodec.unpack(SequenceCodec.pack(values), 3) shouldBe values
  }

  it should "preserve leading, interior and trailing nulls as empty elements" in {
    val values = Seq("", "4.0", "")
    val packed = SequenceCodec.pack(values)
    packed shouldBe ",4.0,"
    // The whole reason this test exists: String.split(",") without limit -1 would
    // return Array("", "4.0") and silently shift every later column by one row.
    SequenceCodec.unpack(packed, 3) shouldBe values
  }

  it should "round-trip an all-null column" in {
    SequenceCodec.unpack(SequenceCodec.pack(Seq("", "", "")), 3) shouldBe Seq("", "", "")
  }

  it should "treat an empty chunk as zero rows, not one blank row" in {
    SequenceCodec.pack(Seq.empty) shouldBe ""
    SequenceCodec.unpack("", 0) shouldBe Seq.empty
  }

  it should "pad a column shorter than n rather than mis-aligning" in {
    SequenceCodec.unpack("a,b", 4) shouldBe Seq("a", "b", "", "")
  }

  it should "truncate a column longer than n" in {
    SequenceCodec.unpack("a,b,c,d", 2) shouldBe Seq("a", "b")
  }

  "merge" should "append fresh rows after existing rows" in {
    val merged = SequenceCodec.merge(chunk("a,b", "1,2", 2), chunk("c", "3", 1), maxRows = 10)
    merged(SequenceSchema.ColItemId) shouldBe "a,b,c"
    merged(SequenceSchema.ColTs) shouldBe "1,2,3"
    merged(SequenceSchema.ColCount) shouldBe "3"
  }

  it should "return fresh unchanged when existing is empty" in {
    val merged = SequenceCodec.merge(Map.empty, chunk("c", "3", 1), maxRows = 10)
    merged(SequenceSchema.ColItemId) shouldBe "c"
    merged(SequenceSchema.ColCount) shouldBe "1"
  }

  it should "drop the oldest rows on overflow, keeping every column aligned" in {
    val merged = SequenceCodec.merge(chunk("a,b,c", "1,2,3", 3), chunk("d,e", "4,5", 2), maxRows = 3)
    merged(SequenceSchema.ColItemId) shouldBe "c,d,e"
    merged(SequenceSchema.ColTs) shouldBe "3,4,5"
    merged(SequenceSchema.ColCount) shouldBe "3"
  }

  it should "keep null elements aligned across a merge" in {
    val existing = chunk("a,b", "1,2", 2) + (SequenceSchema.ColRating -> ",4.0")
    val fresh    = chunk("c", "3", 1) + (SequenceSchema.ColRating -> "")
    val merged   = SequenceCodec.merge(existing, fresh, maxRows = 10)
    merged(SequenceSchema.ColRating) shouldBe ",4.0,"
    merged(SequenceSchema.ColCount) shouldBe "3"
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceCodecSpec"`
Expected: FAIL — compilation error, `object SequenceCodec is not a member of package com.demo.sequence`.

- [ ] **Step 3: Write minimal implementation**

Create `SequenceCodec.scala`:

```scala
package com.demo.sequence

/** Pure manipulation of packed column strings. Every alignment rule in the store lives
  * here so it can be tested without Spark or Redis. */
object SequenceCodec {

  def pack(values: Seq[String]): String =
    values.map(v => if (v == null) "" else v).mkString(SequenceSchema.RowSeparator)

  /** Split into exactly `n` elements: pad short columns, truncate long ones.
    * The `-1` limit is required — the default drops trailing empty strings. */
  def unpack(packed: String, n: Int): Seq[String] = {
    if (n <= 0) return Seq.empty
    val parts =
      if (packed == null || packed.isEmpty) Array.empty[String]
      else packed.split(SequenceSchema.RowSeparator, -1)
    if (parts.length == n) parts.toSeq
    else if (parts.length > n) parts.take(n).toSeq
    else parts.toSeq ++ Seq.fill(n - parts.length)("")
  }

  /** Concatenate `fresh` after `existing`, keeping the newest `maxRows` rows.
    * Columns absent from either side are treated as all-null so they stay aligned. */
  def merge(
      existing: Map[String, String],
      fresh: Map[String, String],
      maxRows: Int
  ): Map[String, String] = {
    val existingCount = count(existing)
    val freshCount    = count(fresh)
    val total         = existingCount + freshCount
    if (total <= 0) return Map(SequenceSchema.ColCount -> "0")

    val kept = math.min(total, math.max(0, maxRows))
    val drop = total - kept

    val columns = SequenceSchema.Columns.map { column =>
      val combined =
        unpack(existing.getOrElse(column, ""), existingCount) ++
          unpack(fresh.getOrElse(column, ""), freshCount)
      column -> pack(combined.drop(drop))
    }.toMap

    columns + (SequenceSchema.ColCount -> kept.toString)
  }

  private def count(chunk: Map[String, String]): Int =
    chunk.get(SequenceSchema.ColCount)
      .flatMap(v => scala.util.Try(v.trim.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(0)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceCodecSpec"`
Expected: PASS — 10 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceCodec.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceCodecSpec.scala
git commit -m "feat(sequence): add SequenceCodec pack/unpack/merge seam"
```

---

### Task 3: `SequenceEncoder` — events DataFrame to column chunks

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceEncoder.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceEncoderSpec.scala`

**Interfaces:**
- Consumes: `SequenceSchema.bucketColumn`, `SequenceSchema.Columns`, `ColCount`, `ValueSeparator`, `RowSeparator`.
- Produces: `SequenceEncoder.toColumnChunks(events: DataFrame, bucketWidth: String): DataFrame`.
  - **Input columns required:** `user_id: String`, `kind: String`, `item_id: String`, `ts: Long` (epoch millis), `action: String`, `rating: Double` (nullable), `genres: Array[String]` (nullable), `release_year: Int` (nullable).
  - **Output columns:** `user_id: String`, `kind: String`, `bucket: String`, `item_id: String`, `ts: String`, `action: String`, `rating: String`, `genres: String`, `release_year: String`, `n: Long` — the six data columns are packed strings.

- [ ] **Step 1: Write the failing test**

Create `SequenceEncoderSpec.scala`:

```scala
package com.demo.sequence

import com.demo.SparkTestSupport
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceEncoderSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val dayStart = 1784764800000L // 2026-07-23T00:00:00Z

  private def events(rows: Seq[(String, String, String, Long, String, java.lang.Double, Seq[String], java.lang.Integer)]) = {
    val sparkSession = spark
    import sparkSession.implicits._
    rows.toDF("user_id", "kind", "item_id", "ts", "action", "rating", "genres", "release_year")
  }

  "toColumnChunks" should "group by user, kind and bucket with ts-ascending columns" in {
    val df = events(Seq(
      ("u1", "rating", "m2", dayStart + 2000L, "rate", 5.0: java.lang.Double, Seq("Action"), 1999: java.lang.Integer),
      ("u1", "rating", "m1", dayStart + 1000L, "rate", 4.0: java.lang.Double, Seq("Drama", "Comedy"), 1995: java.lang.Integer)
    ))

    val chunk = SequenceEncoder.toColumnChunks(df, "day").collect().head

    chunk.getAs[String]("user_id") shouldBe "u1"
    chunk.getAs[String]("kind") shouldBe "rating"
    chunk.getAs[String]("bucket") shouldBe "20260723"
    chunk.getAs[String]("item_id") shouldBe "m1,m2"
    chunk.getAs[String]("ts") shouldBe s"${dayStart + 1000L},${dayStart + 2000L}"
    chunk.getAs[String]("action") shouldBe "rate,rate"
    chunk.getAs[String]("genres") shouldBe "Drama|Comedy,Action"
    chunk.getAs[String]("release_year") shouldBe "1995,1999"
    chunk.getAs[Long]("n") shouldBe 2L
  }

  it should "split a user across buckets on the UTC day boundary" in {
    val df = events(Seq(
      ("u1", "rating", "m1", dayStart, "rate", 4.0: java.lang.Double, Seq("Drama"), 1995: java.lang.Integer),
      ("u1", "rating", "m2", dayStart + 86400000L, "rate", 4.0: java.lang.Double, Seq("Drama"), 1995: java.lang.Integer)
    ))

    val buckets = SequenceEncoder.toColumnChunks(df, "day")
      .collect().map(_.getAs[String]("bucket")).toSet

    buckets shouldBe Set("20260723", "20260724")
  }

  it should "encode null rating and release_year as empty elements" in {
    val df = events(Seq(
      ("u1", "click", "m1", dayStart + 1000L, "click", null.asInstanceOf[java.lang.Double], Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("u1", "click", "m2", dayStart + 2000L, "click", 3.0: java.lang.Double, Seq.empty[String], 2001: java.lang.Integer)
    ))

    val chunk = SequenceEncoder.toColumnChunks(df, "day").collect().head

    chunk.getAs[String]("rating") shouldBe ",3.0"
    chunk.getAs[String]("release_year") shouldBe ",2001"
    chunk.getAs[String]("genres") shouldBe ","
    chunk.getAs[Long]("n") shouldBe 2L
  }

  it should "strip separators out of genre values so they cannot break alignment" in {
    val df = events(Seq(
      ("u1", "rating", "m1", dayStart + 1000L, "rate", 4.0: java.lang.Double, Seq("Sci-Fi, Fantasy", "Drama"), 1995: java.lang.Integer),
      ("u1", "rating", "m2", dayStart + 2000L, "rate", 4.0: java.lang.Double, Seq("Action"), 1996: java.lang.Integer)
    ))

    val chunk = SequenceEncoder.toColumnChunks(df, "day").collect().head

    chunk.getAs[String]("genres") shouldBe "Sci-Fi Fantasy|Drama,Action"
    chunk.getAs[String]("item_id") shouldBe "m1,m2"
  }

  it should "produce columns whose element count always equals n" in {
    val df = events(Seq(
      ("u1", "rating", "m1", dayStart + 1000L, "rate", null.asInstanceOf[java.lang.Double], Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("u1", "rating", "m2", dayStart + 2000L, "rate", null.asInstanceOf[java.lang.Double], Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("u1", "rating", "m3", dayStart + 3000L, "rate", null.asInstanceOf[java.lang.Double], Seq.empty[String], null.asInstanceOf[java.lang.Integer])
    ))

    val chunk: Row = SequenceEncoder.toColumnChunks(df, "day").collect().head
    val n = chunk.getAs[Long]("n").toInt

    SequenceSchema.Columns.foreach { column =>
      withClue(s"column $column: ") {
        SequenceCodec.unpack(chunk.getAs[String](column), n).length shouldBe n
        chunk.getAs[String](column).split(",", -1).length shouldBe n
      }
    }
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceEncoderSpec"`
Expected: FAIL — compilation error, `object SequenceEncoder is not a member of package com.demo.sequence`.

- [ ] **Step 3: Write minimal implementation**

Create `SequenceEncoder.scala`:

```scala
package com.demo.sequence

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** Turns a flat events DataFrame into one row per `(user_id, kind, bucket)` partition,
  * with each event attribute packed into its own positionally-aligned column string. */
object SequenceEncoder {

  private val RowsField = "__rows"

  def toColumnChunks(events: DataFrame, bucketWidth: String): DataFrame = {
    val sorted = events
      .withColumn("bucket", SequenceSchema.bucketColumn(col(SequenceSchema.ColTs), bucketWidth))
      .groupBy("user_id", "kind", "bucket")
      // sort_array on a struct sorts by its first field (ts) — no UDF needed, same idiom
      // as ItemSequencePreprocessingJob.
      .agg(
        sort_array(collect_list(struct(
          col(SequenceSchema.ColTs).as("ts"),
          col(SequenceSchema.ColItemId).as("item_id"),
          col(SequenceSchema.ColAction).as("action"),
          col(SequenceSchema.ColRating).as("rating"),
          col(SequenceSchema.ColGenres).as("genres"),
          col(SequenceSchema.ColReleaseYear).as("release_year")
        ))).as(RowsField)
      )

    sorted.select(
      col("user_id"),
      col("kind"),
      col("bucket"),
      packScalar("item_id").as(SequenceSchema.ColItemId),
      packScalar("ts").as(SequenceSchema.ColTs),
      packScalar("action").as(SequenceSchema.ColAction),
      packScalar("rating").as(SequenceSchema.ColRating),
      packGenres.as(SequenceSchema.ColGenres),
      packScalar("release_year").as(SequenceSchema.ColReleaseYear),
      size(col(RowsField)).cast("long").as(SequenceSchema.ColCount)
    )
  }

  /** `array_join` skips nulls entirely, which would shorten the column and shift every
    * later row — so each element is coalesced to "" before joining. */
  private def packScalar(field: String): Column =
    array_join(
      transform(col(RowsField), row => coalesce(sanitized(row.getField(field).cast("string")), lit(""))),
      SequenceSchema.RowSeparator
    )

  private def packGenres: Column =
    array_join(
      transform(
        col(RowsField),
        row => coalesce(
          array_join(
            transform(coalesce(row.getField("genres"), array()), g => sanitized(g)),
            SequenceSchema.ValueSeparator
          ),
          lit("")
        )
      ),
      SequenceSchema.RowSeparator
    )

  private def sanitized(value: Column): Column =
    regexp_replace(value, "[,|]", "")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceEncoderSpec"`
Expected: PASS — 5 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceEncoder.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceEncoderSpec.scala
git commit -m "feat(sequence): add SequenceEncoder producing time-bucketed column chunks"
```

---

### Task 4: `SequenceRedisSink` — append/overwrite with the two-phase Jedis dance

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceRedisSink.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceRedisSinkSpec.scala`

**Interfaces:**
- Consumes: `SequenceCodec.merge`, `SequenceSchema.key`/`Columns`/`ColCount`, `com.demo.engine.Sink`, `com.demo.engine.RedisPool`, `com.demo.engine.RedisSink.foreachWithFlush`.
- Produces:
  - `sealed trait SequenceWriteMode`, `object SequenceWriteMode { case object Append; case object Overwrite }`
  - `class SequenceRedisSink(host: String, port: Int, poolMax: Int, pipelineSize: Int, ttlSeconds: Int, maxRowsPerBucket: Int, mode: SequenceWriteMode) extends Sink`
  - `object SequenceRedisSink.chunkFields(row: Row): Map[String, String]` — the pure Row→field-map seam.
  - `object SequenceRedisSink.resolve(existing: Map[String, String], fresh: Map[String, String], maxRows: Int, mode: SequenceWriteMode): Map[String, String]` — the pure mode-dispatch seam.

- [ ] **Step 1: Write the failing test**

Following the repo convention set by `RedisSinkSpec`, this tests the pure seams and never opens a Redis connection.

Create `SequenceRedisSinkSpec.scala`:

```scala
package com.demo.sequence

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceRedisSinkSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val existing = Map(
    SequenceSchema.ColItemId -> "a,b",
    SequenceSchema.ColTs     -> "1,2",
    SequenceSchema.ColCount  -> "2"
  )
  private val fresh = Map(
    SequenceSchema.ColItemId -> "c",
    SequenceSchema.ColTs     -> "3",
    SequenceSchema.ColCount  -> "1"
  )

  // NOTE: all arguments positional — in Scala 2.12 a positional argument may not follow
  // a named one, so `resolve(existing, fresh, maxRows = 10, Append)` would not compile.
  "resolve" should "merge existing and fresh rows in Append mode" in {
    val resolved = SequenceRedisSink.resolve(existing, fresh, 10, SequenceWriteMode.Append)
    resolved(SequenceSchema.ColItemId) shouldBe "a,b,c"
    resolved(SequenceSchema.ColCount) shouldBe "3"
  }

  it should "ignore existing rows entirely in Overwrite mode" in {
    val resolved = SequenceRedisSink.resolve(existing, fresh, 10, SequenceWriteMode.Overwrite)
    resolved(SequenceSchema.ColItemId) shouldBe "c"
    resolved(SequenceSchema.ColCount) shouldBe "1"
  }

  it should "cap a bucket at maxRows in Append mode" in {
    val resolved = SequenceRedisSink.resolve(existing, fresh, 2, SequenceWriteMode.Append)
    resolved(SequenceSchema.ColItemId) shouldBe "b,c"
    resolved(SequenceSchema.ColCount) shouldBe "2"
  }

  "chunkFields" should "extract every schema column plus n from a chunk Row" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val row = Seq(("u1", "rating", "20260723", "m1,m2", "1,2", "rate,rate", ",4.0", "Drama,", "1995,", 2L))
      .toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")
      .collect()
      .head

    val fields = SequenceRedisSink.chunkFields(row)

    fields(SequenceSchema.ColItemId) shouldBe "m1,m2"
    fields(SequenceSchema.ColRating) shouldBe ",4.0"
    fields(SequenceSchema.ColCount) shouldBe "2"
    fields.keySet shouldBe (SequenceSchema.Columns.toSet + SequenceSchema.ColCount)
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceRedisSinkSpec"`
Expected: FAIL — compilation error, `not found: value SequenceRedisSink`.

- [ ] **Step 3: Write minimal implementation**

Create `SequenceRedisSink.scala`:

```scala
package com.demo.sequence

import com.demo.engine.{RedisPool, RedisSink, Sink}
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

sealed trait SequenceWriteMode
object SequenceWriteMode {
  /** Streaming producers: read the existing bucket and concatenate. */
  case object Append extends SequenceWriteMode
  /** Backfill: replace the bucket outright, so re-runs are idempotent and skip the read. */
  case object Overwrite extends SequenceWriteMode
}

/** Persists column chunks to Redis, one HASH per `(user, kind, bucket)` partition. */
class SequenceRedisSink(
    host: String,
    port: Int,
    poolMax: Int,
    pipelineSize: Int,
    ttlSeconds: Int,
    maxRowsPerBucket: Int,
    mode: SequenceWriteMode
) extends Sink {

  def write(batch: DataFrame, batchId: Long): Unit = {
    // Copy to locals so the partition closure doesn't capture `this`.
    val h = host; val pt = port; val mx = poolMax; val ps = pipelineSize
    val ttl = ttlSeconds; val cap = maxRowsPerBucket; val m = mode
    val fields = (SequenceSchema.Columns :+ SequenceSchema.ColCount).toArray

    batch.foreachPartition { rows: Iterator[Row] =>
      val log = LoggerFactory.getLogger(classOf[SequenceRedisSink])
      val jedis = RedisPool.get(h, pt, mx).getResource
      try {
        // Phase 1 — direct, NON-pipelined reads. Issuing a read inside an open pipeline
        // makes it consume buffered pipeline replies and corrupts the connection.
        val writes = scala.collection.mutable.ArrayBuffer.empty[(String, java.util.Map[String, String])]
        rows.foreach { row =>
          try {
            val key = SequenceSchema.key(
              row.getAs[String]("user_id"), row.getAs[String]("kind"), row.getAs[String]("bucket")
            )
            val fresh = SequenceRedisSink.chunkFields(row)
            val existing = m match {
              case SequenceWriteMode.Overwrite => Map.empty[String, String]
              case SequenceWriteMode.Append =>
                val values = jedis.hmget(key, fields: _*).asScala
                fields.zip(values).collect { case (f, v) if v != null => f -> v }.toMap
            }
            writes += key -> SequenceRedisSink.resolve(existing, fresh, cap, m).asJava
          } catch {
            case e: Exception => log.warn("Skipping sequence chunk: {}", e.getMessage)
          }
        }

        // Phase 2 — all writes through one pipeline, no reads interleaved.
        if (writes.nonEmpty) {
          val p = jedis.pipelined()
          RedisSink.foreachWithFlush(writes.iterator, ps) { case (key, f) =>
            p.hset(key, f)
            p.expire(key, ttl)
          }(() => p.sync())
        }
      } finally {
        jedis.close()
      }
    }
  }
}

object SequenceRedisSink {

  /** Row → field map. Pure, so the column extraction is testable without Redis. */
  def chunkFields(row: Row): Map[String, String] = {
    val data = SequenceSchema.Columns.map { column =>
      column -> Option(row.getAs[String](column)).getOrElse("")
    }.toMap
    data + (SequenceSchema.ColCount -> row.getAs[Long](SequenceSchema.ColCount).toString)
  }

  /** Mode dispatch. Pure, so both write modes are testable without Redis. */
  def resolve(
      existing: Map[String, String],
      fresh: Map[String, String],
      maxRows: Int,
      mode: SequenceWriteMode
  ): Map[String, String] = mode match {
    case SequenceWriteMode.Overwrite => SequenceCodec.merge(Map.empty, fresh, maxRows)
    case SequenceWriteMode.Append    => SequenceCodec.merge(existing, fresh, maxRows)
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceRedisSinkSpec"`
Expected: PASS — 4 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceRedisSink.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceRedisSinkSpec.scala
git commit -m "feat(sequence): add SequenceRedisSink with append and overwrite modes"
```

---

### Task 5: `SequenceParquetSink` — the offline mirror

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceParquetSink.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceParquetSinkSpec.scala`

**Interfaces:**
- Consumes: `SequenceSchema.Columns`/`ColCount`, `SequenceCodec` (indirectly via the same packing rules), `com.demo.engine.Sink`.
- Produces:
  - `class SequenceParquetSink(outputPath: String, mode: SequenceWriteMode) extends Sink`
  - `object SequenceParquetSink.explodeChunks(chunks: DataFrame): DataFrame` — chunk rows → one row per event, columns `user_id, kind, bucket, item_id, ts, action, rating, genres, release_year`, all `String` except `ts: Long`.

- [ ] **Step 1: Write the failing test**

Create `SequenceParquetSinkSpec.scala`:

```scala
package com.demo.sequence

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceParquetSinkSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def chunks = {
    val sparkSession = spark
    import sparkSession.implicits._
    Seq(
      ("u1", "rating", "20260723", "m1,m2", "1000,2000", "rate,rate", ",4.0", "Drama|Comedy,", "1995,", 2L)
    ).toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")
  }

  "explodeChunks" should "produce one row per event with columns unpacked and aligned" in {
    val rows = SequenceParquetSink.explodeChunks(chunks)
      .orderBy("ts")
      .collect()

    rows.length shouldBe 2

    rows(0).getAs[String]("item_id") shouldBe "m1"
    rows(0).getAs[Long]("ts") shouldBe 1000L
    rows(0).getAs[String]("rating") shouldBe ""
    rows(0).getAs[String]("genres") shouldBe "Drama|Comedy"
    rows(0).getAs[String]("release_year") shouldBe "1995"

    rows(1).getAs[String]("item_id") shouldBe "m2"
    rows(1).getAs[Long]("ts") shouldBe 2000L
    rows(1).getAs[String]("rating") shouldBe "4.0"
    rows(1).getAs[String]("genres") shouldBe ""
    rows(1).getAs[String]("release_year") shouldBe ""
  }

  it should "carry the partition columns onto every exploded row" in {
    val row = SequenceParquetSink.explodeChunks(chunks).collect().head
    row.getAs[String]("user_id") shouldBe "u1"
    row.getAs[String]("kind") shouldBe "rating"
    row.getAs[String]("bucket") shouldBe "20260723"
  }

  it should "emit nothing for a zero-row chunk" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val empty = Seq(("u1", "rating", "20260723", "", "", "", "", "", "", 0L))
      .toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")

    SequenceParquetSink.explodeChunks(empty).count() shouldBe 0L
  }

  "write" should "partition the output by bucket and kind" in {
    val path = java.nio.file.Files.createTempDirectory("seq-parquet").toString + "/out"
    new SequenceParquetSink(path, SequenceWriteMode.Overwrite).write(chunks, 0L)

    val readBack = spark.read.parquet(path)
    readBack.count() shouldBe 2L
    readBack.columns should contain allOf ("bucket", "kind")
    new java.io.File(path).list().toSeq.filter(_.startsWith("bucket=")) shouldBe Seq("bucket=20260723")
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceParquetSinkSpec"`
Expected: FAIL — compilation error, `not found: value SequenceParquetSink`.

- [ ] **Step 3: Write minimal implementation**

Create `SequenceParquetSink.scala`:

```scala
package com.demo.sequence

import com.demo.engine.Sink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** The offline mirror of `SequenceRedisSink`. Parquet does its own columnar encoding,
  * so chunks are exploded back to one row per event rather than stored packed. */
class SequenceParquetSink(outputPath: String, mode: SequenceWriteMode) extends Sink {

  def write(batch: DataFrame, batchId: Long): Unit = {
    val writeMode = mode match {
      case SequenceWriteMode.Overwrite => "overwrite"
      case SequenceWriteMode.Append    => "append"
    }
    SequenceParquetSink.explodeChunks(batch)
      .write
      .mode(writeMode)
      .partitionBy("bucket", "kind")
      .parquet(outputPath)
  }
}

object SequenceParquetSink {

  private val IndexField = "__i"

  def explodeChunks(chunks: DataFrame): DataFrame = {
    val indexed = chunks
      .filter(col(SequenceSchema.ColCount) > 0)
      .withColumn(
        IndexField,
        explode(sequence(lit(0), col(SequenceSchema.ColCount).cast("int") - 1))
      )

    val unpacked = SequenceSchema.Columns.foldLeft(indexed) { (df, column) =>
      // split(..., -1) keeps trailing empty elements; the default would drop them
      // and shift every row whose last value is null.
      df.withColumn(column, element_at(split(col(column), ",", -1), col(IndexField) + 1))
    }

    unpacked.select(
      col("user_id"),
      col("kind"),
      col("bucket"),
      col(SequenceSchema.ColItemId),
      col(SequenceSchema.ColTs).cast("long").as(SequenceSchema.ColTs),
      col(SequenceSchema.ColAction),
      col(SequenceSchema.ColRating),
      col(SequenceSchema.ColGenres),
      col(SequenceSchema.ColReleaseYear)
    )
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceParquetSinkSpec"`
Expected: PASS — 4 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceParquetSink.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceParquetSinkSpec.scala
git commit -m "feat(sequence): add SequenceParquetSink mirroring chunks to partitioned Parquet"
```

---

### Task 6: `SequenceJobConfig` + `SequenceSinks` — the shared producer wiring

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceJobConfig.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceJobConfigSpec.scala`

**Interfaces:**
- Consumes: `com.demo.util.Env`, `SequenceRedisSink`, `SequenceParquetSink`, `SequenceWriteMode`.
- Produces:
  - `final case class SequenceJobConfig(bucketWidth: String, lookbackDays: Int, maxRowsPerBucket: Int, parquetPath: Option[String])`
  - `SequenceJobConfig.fromEnv(): SequenceJobConfig`
  - `SequenceJobConfig.ttlSeconds: Int` (instance method — `lookbackDays * 24 * 3600`)
  - `SequenceSinks.write(chunks: DataFrame, cfg: SequenceJobConfig, redisHost: String, redisPort: Int, poolMax: Int, pipelineSize: Int, mode: SequenceWriteMode, batchId: Long): Unit`

Both producers (Tasks 7 and 8) and the backfill (Task 9) call `SequenceSinks.write`. Without this, the same four config reads and the same persist/try/two-sink/finally block appear verbatim in three places.

`SequenceSinks.write` owns the `persist` / `try` / `finally unpersist` around the two sink writes — `chunks` is consumed twice (Redis and Parquet), so without the persist Spark recomputes the whole `groupBy`.

- [ ] **Step 1: Write the failing test**

Create `SequenceJobConfigSpec.scala`:

```scala
package com.demo.sequence

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceJobConfigSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "fromEnv" should "apply the documented defaults when nothing is set" in {
    // The suite does not set SEQ_* env vars, so this exercises the default path.
    val cfg = SequenceJobConfig.fromEnv()
    cfg.bucketWidth shouldBe "day"
    cfg.lookbackDays shouldBe 90
    cfg.maxRowsPerBucket shouldBe 500
    cfg.parquetPath shouldBe None
  }

  "ttlSeconds" should "convert the lookback window into seconds" in {
    SequenceJobConfig("day", 90, 500, None).ttlSeconds shouldBe 90 * 24 * 3600
    SequenceJobConfig("day", 1, 500, None).ttlSeconds shouldBe 86400
  }

  "SequenceSinks.write" should "write Parquet when a path is configured" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val chunks = Seq(("u1", "rating", "20260723", "m1,m2", "1000,2000", "rate,rate", ",4.0", ",", "1995,", 2L))
      .toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")
    val path = java.nio.file.Files.createTempDirectory("seq-sinks").toString + "/out"

    SequenceSinks.write(
      chunks, SequenceJobConfig("day", 90, 500, Some(path)),
      redisHost = "unused", redisPort = 0, poolMax = 1, pipelineSize = 10,
      mode = SequenceWriteMode.Overwrite, batchId = 0L, writeRedis = false
    )

    spark.read.parquet(path).count() shouldBe 2L
  }

  it should "skip Parquet entirely when no path is configured" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val chunks = Seq(("u1", "rating", "20260723", "m1", "1000", "rate", "4.0", "", "1995", 1L))
      .toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")

    // No Redis, no Parquet path — must be a clean no-op rather than an NPE or a
    // write to some default location.
    noException should be thrownBy SequenceSinks.write(
      chunks, SequenceJobConfig("day", 90, 500, None),
      redisHost = "unused", redisPort = 0, poolMax = 1, pipelineSize = 10,
      mode = SequenceWriteMode.Append, batchId = 0L, writeRedis = false
    )
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceJobConfigSpec"`
Expected: FAIL — compilation error, `not found: value SequenceJobConfig`.

- [ ] **Step 3: Write minimal implementation**

Create `SequenceJobConfig.scala`:

```scala
package com.demo.sequence

import com.demo.util.Env
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/** Shared sequence-store knobs, read once per job. */
final case class SequenceJobConfig(
    bucketWidth: String,
    lookbackDays: Int,
    maxRowsPerBucket: Int,
    parquetPath: Option[String]
) {
  def ttlSeconds: Int = lookbackDays * 24 * 3600
}

object SequenceJobConfig {
  def fromEnv(): SequenceJobConfig = SequenceJobConfig(
    bucketWidth      = sys.env.getOrElse("SEQ_BUCKET_WIDTH", "day"),
    lookbackDays     = math.max(1, Env.int("SEQ_LOOKBACK_DAYS", 90)),
    maxRowsPerBucket = math.max(1, Env.int("SEQ_MAX_ROWS_PER_BUCKET", 500)),
    parquetPath      = sys.env.get("SEQ_PARQUET_PATH").filter(_.nonEmpty)
  )
}

/** Fans one chunk DataFrame out to the Redis and Parquet sinks. Shared by both
  * streaming producers and the backfill job. */
object SequenceSinks {

  def write(
      chunks: DataFrame,
      cfg: SequenceJobConfig,
      redisHost: String,
      redisPort: Int,
      poolMax: Int,
      pipelineSize: Int,
      mode: SequenceWriteMode,
      batchId: Long,
      writeRedis: Boolean = true
  ): Unit = {
    // chunks is consumed by up to two sinks; without persist Spark recomputes the groupBy.
    chunks.persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      if (writeRedis) {
        new SequenceRedisSink(
          redisHost, redisPort, poolMax, pipelineSize,
          cfg.ttlSeconds, cfg.maxRowsPerBucket, mode
        ).write(chunks, batchId)
      }
      cfg.parquetPath.foreach { path =>
        new SequenceParquetSink(path, mode).write(chunks, batchId)
      }
    } finally {
      chunks.unpersist()
    }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceJobConfigSpec"`
Expected: PASS — 4 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceJobConfig.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceJobConfigSpec.scala
git commit -m "feat(sequence): add shared SequenceJobConfig and SequenceSinks fan-out"
```

---

### Task 7: `kind=rating` producer in `MovieLensContextCollectorStreamingJob`

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/MovieLensContextCollectorStreamingJob.scala` (add a method; extend `main`'s `foreachBatch`)
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/MovieLensContextCollectorStreamingJobSpec.scala` (add cases only — do not change existing ones)

**Interfaces:**
- Consumes: `SequenceEncoder.toColumnChunks`, `SequenceSchema.KindRating`, `SequenceJobConfig.fromEnv`, `SequenceSinks.write`, `SequenceWriteMode.Append`.
- Produces: `MovieLensContextCollectorStreamingJob.buildSequenceEvents(events: DataFrame): DataFrame` with columns `user_id, kind, item_id, ts, action, rating, genres, release_year`.

**Critical detail:** this job's `timestamp` field is in **seconds** (MovieLens convention). The store is millisecond-based, so `buildSequenceEvents` multiplies by 1000.

- [ ] **Step 1: Write the failing test**

Append to `MovieLensContextCollectorStreamingJobSpec.scala` (inside the existing class, after the last test):

```scala
  "buildSequenceEvents" should "project rating events into sequence-store shape with millisecond timestamps" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("rating", "u1", "m1", 4.0: java.lang.Double, 105L, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq("Drama"), 1995: java.lang.Integer),
      ("user_update", "u1", null, null.asInstanceOf[java.lang.Double], 100L, 31: java.lang.Integer, "F", "engineer", "10001", null, Seq.empty[String], null)
    ).toDF("event_kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    val rows = MovieLensContextCollectorStreamingJob.buildSequenceEvents(events).collect()

    rows.length shouldBe 1
    rows.head.getAs[String]("user_id") shouldBe "u1"
    rows.head.getAs[String]("kind") shouldBe "rating"
    rows.head.getAs[String]("item_id") shouldBe "m1"
    rows.head.getAs[Long]("ts") shouldBe 105000L
    rows.head.getAs[String]("action") shouldBe "rate"
    rows.head.getAs[Double]("rating") shouldBe 4.0
    rows.head.getAs[Seq[String]]("genres") shouldBe Seq("Drama")
    rows.head.getAs[Int]("release_year") shouldBe 1995
  }

  it should "drop rows missing a user, item or timestamp" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("rating", null, "m1", 4.0: java.lang.Double, 105L, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq.empty[String], null.asInstanceOf[java.lang.Integer]),
      ("rating", "u1", null, 4.0: java.lang.Double, 105L, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq.empty[String], null),
      ("rating", "u1", "m1", 4.0: java.lang.Double, null.asInstanceOf[java.lang.Long], null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq.empty[String], null)
    ).toDF("event_kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    MovieLensContextCollectorStreamingJob.buildSequenceEvents(events).count() shouldBe 0L
  }

  it should "feed SequenceEncoder to produce one chunk per user and day" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("rating", "u1", "m1", 4.0: java.lang.Double, 1784764801L, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq("Drama"), 1995: java.lang.Integer),
      ("rating", "u1", "m2", 5.0: java.lang.Double, 1784764802L, null.asInstanceOf[java.lang.Integer], null, null, null, null, Seq("Action"), 1999: java.lang.Integer)
    ).toDF("event_kind", "user_id", "item_id", "rating", "timestamp", "age", "gender", "occupation", "zip_code", "title", "genres", "release_year")

    val chunk = com.demo.sequence.SequenceEncoder
      .toColumnChunks(MovieLensContextCollectorStreamingJob.buildSequenceEvents(events), "day")
      .collect()

    chunk.length shouldBe 1
    chunk.head.getAs[String]("bucket") shouldBe "20260723"
    chunk.head.getAs[String]("item_id") shouldBe "m1,m2"
    chunk.head.getAs[Long]("n") shouldBe 2L
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.process.MovieLensContextCollectorStreamingJobSpec"`
Expected: FAIL — compilation error, `value buildSequenceEvents is not a member of object MovieLensContextCollectorStreamingJob`.

- [ ] **Step 3: Write minimal implementation**

In `MovieLensContextCollectorStreamingJob.scala`, add this import at the top with the others:

```scala
import com.demo.sequence.{SequenceEncoder, SequenceJobConfig, SequenceSchema, SequenceSinks, SequenceWriteMode}
```

Add this method after `buildMovieFeatureUpdates`:

```scala
  /** Projects rating events into the sequence-store event shape.
    * This job's `timestamp` is in seconds (MovieLens convention); the sequence store is
    * millisecond-based, so it is scaled here at the producer boundary. */
  def buildSequenceEvents(events: DataFrame): DataFrame =
    events
      .filter(
        col("event_kind") === "rating" &&
          col("user_id").isNotNull &&
          col("item_id").isNotNull &&
          col("timestamp").isNotNull
      )
      .select(
        col("user_id"),
        lit(SequenceSchema.KindRating).as("kind"),
        col("item_id"),
        (col("timestamp") * 1000L).cast("long").as("ts"),
        lit("rate").as("action"),
        col("rating").cast("double").as("rating"),
        col("genres"),
        col("release_year").cast("int").as("release_year")
      )
```

In `main`, add one config read after `recentRatingsLimit`:

```scala
    val sequenceConfig = SequenceJobConfig.fromEnv()
```

and extend the `foreachBatch` body — the existing two `write*Updates` calls stay exactly as they are:

```scala
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val userUpdates = buildUserFeatureUpdates(batch, recentRatingsLimit)
        val movieUpdates = buildMovieFeatureUpdates(batch)
        writeUserUpdates(userUpdates, redisHost, redisPort, redisPoolMaxTotal, redisPipelineSize, contextTtlSeconds, recentRatingsLimit)
        writeMovieUpdates(movieUpdates, redisHost, redisPort, redisPoolMaxTotal, redisPipelineSize, contextTtlSeconds)

        SequenceSinks.write(
          SequenceEncoder.toColumnChunks(buildSequenceEvents(batch), sequenceConfig.bucketWidth),
          sequenceConfig, redisHost, redisPort, redisPoolMaxTotal, redisPipelineSize,
          SequenceWriteMode.Append, batchId
        )
      }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.process.MovieLensContextCollectorStreamingJobSpec"`
Expected: PASS — the 3 new tests plus every pre-existing test in the spec, unmodified.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/MovieLensContextCollectorStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/MovieLensContextCollectorStreamingJobSpec.scala
git commit -m "feat(sequence): write rating sequences from the context collector"
```

---

### Task 8: `kind=click` producer in `UserEventStreamingJob`

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/UserEventStreamingJob.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/UserEventStreamingJobSpec.scala` (add cases only)

**Interfaces:**
- Consumes: `SequenceEncoder.toColumnChunks`, `SequenceSchema.KindClick`, `SequenceJobConfig.fromEnv`, `SequenceSinks.write`, `SequenceWriteMode.Append`.
- Produces: `UserEventStreamingJob.buildSequenceEvents(batch: DataFrame): DataFrame` with columns `user_id, kind, item_id, ts, action, rating, genres, release_year`.

**Note:** this job's parsed events already carry `timestamp_ms`, so no scaling is needed. Per-user click sequences do not exist today — this is new capability, not a migration. `rating`, `genres`, and `release_year` are null for clicks; they are still emitted so the chunk schema is uniform across kinds.

- [ ] **Step 1: Write the failing test**

Append to `UserEventStreamingJobSpec.scala` (inside the existing class, after the last test):

```scala
  "buildSequenceEvents" should "project click events into sequence-store shape" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val batch = Seq(
      ("u1", "m1", "click", 1784764801000L),
      ("u1", "m2", "click", 1784764802000L)
    ).toDF("user_id", "item_id", "event_type", "timestamp_ms")

    val rows = UserEventStreamingJob.buildSequenceEvents(batch).orderBy("ts").collect()

    rows.length shouldBe 2
    rows.head.getAs[String]("user_id") shouldBe "u1"
    rows.head.getAs[String]("kind") shouldBe "click"
    rows.head.getAs[String]("item_id") shouldBe "m1"
    rows.head.getAs[Long]("ts") shouldBe 1784764801000L
    rows.head.getAs[String]("action") shouldBe "click"
    rows.head.isNullAt(rows.head.fieldIndex("rating")) shouldBe true
  }

  it should "produce one chunk per user and day through SequenceEncoder" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val batch = Seq(
      ("u1", "m1", "click", 1784764801000L),
      ("u1", "m2", "click", 1784764802000L),
      ("u1", "m3", "click", 1784851201000L)
    ).toDF("user_id", "item_id", "event_type", "timestamp_ms")

    val chunks = com.demo.sequence.SequenceEncoder
      .toColumnChunks(UserEventStreamingJob.buildSequenceEvents(batch), "day")
      .orderBy("bucket")
      .collect()

    chunks.length shouldBe 2
    chunks(0).getAs[String]("bucket") shouldBe "20260723"
    chunks(0).getAs[String]("item_id") shouldBe "m1,m2"
    chunks(1).getAs[String]("bucket") shouldBe "20260724"
    chunks(1).getAs[String]("item_id") shouldBe "m3"
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.UserEventStreamingJobSpec"`
Expected: FAIL — compilation error, `value buildSequenceEvents is not a member of object UserEventStreamingJob`.

- [ ] **Step 3: Write minimal implementation**

In `UserEventStreamingJob.scala`, replace the existing engine import line

```scala
import com.demo.engine.RedisSink
```

with

```scala
import com.demo.engine.RedisSink
import com.demo.sequence.{SequenceEncoder, SequenceJobConfig, SequenceSchema, SequenceSinks, SequenceWriteMode}
```

Add this method after `itemClickCounts`:

```scala
  /** Projects click events into the sequence-store event shape. `timestamp_ms` is already
    * milliseconds. rating/genres/release_year are null for clicks but still emitted so the
    * chunk schema is identical across kinds. */
  def buildSequenceEvents(batch: DataFrame): DataFrame =
    batch
      .filter(col("user_id").isNotNull && col("item_id").isNotNull && col("timestamp_ms").isNotNull)
      .select(
        col("user_id"),
        lit(SequenceSchema.KindClick).as("kind"),
        col("item_id"),
        col("timestamp_ms").cast("long").as("ts"),
        lit("click").as("action"),
        lit(null).cast("double").as("rating"),
        lit(null).cast("array<string>").as("genres"),
        lit(null).cast("int").as("release_year")
      )
```

In `main`, add one config read after `redisPoolMaxTotal`:

```scala
    val sequenceConfig = SequenceJobConfig.fromEnv()
```

and extend the `foreachBatch` body — the existing popularity sink stays exactly as it is:

```scala
    parsed.writeStream.foreachBatch { (batch: DataFrame, batchId: Long) =>
      val counts = itemClickCounts(batch)
      new RedisSink(redisHost, redisPort, redisPoolMaxTotal, redisPipelineSize,
        (p, r) => p.zincrby("global:item_popularity",
                            r.getAs[Long]("count").toDouble, r.getAs[String]("item_id"))
      ).write(counts, batchId)

      SequenceSinks.write(
        SequenceEncoder.toColumnChunks(buildSequenceEvents(batch), sequenceConfig.bucketWidth),
        sequenceConfig, redisHost, redisPort, redisPoolMaxTotal, redisPipelineSize,
        SequenceWriteMode.Append, batchId
      )
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.UserEventStreamingJobSpec"`
Expected: PASS — the 2 new tests plus every pre-existing test in the spec, unmodified.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/UserEventStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/UserEventStreamingJobSpec.scala
git commit -m "feat(sequence): write per-user click sequences from UserEventStreamingJob"
```

---

### Task 9: `SequenceBackfillJob`

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceBackfillJob.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceBackfillJobSpec.scala`

**Interfaces:**
- Consumes: `SequenceEncoder.toColumnChunks`, `SequenceSchema.KindRating`, `SequenceJobConfig.fromEnv`, `SequenceSinks.write`, `SequenceWriteMode.Overwrite`, `com.demo.util.{Env, SparkSessions}`.
- Produces: `SequenceBackfillJob.readRatings(spark: SparkSession, ratingsPath: String): DataFrame` — sequence-store event shape from the MovieLens ratings CSV (`userId,movieId,rating,timestamp`, timestamp in seconds).

Shadow mode is meaningless against an empty store, so this job exists to make the first diff honest. `Overwrite` makes re-runs idempotent and skips the phase-1 read entirely.

- [ ] **Step 1: Write the failing test**

Create `SequenceBackfillJobSpec.scala`:

```scala
package com.demo.sequence

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

class SequenceBackfillJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def ratingsCsv(): String = {
    val dir = Files.createTempDirectory("seq-backfill")
    val file = dir.resolve("ratings.csv")
    Files.write(file, java.util.Arrays.asList(
      "userId,movieId,rating,timestamp",
      "u1,m1,4.0,1784764801",
      "u1,m2,5.0,1784764802",
      "u2,m1,3.0,1784851201"
    ))
    file.toString
  }

  "readRatings" should "project the ratings CSV into sequence-store event shape" in {
    val rows = SequenceBackfillJob.readRatings(spark, ratingsCsv()).orderBy("user_id", "ts").collect()

    rows.length shouldBe 3
    rows.head.getAs[String]("user_id") shouldBe "u1"
    rows.head.getAs[String]("kind") shouldBe "rating"
    rows.head.getAs[String]("item_id") shouldBe "m1"
    rows.head.getAs[Long]("ts") shouldBe 1784764801000L
    rows.head.getAs[String]("action") shouldBe "rate"
    rows.head.getAs[Double]("rating") shouldBe 4.0
  }

  it should "chunk into one partition per user and day" in {
    val chunks = SequenceEncoder
      .toColumnChunks(SequenceBackfillJob.readRatings(spark, ratingsCsv()), "day")
      .orderBy("user_id", "bucket")
      .collect()

    chunks.length shouldBe 2
    chunks(0).getAs[String]("user_id") shouldBe "u1"
    chunks(0).getAs[String]("bucket") shouldBe "20260723"
    chunks(0).getAs[String]("item_id") shouldBe "m1,m2"
    chunks(1).getAs[String]("user_id") shouldBe "u2"
    chunks(1).getAs[String]("bucket") shouldBe "20260724"
  }

  it should "be idempotent under Overwrite: a second run reproduces the same Parquet" in {
    val events = SequenceBackfillJob.readRatings(spark, ratingsCsv())
    val chunks = SequenceEncoder.toColumnChunks(events, "day")
    val path = Files.createTempDirectory("seq-backfill-out").toString + "/out"

    new SequenceParquetSink(path, SequenceWriteMode.Overwrite).write(chunks, 0L)
    val first = spark.read.parquet(path).count()
    new SequenceParquetSink(path, SequenceWriteMode.Overwrite).write(chunks, 1L)
    val second = spark.read.parquet(path).count()

    first shouldBe 3L
    second shouldBe 3L   // not 6 — Overwrite replaces rather than appending
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceBackfillJobSpec"`
Expected: FAIL — compilation error, `not found: value SequenceBackfillJob`.

- [ ] **Step 3: Write minimal implementation**

Create `SequenceBackfillJob.scala`:

```scala
package com.demo.sequence

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** One-shot backfill of the columnar sequence store from the historical ratings CSV.
  * Runs in Overwrite mode, so it is idempotent and skips the read-merge phase entirely. */
object SequenceBackfillJob {

  private val RatingsSchema = StructType(Seq(
    StructField("userId", StringType),
    StructField("movieId", StringType),
    StructField("rating", DoubleType),
    StructField("timestamp", LongType)
  ))

  def main(args: Array[String]): Unit = {
    val ratingsPath = Env.requiredArgOrEnv(args, 0, "RATINGS_INPUT_PATH", "ratings input path")
    val redisHost   = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort   = Env.int("REDIS_PORT", 6379)
    val poolMax     = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))
    val pipelineSz  = math.max(3, Env.int("REDIS_PIPELINE_SIZE", 500))
    val cfg         = SequenceJobConfig.fromEnv()

    val spark = SparkSessions.create("SequenceBackfillJob")
    try {
      SequenceSinks.write(
        SequenceEncoder.toColumnChunks(readRatings(spark, ratingsPath), cfg.bucketWidth),
        cfg, redisHost, redisPort, poolMax, pipelineSz,
        SequenceWriteMode.Overwrite, 0L
      )
    } finally {
      spark.stop()
    }
  }

  /** MovieLens ratings CSV → sequence-store event shape. `timestamp` is in seconds. */
  def readRatings(spark: SparkSession, ratingsPath: String): DataFrame =
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(RatingsSchema)
      .load(ratingsPath)
      .filter(col("userId").isNotNull && col("movieId").isNotNull && col("timestamp").isNotNull)
      .select(
        col("userId").as("user_id"),
        lit(SequenceSchema.KindRating).as("kind"),
        col("movieId").as("item_id"),
        (col("timestamp") * 1000L).cast("long").as("ts"),
        lit("rate").as("action"),
        col("rating").cast("double").as("rating"),
        lit(null).cast("array<string>").as("genres"),
        lit(null).cast("int").as("release_year")
      )
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.sequence.SequenceBackfillJobSpec"`
Expected: PASS — 3 tests.

Then run the whole Scala suite to confirm nothing regressed:

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt test`
Expected: PASS — all specs, including every pre-existing one.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sequence/SequenceBackfillJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/sequence/SequenceBackfillJobSpec.scala
git commit -m "feat(sequence): add idempotent SequenceBackfillJob"
```

---

### Task 10: Java schema constants, codec and slice

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/SequenceSchemaConstants.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/SequenceCodec.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/SequenceSlice.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/sequence/SequenceCodecTest.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/resources/sequence-schema.json` (byte-identical to the Scala one from Task 1)

**Interfaces:**
- Consumes: the shared `sequence-schema.json` fixture.
- Produces:
  - `SequenceSchemaConstants.KEY_PREFIX/KIND_RATING/KIND_CLICK/COL_ITEM_ID/COL_TS/COL_ACTION/COL_RATING/COL_GENRES/COL_RELEASE_YEAR/COL_COUNT: String`, `COLUMNS: List<String>`, `ROW_SEPARATOR/VALUE_SEPARATOR: String`, `String key(String userId, String kind, String bucket)`, `String bucket(long epochMillis)`.
  - `SequenceCodec.unpack(String packed, int n): List<String>`
  - `SequenceCodec.rowCount(Map<String, String> fields): int` — reads `n`, then clamps to the shortest present data column.
  - `SequenceSlice` — `SequenceSlice(Map<String, List<String>> columns, int size)`, `List<String> column(String name)`, `int size()`, `List<String> itemIds()`, `List<Long> timestamps()`, `static SequenceSlice empty()`.

- [ ] **Step 1: Write the failing test**

Create `SequenceCodecTest.java`:

```java
package com.demo.retrieval.service.sequence;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequenceCodecTest {

    @Test
    void unpackRoundTripsASimpleColumn() {
        assertEquals(List.of("31", "1029", "1061"), SequenceCodec.unpack("31,1029,1061", 3));
    }

    @Test
    void unpackPreservesTrailingNullsAsEmptyElements() {
        // String.split(",") without limit -1 returns ["", "4.0"] and shifts every later row.
        assertEquals(List.of("", "4.0", ""), SequenceCodec.unpack(",4.0,", 3));
    }

    @Test
    void unpackTreatsEmptyStringAsZeroRows() {
        assertEquals(List.of(), SequenceCodec.unpack("", 0));
    }

    @Test
    void unpackPadsAShortColumnInsteadOfMisaligning() {
        assertEquals(List.of("a", "b", "", ""), SequenceCodec.unpack("a,b", 4));
    }

    @Test
    void unpackTruncatesALongColumn() {
        assertEquals(List.of("a", "b"), SequenceCodec.unpack("a,b,c,d", 2));
    }

    @Test
    void rowCountUsesTheCountField() {
        assertEquals(3, SequenceCodec.rowCount(Map.of(
            SequenceSchemaConstants.COL_COUNT, "3",
            SequenceSchemaConstants.COL_ITEM_ID, "a,b,c"
        )));
    }

    @Test
    void rowCountClampsToTheShortestColumnWhenTheyDisagree() {
        // A torn write must degrade to fewer rows, never mis-align them.
        assertEquals(2, SequenceCodec.rowCount(Map.of(
            SequenceSchemaConstants.COL_COUNT, "3",
            SequenceSchemaConstants.COL_ITEM_ID, "a,b",
            SequenceSchemaConstants.COL_TS, "1,2"
        )));
    }

    @Test
    void rowCountIsZeroForMissingOrUnparseableCount() {
        assertEquals(0, SequenceCodec.rowCount(Map.of()));
        assertEquals(0, SequenceCodec.rowCount(Map.of(SequenceSchemaConstants.COL_COUNT, "oops")));
    }

    @Test
    void constantsMatchTheSharedCrossLanguageFixture() throws Exception {
        String fixture = new String(
            getClass().getResourceAsStream("/sequence-schema.json").readAllBytes(),
            StandardCharsets.UTF_8
        );
        for (String column : SequenceSchemaConstants.COLUMNS) {
            assertTrue(fixture.contains("\"" + column + "\""), "missing column " + column);
        }
        assertTrue(fixture.contains("\"rowSeparator\": \"" + SequenceSchemaConstants.ROW_SEPARATOR + "\""));
        assertTrue(fixture.contains("\"valueSeparator\": \"" + SequenceSchemaConstants.VALUE_SEPARATOR + "\""));
        assertTrue(fixture.contains("\"keyPrefix\": \"" + SequenceSchemaConstants.KEY_PREFIX + "\""));
        assertTrue(fixture.contains("\"countField\": \"" + SequenceSchemaConstants.COL_COUNT + "\""));
    }

    @Test
    void keyMatchesTheScalaFormat() {
        assertEquals("seq:u1:rating:20260723",
            SequenceSchemaConstants.key("u1", SequenceSchemaConstants.KIND_RATING, "20260723"));
    }

    @Test
    void bucketMapsAWholeUtcDayToOneStamp() {
        assertEquals("20260723", SequenceSchemaConstants.bucket(1784764800000L));
        assertEquals("20260723", SequenceSchemaConstants.bucket(1784851199999L));
        assertEquals("20260724", SequenceSchemaConstants.bucket(1784851200000L));
    }

    @Test
    void sliceExposesRequestedColumnsAndEmptyListsForTheRest() {
        SequenceSlice slice = new SequenceSlice(
            Map.of(
                SequenceSchemaConstants.COL_ITEM_ID, List.of("m1", "m2"),
                SequenceSchemaConstants.COL_TS, List.of("100", "200")
            ),
            2
        );

        assertEquals(2, slice.size());
        assertEquals(List.of("m1", "m2"), slice.itemIds());
        assertEquals(List.of(100L, 200L), slice.timestamps());
        assertEquals(List.of(), slice.column(SequenceSchemaConstants.COL_GENRES));
    }
}
```

Create `src/test/resources/sequence-schema.json` with exactly the same bytes as the Scala fixture:

```json
{
  "keyPrefix": "seq",
  "kinds": ["rating", "click"],
  "columns": ["item_id", "ts", "action", "rating", "genres", "release_year"],
  "countField": "n",
  "rowSeparator": ",",
  "valueSeparator": "|"
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && mvn -pl services/java-retrieval-service test -Dtest=SequenceCodecTest`
Expected: FAIL — compilation error, `package com.demo.retrieval.service.sequence does not exist`.

- [ ] **Step 3: Write minimal implementation**

Create `SequenceSchemaConstants.java`:

```java
package com.demo.retrieval.service.sequence;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Mirror of the Scala {@code com.demo.sequence.SequenceSchema}. Cross-language drift is the
 * most exposed failure mode of the sequence store, so both sides assert against the shared
 * {@code sequence-schema.json} test fixture.
 */
public final class SequenceSchemaConstants {

    public static final String KEY_PREFIX = "seq";

    public static final String KIND_RATING = "rating";
    public static final String KIND_CLICK = "click";

    public static final String COL_ITEM_ID = "item_id";
    public static final String COL_TS = "ts";
    public static final String COL_ACTION = "action";
    public static final String COL_RATING = "rating";
    public static final String COL_GENRES = "genres";
    public static final String COL_RELEASE_YEAR = "release_year";
    public static final String COL_COUNT = "n";

    public static final List<String> COLUMNS =
        List.of(COL_ITEM_ID, COL_TS, COL_ACTION, COL_RATING, COL_GENRES, COL_RELEASE_YEAR);

    public static final String ROW_SEPARATOR = ",";
    public static final String VALUE_SEPARATOR = "|";

    private static final DateTimeFormatter DAY_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private SequenceSchemaConstants() {
    }

    public static String key(String userId, String kind, String bucket) {
        return KEY_PREFIX + ":" + userId + ":" + kind + ":" + bucket;
    }

    public static String bucket(long epochMillis) {
        return DAY_FORMAT.format(Instant.ofEpochMilli(epochMillis));
    }
}
```

Create `SequenceCodec.java`:

```java
package com.demo.retrieval.service.sequence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Read-side unpacking of the packed column strings. Serving never writes sequences. */
public final class SequenceCodec {

    private SequenceCodec() {
    }

    /**
     * Split into exactly {@code n} elements: pad short columns, truncate long ones.
     * The {@code -1} limit is required — the default drops trailing empty strings, which
     * would shift every row whose last value is null.
     */
    public static List<String> unpack(String packed, int n) {
        if (n <= 0) {
            return List.of();
        }
        String[] parts = (packed == null || packed.isEmpty())
            ? new String[0]
            : packed.split(SequenceSchemaConstants.ROW_SEPARATOR, -1);

        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(i < parts.length ? parts[i] : "");
        }
        return List.copyOf(out);
    }

    /**
     * Authoritative row count for a chunk: the {@code n} field, clamped down to the shortest
     * present data column. A torn write must produce fewer rows, never mis-aligned ones.
     */
    public static int rowCount(Map<String, String> fields) {
        int declared = parseCount(fields.get(SequenceSchemaConstants.COL_COUNT));
        if (declared <= 0) {
            return 0;
        }
        int shortest = declared;
        for (String column : SequenceSchemaConstants.COLUMNS) {
            String packed = fields.get(column);
            if (packed == null) {
                continue;
            }
            int length = packed.isEmpty() ? 0 : packed.split(SequenceSchemaConstants.ROW_SEPARATOR, -1).length;
            shortest = Math.min(shortest, length);
        }
        return Math.max(0, shortest);
    }

    private static int parseCount(String raw) {
        if (raw == null) {
            return 0;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
```

Create `SequenceSlice.java`:

```java
package com.demo.retrieval.service.sequence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A columnar read result. Deliberately not a {@code List<SequenceRow>}: decoding into row
 * objects at this boundary would reduce column pruning to a network-bytes optimisation
 * instead of a work saving.
 */
public final class SequenceSlice {

    private final Map<String, List<String>> columns;
    private final int size;

    public SequenceSlice(Map<String, List<String>> columns, int size) {
        this.columns = Map.copyOf(columns);
        this.size = size;
    }

    public static SequenceSlice empty() {
        return new SequenceSlice(Map.of(), 0);
    }

    public int size() {
        return size;
    }

    /** Empty list when the column was not requested — callers never need a null check. */
    public List<String> column(String name) {
        return columns.getOrDefault(name, List.of());
    }

    public List<String> itemIds() {
        return column(SequenceSchemaConstants.COL_ITEM_ID);
    }

    public List<Long> timestamps() {
        List<String> raw = column(SequenceSchemaConstants.COL_TS);
        List<Long> out = new ArrayList<>(raw.size());
        for (String value : raw) {
            out.add(parseLong(value));
        }
        return List.copyOf(out);
    }

    private static long parseLong(String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException | NullPointerException e) {
            return 0L;
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && mvn -pl services/java-retrieval-service test -Dtest=SequenceCodecTest`
Expected: PASS — 12 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/ \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/sequence/SequenceCodecTest.java \
        recsys-pipeline/services/java-retrieval-service/src/test/resources/sequence-schema.json
git commit -m "feat(sequence): add Java schema constants, codec and columnar slice"
```

---

### Task 11: `RedisSequenceClient` — chunked bucket walk with early exit

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/SequenceClient.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/RedisSequenceClient.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/sequence/RedisSequenceClientTest.java`

**Interfaces:**
- Consumes: `SequenceSchemaConstants`, `SequenceCodec`, `SequenceSlice`, `org.springframework.data.redis.core.StringRedisTemplate`.
- Produces:
  - `interface SequenceClient { SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback); }`
  - `class RedisSequenceClient implements SequenceClient` with constructor `RedisSequenceClient(StringRedisTemplate redis, int bucketFetchChunk, Clock clock)`.
  - `static List<String> RedisSequenceClient.bucketsToWalk(long nowMillis, Duration lookback)` — newest-first day stamps, inclusive of today.

**Design notes for the implementer:** `ts` and `n` are always fetched regardless of what the caller requested, because the result must be `ts`-descending across bucket boundaries and `n` is the alignment authority. The walk fetches `bucketFetchChunk` buckets per round trip and stops as soon as `maxRows` rows are held, so `lookback` is a ceiling on cost, not the normal cost. `Clock` is injected so the bucket walk is testable without freezing wall-clock time.

- [ ] **Step 1: Write the failing test**

Create `RedisSequenceClientTest.java`:

```java
package com.demo.retrieval.service.sequence;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisSequenceClientTest {

    // 2026-07-23T12:00:00Z — "today" is bucket 20260723
    private static final long NOW = 1784808000000L;
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC);

    /** A fake Redis backed by an in-memory map. */
    private static final class RecordingRedis {
        final List<List<String>> fetchedKeyBatches = new ArrayList<>();
        final StringRedisTemplate template;

        @SuppressWarnings("unchecked")
        RecordingRedis(Map<String, Map<String, String>> store) {
            this.template = mock(StringRedisTemplate.class);
            HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
            when(template.opsForHash()).thenReturn((HashOperations) hashOps);
            when(hashOps.multiGet(anyString(), any())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                List<Object> fields = new ArrayList<>((java.util.Collection<Object>) invocation.getArgument(1));
                Map<String, String> chunk = store.getOrDefault(key, Map.of());
                List<Object> out = new ArrayList<>();
                for (Object field : fields) {
                    out.add(chunk.get(String.valueOf(field)));
                }
                return out;
            });
        }
    }

    private static Map<String, String> chunk(String itemIds, String ts, int n) {
        return Map.of(
            SequenceSchemaConstants.COL_ITEM_ID, itemIds,
            SequenceSchemaConstants.COL_TS, ts,
            SequenceSchemaConstants.COL_COUNT, String.valueOf(n)
        );
    }

    private static String key(String bucket) {
        return SequenceSchemaConstants.key("u1", SequenceSchemaConstants.KIND_RATING, bucket);
    }

    /** Client that records every batch of keys it fetches, so the walk's cost is assertable. */
    private static RedisSequenceClient recordingClient(RecordingRedis redis) {
        return new RedisSequenceClient(redis.template, 7, CLOCK) {
            @Override
            protected List<Map<String, String>> fetchBatch(List<String> keys, List<String> fields) {
                redis.fetchedKeyBatches.add(List.copyOf(keys));
                return super.fetchBatch(keys, fields);
            }
        };
    }

    @Test
    void bucketsToWalkListsDaysNewestFirstInclusiveOfToday() {
        List<String> buckets = RedisSequenceClient.bucketsToWalk(NOW, Duration.ofDays(3));
        assertEquals(List.of("20260723", "20260722", "20260721"), buckets);
    }

    @Test
    void readReturnsRowsNewestFirstAcrossBucketBoundaries() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), chunk("m3,m4", "300,400", 2),
            key("20260722"), chunk("m1,m2", "100,200", 2)
        ));
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(4, slice.size());
        assertEquals(List.of("m4", "m3", "m2", "m1"), slice.itemIds());
        assertEquals(List.of(400L, 300L, 200L, 100L), slice.timestamps());
    }

    @Test
    void readStopsAfterTheFirstChunkOnceMaxRowsIsFilled() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), chunk("m1,m2,m3", "100,200,300", 3)
        ));
        RedisSequenceClient client = recordingClient(redis);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 2, Duration.ofDays(90));

        assertEquals(2, slice.size());
        assertEquals(List.of("m3", "m2"), slice.itemIds());
        // 90 days of lookback must not cost 90 buckets when the first chunk already suffices.
        assertEquals(1, redis.fetchedKeyBatches.size(), "expected a single pipelined round trip");
        assertEquals(7, redis.fetchedKeyBatches.get(0).size());
    }

    @Test
    void readBoundsTheWalkByLookback() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260701"), chunk("old", "1", 1)   // 22 days ago, outside a 7-day lookback
        ));
        RedisSequenceClient client = recordingClient(redis);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(0, slice.size());
        List<String> walked = new ArrayList<>();
        redis.fetchedKeyBatches.forEach(walked::addAll);
        assertEquals(7, walked.size());
        assertTrue(walked.stream().noneMatch(k -> k.endsWith("20260701")));
    }

    @Test
    void readAlwaysFetchesTsAndCountEvenWhenNotRequested() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), chunk("m1", "100", 1)
        ));
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(List.of(100L), slice.timestamps());
        assertEquals(List.of("m1"), slice.itemIds());
        // Column pruning is real: genres was never requested, so it is not populated.
        assertEquals(List.of(), slice.column(SequenceSchemaConstants.COL_GENRES));
    }

    @Test
    void readReturnsEmptyForAUserWithNoBuckets() {
        RecordingRedis redis = new RecordingRedis(Map.of());
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("ghost", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(0, slice.size());
        assertEquals(List.of(), slice.itemIds());
    }

    @Test
    void readClampsAChunkWhoseColumnsDisagreeWithN() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), Map.of(
                SequenceSchemaConstants.COL_ITEM_ID, "m1,m2",
                SequenceSchemaConstants.COL_TS, "100,200",
                SequenceSchemaConstants.COL_COUNT, "5"   // torn write
            )
        ));
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(2, slice.size());
        assertEquals(List.of("m2", "m1"), slice.itemIds());
    }
}
```

The `recordingClient` helper depends on `fetchBatch` being `protected` rather than private — that is the one seam that makes the walk's cost (not just its result) assertable, which is what criterion 5 requires.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && mvn -pl services/java-retrieval-service test -Dtest=RedisSequenceClientTest`
Expected: FAIL — compilation error, `cannot find symbol: class RedisSequenceClient`.

- [ ] **Step 3: Write minimal implementation**

Create `SequenceClient.java`:

```java
package com.demo.retrieval.service.sequence;

import java.time.Duration;
import java.util.Set;

public interface SequenceClient {

    /**
     * Reads up to {@code maxRows} of a user's sequence, newest first, looking back no further
     * than {@code lookback}. Only {@code columns} are populated in the result.
     */
    SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback);
}
```

Create `RedisSequenceClient.java`:

```java
package com.demo.retrieval.service.sequence;

import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Walks day buckets newest-first, pipelining a fixed number per round trip and stopping as
 * soon as enough rows are held. A user active today costs one round trip regardless of how
 * long the lookback window is.
 */
public class RedisSequenceClient implements SequenceClient {

    private final StringRedisTemplate redis;
    private final int bucketFetchChunk;
    private final Clock clock;

    public RedisSequenceClient(StringRedisTemplate redis, int bucketFetchChunk, Clock clock) {
        this.redis = redis;
        this.bucketFetchChunk = Math.max(1, bucketFetchChunk);
        this.clock = clock;
    }

    /** Day stamps from today backwards, inclusive, bounded by {@code lookback}. */
    public static List<String> bucketsToWalk(long nowMillis, Duration lookback) {
        int days = Math.max(1, (int) lookback.toDays());
        List<String> buckets = new ArrayList<>(days);
        for (int i = 0; i < days; i++) {
            buckets.add(SequenceSchemaConstants.bucket(nowMillis - (long) i * 86_400_000L));
        }
        return List.copyOf(buckets);
    }

    @Override
    public SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback) {
        if (maxRows <= 0) {
            return SequenceSlice.empty();
        }

        // ts and n are always fetched: ordering across buckets needs ts, and n is the
        // alignment authority. Everything else is exactly what the caller asked for.
        List<String> fields = new ArrayList<>();
        fields.add(SequenceSchemaConstants.COL_TS);
        fields.add(SequenceSchemaConstants.COL_COUNT);
        for (String column : SequenceSchemaConstants.COLUMNS) {
            if (columns.contains(column) && !fields.contains(column)) {
                fields.add(column);
            }
        }

        List<String> buckets = bucketsToWalk(clock.millis(), lookback);
        List<Map<String, List<String>>> decoded = new ArrayList<>();
        int held = 0;

        for (int offset = 0; offset < buckets.size() && held < maxRows; offset += bucketFetchChunk) {
            List<String> batch = new ArrayList<>();
            for (int i = offset; i < Math.min(offset + bucketFetchChunk, buckets.size()); i++) {
                batch.add(SequenceSchemaConstants.key(userId, kind, buckets.get(i)));
            }

            for (Map<String, String> chunk : fetchBatch(batch, fields)) {
                int n = SequenceCodec.rowCount(chunk);
                if (n == 0) {
                    continue;
                }
                Map<String, List<String>> unpacked = new LinkedHashMap<>();
                for (String field : fields) {
                    if (!SequenceSchemaConstants.COL_COUNT.equals(field)) {
                        unpacked.put(field, SequenceCodec.unpack(chunk.get(field), n));
                    }
                }
                decoded.add(unpacked);
                held += n;
            }
        }

        return assemble(decoded, fields, columns, maxRows);
    }

    /** One pipelined round trip. Overridable so tests can assert on the walk's cost. */
    protected List<Map<String, String>> fetchBatch(List<String> keys, List<String> fields) {
        List<Object> fieldKeys = new ArrayList<>(fields);
        List<Object> raw = redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> ops) {
                RedisOperations<String, String> typed = (RedisOperations<String, String>) ops;
                for (String key : keys) {
                    typed.opsForHash().multiGet(key, fieldKeys);
                }
                return null;
            }
        });

        List<Map<String, String>> out = new ArrayList<>(keys.size());
        for (Object entry : raw) {
            Map<String, String> chunk = new HashMap<>();
            if (entry instanceof List<?> values) {
                for (int i = 0; i < fields.size() && i < values.size(); i++) {
                    Object value = values.get(i);
                    if (value != null) {
                        chunk.put(fields.get(i), String.valueOf(value));
                    }
                }
            }
            out.add(chunk);
        }
        return out;
    }

    /** Flatten decoded chunks into one ts-descending slice, keeping the layout columnar. */
    private SequenceSlice assemble(
        List<Map<String, List<String>>> decoded,
        List<String> fields,
        Set<String> requested,
        int maxRows
    ) {
        record Ref(long ts, int chunk, int row) {
        }

        List<Ref> order = new ArrayList<>();
        for (int c = 0; c < decoded.size(); c++) {
            List<String> timestamps = decoded.get(c).getOrDefault(SequenceSchemaConstants.COL_TS, List.of());
            for (int r = 0; r < timestamps.size(); r++) {
                order.add(new Ref(parseLong(timestamps.get(r)), c, r));
            }
        }
        order.sort(Comparator.comparingLong(Ref::ts).reversed());
        if (order.size() > maxRows) {
            order = order.subList(0, maxRows);
        }

        List<String> emitted = new ArrayList<>(fields);
        emitted.remove(SequenceSchemaConstants.COL_COUNT);
        emitted.removeIf(f -> !requested.contains(f) && !SequenceSchemaConstants.COL_TS.equals(f));

        Map<String, List<String>> out = new LinkedHashMap<>();
        for (String field : emitted) {
            List<String> values = new ArrayList<>(order.size());
            for (Ref ref : order) {
                List<String> column = decoded.get(ref.chunk()).getOrDefault(field, List.of());
                values.add(ref.row() < column.size() ? column.get(ref.row()) : "");
            }
            out.put(field, List.copyOf(values));
        }
        return new SequenceSlice(out, order.size());
    }

    private static long parseLong(String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException | NullPointerException e) {
            return 0L;
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && mvn -pl services/java-retrieval-service test -Dtest=RedisSequenceClientTest`
Expected: PASS — 7 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/SequenceClient.java \
        recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/sequence/RedisSequenceClient.java \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/sequence/RedisSequenceClientTest.java
git commit -m "feat(sequence): add RedisSequenceClient with chunked early-exit bucket walk"
```

---

### Task 12: Wire the hydrator with off / shadow / on

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/query_hydrators/RatingSequencesQueryHydrator.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/query_hydrators/RatingSequencesQueryHydratorSequenceStoreTest.java`

**Interfaces:**
- Consumes: `SequenceClient`, `SequenceSlice`, `SequenceSchemaConstants`, `MovieLensFeatureClient`, `MovieLensUserFeatures`, `ScoredMoviesQuery`.
- Produces:
  - `RecommendationProperties.Sequence` with `mode: String` (default `"off"`), `lookbackDays: int` (default `90`), `bucketFetchChunk: int` (default `7`); getter/setter pairs matching the existing style.
  - Second `RatingSequencesQueryHydrator` constructor: `RatingSequencesQueryHydrator(MovieLensFeatureClient featureClient, SequenceClient sequenceClient, String mode, int lookbackDays)`.

**Critical:** the existing single-argument constructor must stay, defaulting to `mode="off"`. `RatingSequencesQueryHydratorTest` and every other existing test must pass **without edits**.

- [ ] **Step 1: Write the failing test**

Create `RatingSequencesQueryHydratorSequenceStoreTest.java`:

```java
package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.sequence.SequenceClient;
import com.demo.retrieval.service.sequence.SequenceSchemaConstants;
import com.demo.retrieval.service.sequence.SequenceSlice;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RatingSequencesQueryHydratorSequenceStoreTest {

    /** Records the requested columns so column pruning is asserted, not assumed. */
    private static final class RecordingSequenceClient implements SequenceClient {
        Set<String> requestedColumns;
        int requestedMaxRows;
        Duration requestedLookback;
        final List<String> itemIds;

        RecordingSequenceClient(List<String> itemIds) {
            this.itemIds = itemIds;
        }

        @Override
        public SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback) {
            this.requestedColumns = columns;
            this.requestedMaxRows = maxRows;
            this.requestedLookback = lookback;
            List<String> timestamps = new ArrayList<>();
            for (int i = 0; i < itemIds.size(); i++) {
                timestamps.add(String.valueOf(1_000_000L - i));
            }
            Map<String, List<String>> columnMap = new LinkedHashMap<>();
            columnMap.put(SequenceSchemaConstants.COL_ITEM_ID, itemIds);
            columnMap.put(SequenceSchemaConstants.COL_TS, timestamps);
            return new SequenceSlice(columnMap, itemIds.size());
        }
    }

    private static List<String> ids(String prefix, int count) {
        List<String> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(prefix + i);
        }
        return out;
    }

    private static ScoredMoviesQuery query() {
        return ScoredMoviesQuery.forUser("u1");
    }

    private static MovieLensUserFeatures legacyFeatures(List<String> rated) {
        return new MovieLensUserFeatures("u1", List.of("drama"), 4.0, rated.size(), rated);
    }

    @Test
    void offModeIgnoresTheSequenceStoreEntirely() {
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(ids("new", 200));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 10))), sequenceClient, "off", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(ids("legacy", 10), result.actionSequenceMovieIds());
        assertNull(sequenceClient.requestedColumns, "sequence store must not be read in off mode");
    }

    @Test
    void shadowModeReadsBothButServesTheLegacyResult() {
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(ids("new", 200));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 10))), sequenceClient, "shadow", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(ids("legacy", 10), result.actionSequenceMovieIds());
        assertTrue(sequenceClient.requestedColumns != null, "shadow mode must still read the store");
    }

    @Test
    void onModeServesSequencesLongerThanTheLegacyFiftyItemCap() {
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(ids("m", 200));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 50))), sequenceClient, "on", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        // The criterion the whole design exists to satisfy.
        assertEquals(100, result.retrievalSequenceMovieIds().size());
        assertEquals(50, result.actionSequenceMovieIds().size());
        assertEquals(20, result.scoringSequenceMovieIds().size());
        assertEquals("m0", result.retrievalSequenceMovieIds().get(0));
    }

    @Test
    void onModeRequestsOnlyItemIdAndTimestampColumns() {
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(ids("m", 10));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(List.of())), sequenceClient, "on", 90
        );

        hydrator.hydrate(query());

        assertEquals(Set.of(SequenceSchemaConstants.COL_ITEM_ID, SequenceSchemaConstants.COL_TS),
            sequenceClient.requestedColumns);
        assertEquals(RatingSequencesQueryHydrator.MAX_RETRIEVAL_SEQ_LENGTH, sequenceClient.requestedMaxRows);
        assertEquals(Duration.ofDays(90), sequenceClient.requestedLookback);
    }

    @Test
    void onModeDeduplicatesWhilePreservingRecencyOrder() {
        RecordingSequenceClient sequenceClient =
            new RecordingSequenceClient(List.of("m1", "m2", "m1", "m3", "m2"));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(List.of())), sequenceClient, "on", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(List.of("m1", "m2", "m3"), result.retrievalSequenceMovieIds());
    }

    @Test
    void onModeServesAnEmptySequenceRatherThanFallingBackToLegacy() {
        // An empty sequence for a user with no events is a correct answer. Falling back would
        // mask exactly the bug shadow mode exists to catch.
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(List.of());
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 10))), sequenceClient, "on", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(List.of(), result.actionSequenceMovieIds());
    }

    @Test
    void onModeFallsBackToLegacyWhenTheSequenceStoreThrows() {
        SequenceClient failing = (userId, kind, columns, maxRows, lookback) -> {
            throw new IllegalStateException("redis down");
        };
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 10))), failing, "on", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(ids("legacy", 10), result.actionSequenceMovieIds());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && mvn -pl services/java-retrieval-service test -Dtest=RatingSequencesQueryHydratorSequenceStoreTest`
Expected: FAIL — compilation error, no `RatingSequencesQueryHydrator` constructor accepting `(MovieLensFeatureClient, SequenceClient, String, int)`.

- [ ] **Step 3: Write minimal implementation**

Replace the body of `RatingSequencesQueryHydrator.java` with:

```java
package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.MovieLensFeatureClient;
import com.demo.retrieval.service.sequence.SequenceClient;
import com.demo.retrieval.service.sequence.SequenceSchemaConstants;
import com.demo.retrieval.service.sequence.SequenceSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Hydrates all three rating-sequence fields in a single feature-store read.
 *
 * The three sequences are length views over one deduped rating sequence:
 *
 *   actionSequenceMovieIds    — 50 items   (sequential model input)
 *   retrievalSequenceMovieIds — 100 items  (ANN candidate retrieval)
 *   scoringSequenceMovieIds   — 20 items   (ranking model input)
 *
 * The source is either the legacy {@code user:{id}:features} CSV blob or the columnar
 * sequence store, selected by {@code recsys.sequence.mode}:
 *   off    — legacy only
 *   shadow — read both, serve legacy, log the diff
 *   on     — serve the sequence store, falling back to legacy only on error
 */
@Component
public class RatingSequencesQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_ACTION_SEQ_LENGTH = 50;
    public static final int MAX_RETRIEVAL_SEQ_LENGTH = 100;
    public static final int MAX_SCORING_SEQ_LENGTH = 20;

    public static final String MODE_OFF = "off";
    public static final String MODE_SHADOW = "shadow";
    public static final String MODE_ON = "on";

    private static final Logger log = LoggerFactory.getLogger(RatingSequencesQueryHydrator.class);
    private static final Set<String> READ_COLUMNS =
        Set.of(SequenceSchemaConstants.COL_ITEM_ID, SequenceSchemaConstants.COL_TS);

    private final MovieLensFeatureClient featureClient;
    private final SequenceClient sequenceClient;
    private final String mode;
    private final Duration lookback;

    public RatingSequencesQueryHydrator(MovieLensFeatureClient featureClient) {
        this(featureClient, null, MODE_OFF, 90);
    }

    public RatingSequencesQueryHydrator(
        MovieLensFeatureClient featureClient,
        SequenceClient sequenceClient,
        String mode,
        int lookbackDays
    ) {
        this.featureClient = featureClient;
        this.sequenceClient = sequenceClient;
        this.mode = mode == null ? MODE_OFF : mode.trim().toLowerCase();
        this.lookback = Duration.ofDays(Math.max(1, lookbackDays));
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<String> legacy = dedupe(featureClient.getUserFeatures(query.userId())
            .map(MovieLensUserFeatures::recentlyRatedMovieIds)
            .orElseGet(List::of));

        List<String> source = legacy;
        if (!MODE_OFF.equals(mode) && sequenceClient != null) {
            try {
                List<String> fromStore = dedupe(readSequence(query.userId()));
                if (MODE_SHADOW.equals(mode)) {
                    logDiff(query.userId(), legacy, fromStore);
                } else {
                    source = fromStore;
                }
            } catch (RuntimeException e) {
                log.warn("Sequence store read failed for user {}, using legacy path: {}",
                    query.userId(), e.getMessage());
            }
        }

        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures()
                .withActionSequenceMovieIds(truncate(source, MAX_ACTION_SEQ_LENGTH))
                .withRetrievalSequenceMovieIds(truncate(source, MAX_RETRIEVAL_SEQ_LENGTH))
                .withScoringSequenceMovieIds(truncate(source, MAX_SCORING_SEQ_LENGTH)),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        MovieLensUserFeatures hf = hydrated.userFeatures();
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures()
                .withActionSequenceMovieIds(hf.actionSequenceMovieIds())
                .withRetrievalSequenceMovieIds(hf.retrievalSequenceMovieIds())
                .withScoringSequenceMovieIds(hf.scoringSequenceMovieIds()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    private List<String> readSequence(String userId) {
        SequenceSlice slice = sequenceClient.read(
            userId, SequenceSchemaConstants.KIND_RATING, READ_COLUMNS, MAX_RETRIEVAL_SEQ_LENGTH, lookback
        );
        return slice.itemIds();
    }

    private void logDiff(String userId, List<String> legacy, List<String> fromStore) {
        int prefix = 0;
        while (prefix < legacy.size() && prefix < fromStore.size()
            && legacy.get(prefix).equals(fromStore.get(prefix))) {
            prefix++;
        }
        log.info("sequence-shadow user={} legacyLen={} storeLen={} prefixAgreement={} firstDivergence={}",
            userId, legacy.size(), fromStore.size(), prefix,
            prefix == Math.min(legacy.size(), fromStore.size()) ? -1 : prefix);
    }

    private static List<String> dedupe(List<String> items) {
        return List.copyOf(new LinkedHashSet<>(items));
    }

    private static List<String> truncate(List<String> items, int maxLen) {
        return items.size() > maxLen ? List.copyOf(items.subList(0, maxLen)) : items;
    }
}
```

In `RecommendationProperties.java`, add the field next to the other nested-config fields:

```java
    private Sequence sequence = new Sequence();
```

its accessors alongside the existing ones:

```java
    public Sequence getSequence() {
        return sequence;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }
```

and this nested class alongside the other nested classes (`Cache`, `Bandit`, …):

```java
    public static class Sequence {
        /** off | shadow | on */
        private String mode = "off";
        private int lookbackDays = 90;
        private int bucketFetchChunk = 7;

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public int getLookbackDays() {
            return lookbackDays;
        }

        public void setLookbackDays(int lookbackDays) {
            this.lookbackDays = lookbackDays;
        }

        public int getBucketFetchChunk() {
            return bucketFetchChunk;
        }

        public void setBucketFetchChunk(int bucketFetchChunk) {
            this.bucketFetchChunk = bucketFetchChunk;
        }
    }
```

In `application.yml`, add under `recsys:` (immediately after the `cache:` block):

```yaml
  sequence:
    # off = legacy CSV blobs only; shadow = read both and log the diff; on = serve the store
    mode: ${RECSYS_SEQUENCE_MODE:off}
    lookback-days: ${RECSYS_SEQUENCE_LOOKBACK_DAYS:90}
    bucket-fetch-chunk: ${RECSYS_SEQUENCE_BUCKET_FETCH_CHUNK:7}
```

Finally, add the Spring wiring so `RedisSequenceClient` and the configured hydrator become beans. Add to `RecommendationProperties`' owning configuration class — create `services/java-retrieval-service/src/main/java/com/demo/retrieval/config/SequenceConfig.java`:

```java
package com.demo.retrieval.config;

import com.demo.retrieval.service.clients.MovieLensFeatureClient;
import com.demo.retrieval.service.query_hydrators.RatingSequencesQueryHydrator;
import com.demo.retrieval.service.sequence.RedisSequenceClient;
import com.demo.retrieval.service.sequence.SequenceClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;

@Configuration
public class SequenceConfig {

    @Bean
    public SequenceClient sequenceClient(StringRedisTemplate redis, RecommendationProperties properties) {
        return new RedisSequenceClient(redis, properties.getSequence().getBucketFetchChunk(), Clock.systemUTC());
    }

    @Bean
    public RatingSequencesQueryHydrator ratingSequencesQueryHydrator(
        MovieLensFeatureClient featureClient,
        SequenceClient sequenceClient,
        RecommendationProperties properties
    ) {
        return new RatingSequencesQueryHydrator(
            featureClient,
            sequenceClient,
            properties.getSequence().getMode(),
            properties.getSequence().getLookbackDays()
        );
    }
}
```

Then remove the `@Component` annotation (and its `org.springframework.stereotype.Component` import) from `RatingSequencesQueryHydrator`, since `SequenceConfig` now constructs it — leaving both would give Spring two competing definitions.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && mvn -pl services/java-retrieval-service test -Dtest=RatingSequencesQueryHydratorSequenceStoreTest`
Expected: PASS — 7 tests.

Then the full Java suite, which must include the **unmodified** `RatingSequencesQueryHydratorTest`:

Run: `cd recsys-pipeline && mvn -pl services/java-retrieval-service test`
Expected: PASS — all tests, no pre-existing test edited.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/ \
        recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/query_hydrators/RatingSequencesQueryHydrator.java \
        recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/query_hydrators/RatingSequencesQueryHydratorSequenceStoreTest.java
git commit -m "feat(sequence): serve long rating sequences behind recsys.sequence.mode"
```

---

## Final verification

- [ ] **Run both suites end to end**

```bash
cd recsys-pipeline/services/spark-streaming-job && sbt test
cd recsys-pipeline && mvn -pl services/java-retrieval-service test
```

Expected: PASS on both. Confirm with `git diff master --stat` that no pre-existing test file appears except `MovieLensContextCollectorStreamingJobSpec.scala`, `UserEventStreamingJobSpec.scala`, and `RatingSequencesQueryHydrator.java` — and that the first two only gained tests.

- [ ] **Check the success criteria from the spec**

| # | Criterion | Verified by |
|---|---|---|
| 1 | Both suites pass, every existing spec unmodified | Final verification above |
| 2 | `mode=off` leaves serving output unchanged | Untouched `RatingSequencesQueryHydratorTest` + `offModeIgnoresTheSequenceStoreEntirely` |
| 3 | A 200-item user yields 100 retrieval ids | `onModeServesSequencesLongerThanTheLegacyFiftyItemCap` |
| 4 | Hydrator requests exactly `{item_id, ts}` | `onModeRequestsOnlyItemIdAndTimestampColumns` |
| 5 | Active user costs one round trip | `readStopsAfterTheFirstChunkOnceMaxRowsIsFilled` |
| 6 | Shadow diff over a sim run | Manual — see below |

- [ ] **Manual shadow-mode check (criterion 6)**

Build the assembly, backfill the store, then run the sim with shadow mode on. The
`spark-submit` shape and jar path match `run-offline-pipeline.sh`.

```bash
cd recsys-pipeline/services/spark-streaming-job && sbt assembly && cd ../..

"${SPARK_HOME:?set SPARK_HOME or put spark-submit on PATH}/bin/spark-submit" \
  --master "local[*]" \
  --class com.demo.sequence.SequenceBackfillJob \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar \
  sampledata/ratings.csv

RECSYS_SEQUENCE_MODE=shadow ./run-movielens-segment-sim.sh
```

Expected: `sequence-shadow` log lines with `prefixAgreement` equal to `min(legacyLen, storeLen)` for users under 50 items — that is, the two paths agree wherever they overlap.

If `sampledata/ratings.csv` does not exist, substitute whichever ratings CSV
`run-offline-pipeline.sh` uses; the job takes the path as its first argument or via
`RATINGS_INPUT_PATH`.
