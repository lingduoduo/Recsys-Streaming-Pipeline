package com.demo.engine

import java.nio.file.Files
import java.sql.Timestamp
import java.security.MessageDigest

import com.demo.SparkTestSupport
import com.demo.event.DecodedEventFrames
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.storage.StorageLevel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class RawArchiveSinkSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val QueryIdentity = "checkpoint://task-4-test"

  private def queryNamespace(queryIdentity: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(queryIdentity.getBytes("UTF-8"))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private def queryRoot(root: java.nio.file.Path, queryIdentity: String): java.nio.file.Path =
    root.resolve("_queries").resolve(queryNamespace(queryIdentity))

  private def batchPath(
      root: java.nio.file.Path,
      queryIdentity: String,
      batchId: Long
  ): java.nio.file.Path = queryRoot(root, queryIdentity).resolve("_batches").resolve(batchId.toString)

  private def containsPath(root: java.nio.file.Path, suffix: String): Boolean = {
    val paths = Files.walk(root)
    try paths.iterator().asScala.exists(_.toString.endsWith(suffix))
    finally paths.close()
  }

  private def decodedFrames(eventId: String, timestampMs: Long, offset: Long): DecodedEventFrames = {
    val s = spark
    import s.implicits._
    val valid = Seq(
      (eventId, timestampMs, "recsys_events", 0, offset,
        Timestamp.valueOf("2024-06-15 00:00:01"), 1234L)
    ).toDF("event_id", "timestamp_ms", "kafka_topic", "kafka_partition", "kafka_offset",
      "kafka_timestamp", "schema_fingerprint")
    val deadLetters = valid.limit(0).select(
      lit(null).cast(StringType).as("kafka_topic"),
      lit(null).cast(IntegerType).as("kafka_partition"),
      lit(null).cast(LongType).as("kafka_offset"),
      lit(null).cast(TimestampType).as("kafka_timestamp"),
      lit(null).cast(BinaryType).as("raw_value"),
      lit(null).cast(LongType).as("schema_fingerprint"),
      lit(null).cast(StringType).as("error_code"),
      lit(null).cast(StringType).as("error_detail")
    )
    DecodedEventFrames(valid, deadLetters)
  }

  private def durableSink(identity: String)(
      writer: (org.apache.spark.sql.DataFrame, SinkWriteContext) => Unit
  ): DurableSink = new DurableSink {
    override val sinkIdentity: String = identity
    override def writeDurably(
        batch: org.apache.spark.sql.DataFrame,
        context: SinkWriteContext
    ): Unit = writer(batch, context)
    override def write(batch: org.apache.spark.sql.DataFrame, batchId: Long): Unit =
      throw new AssertionError("legacy write must not be used")
  }

  "RawArchiveSink.writeValid" should "archive lineage by UTC event date exactly once per batch" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("raw-archive-valid")
    val validPath = root.resolve("valid")
    val deadPath = root.resolve("dead")
    val sink = new RawArchiveSink(validPath.toString, deadPath.toString, QueryIdentity)
    val valid = Seq(
      ("e1", 1718409600000L, "recsys_events", 2, 42L,
        Timestamp.valueOf("2024-06-15 00:00:01"), 1234L)
    ).toDF("event_id", "timestamp_ms", "kafka_topic", "kafka_partition", "kafka_offset",
      "kafka_timestamp", "schema_fingerprint")

    sink.writeValid(valid, batchId = 7L)
    sink.writeValid(valid, batchId = 7L)

    val committedBatch = batchPath(validPath, QueryIdentity, 7L)
    containsPath(committedBatch, "date=2024-06-15") shouldBe true
    Files.exists(committedBatch.resolve("_COMMITTED")) shouldBe true
    val archived = spark.read.parquet(committedBatch.toString)
    archived.count() shouldBe 1L
    archived.columns should contain allOf
      ("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "schema_fingerprint", "archived_at", "date")
  }

  "RawArchiveSink.writeDeadLetters" should "use Kafka ingestion date and a separate idempotent batch path" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("raw-archive-dead")
    val validPath = root.resolve("valid")
    val deadPath = root.resolve("dead")
    val sink = new RawArchiveSink(validPath.toString, deadPath.toString, QueryIdentity)
    val deadLetters = Seq(
      ("recsys_events", 3, 99L, Timestamp.valueOf("2024-06-16 01:02:03"),
        Array[Byte](1, 2, 3), 1234L, "corrupt_payload", "truncated")
    ).toDF("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
      "raw_value", "schema_fingerprint", "error_code", "error_detail")

    sink.writeDeadLetters(deadLetters, batchId = 8L)
    sink.writeDeadLetters(deadLetters, batchId = 8L)

    val committedBatch = batchPath(deadPath, QueryIdentity, 8L)
    containsPath(committedBatch, "date=2024-06-16") shouldBe true
    Files.exists(committedBatch.resolve("_COMMITTED")) shouldBe true
    val archived = spark.read.parquet(committedBatch.toString)
    archived.count() shouldBe 1L
    archived.select("error_code").as[String].head() shouldBe "corrupt_payload"
    archived.columns should contain allOf ("raw_value", "error_detail", "archived_at", "date")
  }

  it should "isolate the same batch id for distinct stable query identities" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("raw-archive-query-identity")
    val validPath = root.resolve("valid")
    val deadPath = root.resolve("dead")
    val queryA = "checkpoint://online-joiner"
    val queryB = "checkpoint://user-event"
    val sinkA = new RawArchiveSink(validPath.toString, deadPath.toString, queryA)
    val sinkB = new RawArchiveSink(validPath.toString, deadPath.toString, queryB)

    sinkA.writeValid(decodedFrames("from-a", 1718409600000L, 1L).valid, batchId = 3L)
    sinkB.writeValid(decodedFrames("from-b", 1718409600000L, 2L).valid, batchId = 3L)

    spark.read.parquet(batchPath(validPath, queryA, 3L).toString)
      .select("event_id").as[String].collect() should contain only "from-a"
    spark.read.parquet(batchPath(validPath, queryB, 3L).toString)
      .select("event_id").as[String].collect() should contain only "from-b"
  }

  it should "reject an uncommitted final directory and preserve other staging attempts" in {
    val root = Files.createTempDirectory("raw-archive-commit-validation")
    val validPath = root.resolve("valid")
    val deadPath = root.resolve("dead")
    val sink = new RawArchiveSink(validPath.toString, deadPath.toString, QueryIdentity)
    val incompleteFinal = batchPath(validPath, QueryIdentity, 5L)
    Files.createDirectories(incompleteFinal)
    Files.write(incompleteFinal.resolve("partial-data"), Array[Byte](1))

    an[IllegalStateException] should be thrownBy
      sink.writeValid(decodedFrames("e1", 1718409600000L, 1L).valid, batchId = 5L)

    val legacyOtherAttempt = validPath.resolve("_staging").resolve("6")
    Files.createDirectories(legacyOtherAttempt)
    Files.write(legacyOtherAttempt.resolve("other-writer"), Array[Byte](2))
    val namespacedOtherAttempt = queryRoot(validPath, QueryIdentity)
      .resolve("_attempts").resolve("6").resolve("other-writer")
    Files.createDirectories(namespacedOtherAttempt)
    Files.write(namespacedOtherAttempt.resolve("sentinel"), Array[Byte](3))

    sink.writeValid(decodedFrames("e2", 1718409600000L, 2L).valid, batchId = 6L)

    Files.exists(legacyOtherAttempt.resolve("other-writer")) shouldBe true
    Files.exists(namespacedOtherAttempt.resolve("sentinel")) shouldBe true

    val wrongIdentity = batchPath(validPath, QueryIdentity, 7L)
    Files.createDirectories(wrongIdentity)
    Files.write(wrongIdentity.resolve("_SUCCESS"), Array.emptyByteArray)
    Files.write(wrongIdentity.resolve("_COMMITTED"), "wrong-query".getBytes("UTF-8"))
    an[IllegalStateException] should be thrownBy
      sink.writeValid(decodedFrames("e3", 1718409600000L, 3L).valid, batchId = 7L)
  }

  "ExecutionEngine.processDecodedBatch" should "archive both outcomes before invoking valid-event stages and sinks" in {
    val s = spark
    import s.implicits._
    val calls = scala.collection.mutable.ArrayBuffer[String]()
    val frames = DecodedEventFrames(Seq("e1").toDF("event_id"), Seq("bad").toDF("error_code"))
    val root = Files.createTempDirectory("engine-ordering")
    val archive = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity) {
      override def writeValid(df: org.apache.spark.sql.DataFrame, batchId: Long): Unit = calls += "valid-archive"
      override def writeDeadLetters(df: org.apache.spark.sql.DataFrame, batchId: Long): Unit = calls += "dead-archive"
    }
    val stage: Stage = df => { calls += "stage"; df }
    val sink = durableSink("ordering")((_, _) => calls += "business-sink")

    ExecutionEngine.processDecodedBatch(
      Seq(Array[Byte](1)).toDF("value"), 3L,
      _ => { calls += "decode"; frames }, archive,
      Seq(stage), Seq.empty, Seq(sink), maxRetries = 0
    )

    calls shouldBe Seq("decode", "valid-archive", "dead-archive", "stage", "business-sink")
    frames.valid.storageLevel shouldBe StorageLevel.NONE
    frames.deadLetters.storageLevel shouldBe StorageLevel.NONE
  }

  it should "propagate either required archive failure without invoking business processing" in {
    val s = spark
    import s.implicits._

    Seq("valid", "dead").foreach { failingWrite =>
      var businessCalls = 0
      val frames = DecodedEventFrames(Seq("e1").toDF("event_id"), Seq("bad").toDF("error_code"))
      val root = Files.createTempDirectory(s"engine-archive-failure-$failingWrite")
      val archive = new RawArchiveSink(
        root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity) {
        override def writeValid(df: org.apache.spark.sql.DataFrame, batchId: Long): Unit =
          if (failingWrite == "valid") throw new RuntimeException("valid archive failed")
        override def writeDeadLetters(df: org.apache.spark.sql.DataFrame, batchId: Long): Unit =
          if (failingWrite == "dead") throw new RuntimeException("dead archive failed")
      }
      val sink = durableSink(s"archive-failure-$failingWrite")((_, _) => businessCalls += 1)

      an[RuntimeException] should be thrownBy ExecutionEngine.processDecodedBatch(
        Seq(Array[Byte](1)).toDF("value"), 4L, _ => frames, archive,
        Seq.empty, Seq.empty, Seq(sink), maxRetries = 0
      )

      businessCalls shouldBe 0
      frames.valid.storageLevel shouldBe StorageLevel.NONE
      frames.deadLetters.storageLevel shouldBe StorageLevel.NONE
    }
  }

  it should "suppress an event already processed in an earlier archived engine batch" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-cross-batch-dedupe")
    val archive = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity)
    val frames = decodedFrames("e1", 1718409600000L, 10L)
    val received = scala.collection.mutable.ArrayBuffer[String]()
    val sink = durableSink("cross-batch")((batch, _) =>
      batch.select("event_id").as[String].collect().foreach(received += _)
    )

    Seq(0L, 1L).foreach { batchId =>
      ExecutionEngine.processDecodedBatch(
        Seq(Array[Byte](1)).toDF("value"), batchId, _ => frames, archive,
        Seq(identity[org.apache.spark.sql.DataFrame]), Seq.empty, Seq(sink), maxRetries = 0
      )
    }

    received shouldBe Seq("e1")
  }

  it should "replay deterministically when business completed before the checkpoint committed" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-retry-before-checkpoint")
    val archive = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity)
    val frames = decodedFrames("e1", 1718409600000L, 10L)
    val received = scala.collection.mutable.ArrayBuffer[String]()
    val sink = durableSink("checkpoint-retry")((batch, _) =>
      batch.select("event_id").as[String].collect().foreach(received += _)
    )

    Seq(0L, 1L, 1L).foreach { batchId =>
      ExecutionEngine.processDecodedBatch(
        Seq(Array[Byte](1)).toDF("value"), batchId, _ => frames, archive,
        Seq(identity[org.apache.spark.sql.DataFrame]), Seq.empty, Seq(sink), maxRetries = 0
      )
    }

    received shouldBe Seq("e1")
  }

  it should "keep dedupe ownership separate for distinct query identities" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-dedupe-query-identity")
    val queryA = "checkpoint://query-a"
    val queryB = "checkpoint://query-b"
    val archiveA = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, queryA)
    val archiveB = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, queryB)
    val received = scala.collection.mutable.ArrayBuffer[String]()
    val sink = durableSink("query-separation")((batch, _) =>
      batch.select("event_id").as[String].collect().foreach(received += _)
    )

    Seq((archiveA, 0L), (archiveB, 0L)).foreach { case (archive, batchId) =>
      ExecutionEngine.processDecodedBatch(
        Seq(Array[Byte](1)).toDF("value"), batchId,
        _ => decodedFrames("e1", 1718409600000L, batchId), archive,
        Seq(identity[org.apache.spark.sql.DataFrame]), Seq.empty, Seq(sink), maxRetries = 0
      )
    }

    received shouldBe Seq("e1", "e1")
  }

  it should "expire archived event-id state at the configured watermark horizon" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-watermark-dedupe")
    val archive = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity)
    val received = scala.collection.mutable.ArrayBuffer[String]()
    val sink = durableSink("watermark-state")((batch, _) =>
      batch.select("event_id").as[String].collect().foreach(received += _)
    )
    val inputs = Seq(
      decodedFrames("e1", 1718409600000L, 10L),
      decodedFrames("future", 1718410260000L, 11L),
      decodedFrames("e1", 1718410320000L, 12L)
    )

    inputs.zipWithIndex.foreach { case (frames, batchId) =>
      ExecutionEngine.processDecodedBatch(
        Seq(Array[Byte](1)).toDF("value"), batchId.toLong, _ => frames, archive,
        Seq(identity[org.apache.spark.sql.DataFrame]), Seq.empty, Seq(sink), maxRetries = 0
      )
    }

    received shouldBe Seq("e1", "future", "e1")
    val stateDirectories = Files.list(queryRoot(root.resolve("valid"), QueryIdentity).resolve("_dedupe"))
    try {
      stateDirectories.iterator().asScala
        .map(_.getFileName.toString)
        .filter(_.forall(_.isDigit))
        .toSet shouldBe Set("1", "2")
    } finally stateDirectories.close()
  }

  it should "retain unique current-batch events on both sides of the watermark horizon" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-current-batch-watermark")
    val archive = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity)
    val valid = Seq(
      ("old", 1718409600000L, "recsys_events", 0, 10L,
        Timestamp.valueOf("2024-06-15 00:00:01"), 1234L),
      ("new", 1718413200000L, "recsys_events", 0, 11L,
        Timestamp.valueOf("2024-06-15 01:00:01"), 1234L)
    ).toDF("event_id", "timestamp_ms", "kafka_topic", "kafka_partition", "kafka_offset",
      "kafka_timestamp", "schema_fingerprint")
    val frames = DecodedEventFrames(valid, decodedFrames("unused", 1718409600000L, 0L).deadLetters)
    val received = scala.collection.mutable.ArrayBuffer[String]()
    val sink = durableSink("current-batch")((batch, _) =>
      batch.select("event_id").as[String].collect().foreach(received += _)
    )

    ExecutionEngine.processDecodedBatch(
      Seq(Array[Byte](1)).toDF("value"), 0L, _ => frames, archive,
      Seq(identity[org.apache.spark.sql.DataFrame]), Seq.empty, Seq(sink), maxRetries = 0,
      watermarkDelay = "10 minutes"
    )

    received.toSet shouldBe Set("old", "new")
  }

  it should "reject a plain sink on the migrated Avro path" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-unsupported-sink")
    val archive = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity)
    var writes = 0
    val unsupported: Sink = (_, _) => writes += 1

    an[IllegalArgumentException] should be thrownBy ExecutionEngine.processDecodedBatch(
      Seq(Array[Byte](1)).toDF("value"), 0L,
      _ => decodedFrames("e1", 1718409600000L, 1L), archive,
      Seq(identity[org.apache.spark.sql.DataFrame]), Seq.empty, Seq(unsupported), maxRetries = 0)

    writes shouldBe 0
  }

  it should "reject duplicate stable sink identities before writing" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-duplicate-sink")
    val archive = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity)
    var writes = 0
    val first = durableSink("duplicate")((_, _) => writes += 1)
    val second = durableSink("duplicate")((_, _) => writes += 1)

    an[IllegalArgumentException] should be thrownBy ExecutionEngine.processDecodedBatch(
      Seq(Array[Byte](1)).toDF("value"), 0L,
      _ => decodedFrames("e1", 1718409600000L, 1L), archive,
      Seq(identity[org.apache.spark.sql.DataFrame]), Seq.empty, Seq(first, second), maxRetries = 0)

    writes shouldBe 0
  }

  it should "skip a completed first sink when a later sink fails and the batch retries" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-multi-sink-retry")
    val archive = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity)
    var firstWrites = 0
    var secondWrites = 0
    val first = new DurableSink {
      override val sinkIdentity: String = "first"
      override def writeDurably(batch: org.apache.spark.sql.DataFrame, context: SinkWriteContext): Unit =
        firstWrites += 1
      override def write(batch: org.apache.spark.sql.DataFrame, batchId: Long): Unit =
        throw new AssertionError("legacy write must not be used")
    }
    val second = new DurableSink {
      override val sinkIdentity: String = "second"
      override def writeDurably(batch: org.apache.spark.sql.DataFrame, context: SinkWriteContext): Unit = {
        secondWrites += 1
        if (secondWrites == 1) throw new RuntimeException("second failed")
      }
      override def write(batch: org.apache.spark.sql.DataFrame, batchId: Long): Unit =
        throw new AssertionError("legacy write must not be used")
    }
    def process(): Unit = ExecutionEngine.processDecodedBatch(
      Seq(Array[Byte](1)).toDF("value"), 4L,
      _ => decodedFrames("e1", 1718409600000L, 1L), archive,
      Seq(identity[org.apache.spark.sql.DataFrame]), Seq.empty, Seq(first, second), maxRetries = 0)

    an[RuntimeException] should be thrownBy process()
    process()

    firstWrites shouldBe 1
    secondWrites shouldBe 2
  }

  it should "pass one stable sink context to a partial-write retry" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-partial-sink-retry")
    val archive = new RawArchiveSink(
      root.resolve("valid").toString, root.resolve("dead").toString, QueryIdentity)
    val contexts = scala.collection.mutable.ArrayBuffer[SinkWriteContext]()
    val applied = scala.collection.mutable.ArrayBuffer[String]()
    val completedEffects = scala.collection.mutable.Set[String]()
    var invocations = 0
    val partial = new DurableSink {
      override val sinkIdentity: String = "partial"
      override def writeDurably(batch: org.apache.spark.sql.DataFrame, context: SinkWriteContext): Unit = {
        contexts += context
        invocations += 1
        val eventIds = batch.select("event_id").as[String].collect().sorted
        eventIds.foreach { eventId =>
          if (invocations == 1 && eventId == "e2") throw new RuntimeException("mid-write")
          if (completedEffects.add(eventId)) applied += eventId
        }
      }
      override def write(batch: org.apache.spark.sql.DataFrame, batchId: Long): Unit =
        throw new AssertionError("legacy write must not be used")
    }
    val base = decodedFrames("e1", 1718409600000L, 1L)
    val valid = base.valid.unionByName(decodedFrames("e2", 1718409601000L, 2L).valid)

    ExecutionEngine.processDecodedBatch(
      Seq(Array[Byte](1)).toDF("value"), 5L,
      _ => DecodedEventFrames(valid, base.deadLetters), archive,
      Seq(identity[org.apache.spark.sql.DataFrame]), Seq.empty, Seq(partial), maxRetries = 1)

    applied shouldBe Seq("e1", "e2")
    contexts should have size 2
    contexts.distinct should have size 1
    contexts.head.batchId shouldBe 5L
    contexts.head.queryIdentity shouldBe QueryIdentity
  }
}
