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
    val archive = new RawArchiveSink("unused-valid", "unused-dead", QueryIdentity) {
      override def writeValid(df: org.apache.spark.sql.DataFrame, batchId: Long): Unit = calls += "valid-archive"
      override def writeDeadLetters(df: org.apache.spark.sql.DataFrame, batchId: Long): Unit = calls += "dead-archive"
    }
    val stage: Stage = df => { calls += "stage"; df }
    val sink: Sink = (_, _) => calls += "business-sink"

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
      val archive = new RawArchiveSink("unused-valid", "unused-dead", QueryIdentity) {
        override def writeValid(df: org.apache.spark.sql.DataFrame, batchId: Long): Unit =
          if (failingWrite == "valid") throw new RuntimeException("valid archive failed")
        override def writeDeadLetters(df: org.apache.spark.sql.DataFrame, batchId: Long): Unit =
          if (failingWrite == "dead") throw new RuntimeException("dead archive failed")
      }
      val sink: Sink = (_, _) => businessCalls += 1

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
    val sink: Sink = (batch, _) =>
      batch.select("event_id").as[String].collect().foreach(received += _)

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
    val sink: Sink = (batch, _) =>
      batch.select("event_id").as[String].collect().foreach(received += _)

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
    val sink: Sink = (batch, _) =>
      batch.select("event_id").as[String].collect().foreach(received += _)

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
    val sink: Sink = (batch, _) =>
      batch.select("event_id").as[String].collect().foreach(received += _)
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
}
