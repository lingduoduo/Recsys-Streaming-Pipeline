package com.demo.engine

import java.nio.file.Files
import java.sql.Timestamp

import com.demo.SparkTestSupport
import com.demo.event.DecodedEventFrames
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.storage.StorageLevel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class RawArchiveSinkSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

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
    val sink = new RawArchiveSink(validPath.toString, deadPath.toString)
    val valid = Seq(
      ("e1", 1718409600000L, "recsys_events", 2, 42L,
        Timestamp.valueOf("2024-06-15 00:00:01"), 1234L)
    ).toDF("event_id", "timestamp_ms", "kafka_topic", "kafka_partition", "kafka_offset",
      "kafka_timestamp", "schema_fingerprint")

    sink.writeValid(valid, batchId = 7L)
    sink.writeValid(valid, batchId = 7L)

    val batchPath = validPath.resolve("_batches").resolve("7")
    containsPath(batchPath, "date=2024-06-15") shouldBe true
    val archived = spark.read.parquet(batchPath.toString)
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
    val sink = new RawArchiveSink(validPath.toString, deadPath.toString)
    val deadLetters = Seq(
      ("recsys_events", 3, 99L, Timestamp.valueOf("2024-06-16 01:02:03"),
        Array[Byte](1, 2, 3), "corrupt_payload", "truncated")
    ).toDF("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
      "raw_value", "error_code", "error_detail")

    sink.writeDeadLetters(deadLetters, batchId = 8L)
    sink.writeDeadLetters(deadLetters, batchId = 8L)

    val batchPath = deadPath.resolve("_batches").resolve("8")
    containsPath(batchPath, "date=2024-06-16") shouldBe true
    val archived = spark.read.parquet(batchPath.toString)
    archived.count() shouldBe 1L
    archived.select("error_code").as[String].head() shouldBe "corrupt_payload"
    archived.columns should contain allOf ("raw_value", "error_detail", "archived_at", "date")
  }

  "ExecutionEngine.processDecodedBatch" should "archive both outcomes before invoking valid-event stages and sinks" in {
    val s = spark
    import s.implicits._
    val calls = scala.collection.mutable.ArrayBuffer[String]()
    val frames = DecodedEventFrames(Seq("e1").toDF("event_id"), Seq("bad").toDF("error_code"))
    val archive = new RawArchiveSink("unused-valid", "unused-dead") {
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
      val archive = new RawArchiveSink("unused-valid", "unused-dead") {
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
    val archive = new RawArchiveSink(root.resolve("valid").toString, root.resolve("dead").toString)
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

  it should "expire archived event-id state at the configured watermark horizon" in {
    val s = spark
    import s.implicits._
    val root = Files.createTempDirectory("engine-watermark-dedupe")
    val archive = new RawArchiveSink(root.resolve("valid").toString, root.resolve("dead").toString)
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
    val stateDirectories = Files.list(root.resolve("valid").resolve("_dedupe").resolve("default"))
    try {
      stateDirectories.iterator().asScala
        .map(_.getFileName.toString)
        .filter(_.forall(_.isDigit))
        .toSet shouldBe Set("2")
    } finally stateDirectories.close()
  }
}
