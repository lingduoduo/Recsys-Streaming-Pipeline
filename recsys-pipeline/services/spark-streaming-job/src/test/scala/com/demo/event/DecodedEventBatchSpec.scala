package com.demo.event

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Timestamp

import com.demo.SparkTestSupport
import org.apache.avro.generic.GenericData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class DecodedEventBatchSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def encodedEvent(eventId: String): Array[Byte] = {
    val record = new GenericData.Record(EventAvroCodec.schema)
    record.put("event_id", eventId)
    record.put("request_id", "r1")
    record.put("session_id", "s1")
    record.put("user_id", "u1")
    record.put("item_id", "i1")
    record.put("event_type", "click")
    record.put("timestamp_ms", 1718400000000L)
    record.put("position", 2)
    record.put("user_features", Map("tier" -> "gold").asJava)
    EventAvroCodec.encode(record)
  }

  "DecodedEventBatch.decode" should "split valid events from stable structured failures" in {
    val s = spark
    import s.implicits._

    val unknownFingerprint = EventAvroCodec.Magic ++ Array.fill[Byte](8)(0)
    val corruptPayload = EventAvroCodec.Magic ++
      ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(EventAvroCodec.fingerprint).array()
    val kafkaTimestamp = Timestamp.valueOf("2024-06-15 12:34:56")
    val rawKafka = Seq(
      (encodedEvent("e1"), "recsys_events", 1, 10L, kafkaTimestamp),
      (unknownFingerprint, "recsys_events", 1, 11L, kafkaTimestamp),
      (corruptPayload, "recsys_events", 1, 12L, kafkaTimestamp)
    ).toDF("value", "topic", "partition", "offset", "timestamp")

    val frames = DecodedEventBatch.decode(rawKafka)

    frames.valid.select("event_id").as[String].collect() should contain only "e1"
    frames.valid.select("kafka_topic", "kafka_partition", "kafka_offset", "schema_fingerprint")
      .as[(String, Int, Long, Long)].head() shouldBe
      (("recsys_events", 1, 10L, EventAvroCodec.fingerprint))
    frames.valid.select("user_features").as[Map[String, String]].head() shouldBe Map("tier" -> "gold")

    val failures = frames.deadLetters.select("kafka_offset", "error_code")
      .as[(Long, String)].collect().toMap
    failures shouldBe Map(11L -> "unknown_fingerprint", 12L -> "corrupt_payload")
    frames.deadLetters.columns should contain allOf
      ("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "raw_value", "error_detail")
  }
}
