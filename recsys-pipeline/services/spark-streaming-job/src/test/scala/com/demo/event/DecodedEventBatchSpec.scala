package com.demo.event

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Timestamp

import com.demo.SparkTestSupport
import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
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

  /** Hand-encodes a payload with the v1 writer schema (not `EventAvroCodec.schema`, which is
    * v3), so it carries the v1 fingerprint on the wire rather than whatever the codec's current
    * default happens to be. */
  private def encodedV1Event(eventId: String): Array[Byte] = {
    val v1Schema = {
      val input = getClass.getResourceAsStream("/schemas/recsys-event-v1.avsc")
      try new Schema.Parser().parse(input) finally input.close()
    }
    val record = new GenericData.Record(v1Schema)
    record.put("event_id", eventId)
    record.put("user_id", "u1")
    record.put("item_id", "i1")
    record.put("event_type", "click")
    record.put("timestamp_ms", 1718400000000L)

    val output = new ByteArrayOutputStream()
    output.write(EventAvroCodec.Magic)
    output.write(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      .putLong(SchemaNormalization.parsingFingerprint64(v1Schema)).array())
    val encoder = EncoderFactory.get.binaryEncoder(output, null)
    new GenericDatumWriter[GenericRecord](v1Schema).write(record, encoder)
    encoder.flush()
    output.toByteArray
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
      (corruptPayload, "recsys_events", 1, 12L, kafkaTimestamp),
      (Array[Byte](1, 2), "recsys_events", 1, 13L, kafkaTimestamp)
    ).toDF("value", "topic", "partition", "offset", "timestamp")

    val frames = DecodedEventBatch.decode(rawKafka)

    frames.valid.select("event_id").as[String].collect() should contain only "e1"
    frames.valid.select("kafka_topic", "kafka_partition", "kafka_offset", "schema_fingerprint")
      .as[(String, Int, Long, Long)].head() shouldBe
      (("recsys_events", 1, 10L, EventAvroCodec.fingerprint))
    frames.valid.select("user_features").as[Map[String, String]].head() shouldBe Map("tier" -> "gold")

    val failures = frames.deadLetters.select("kafka_offset", "error_code", "schema_fingerprint")
      .collect().map(row => row.getLong(0) ->
        (row.getString(1), if (row.isNullAt(2)) None else Some(row.getLong(2)))).toMap
    failures shouldBe Map(
      11L -> ("unknown_fingerprint", Some(0L)),
      12L -> ("corrupt_payload", Some(EventAvroCodec.fingerprint)),
      13L -> ("invalid_marker", None)
    )
    frames.deadLetters.columns should contain allOf
      ("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "raw_value",
        "schema_fingerprint", "error_detail")
  }

  it should "stamp a successfully decoded v1 payload with the v1 writer fingerprint, not v2's" in {
    val s = spark
    import s.implicits._

    val kafkaTimestamp = Timestamp.valueOf("2024-06-15 12:34:56")
    val rawKafka = Seq(
      (encodedV1Event("e-legacy"), "recsys_events", 1, 20L, kafkaTimestamp)
    ).toDF("value", "topic", "partition", "offset", "timestamp")

    val frames = DecodedEventBatch.decode(rawKafka)

    frames.valid.select("event_id").as[String].collect() should contain only "e-legacy"
    frames.valid.select("schema_fingerprint").as[Long].head() shouldBe 0x225B275F487979ABL
  }
}
