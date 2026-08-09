package com.demo.event

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.avro.generic.GenericData
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventAvroCodecSpec extends AnyFlatSpec with Matchers with OptionValues {

  private def readResource(path: String): Array[Byte] = {
    val stream = getClass.getResourceAsStream(path)
    try stream.readAllBytes()
    finally stream.close()
  }

  private val requiredStringFields = Seq(
    "event_id" -> "e-cross-language",
    "user_id" -> "u-cross-language",
    "item_id" -> "i-cross-language",
    "event_type" -> "click"
  )
  private val timestampMs = 1718400000000L

  "EventAvroCodec" should "decode the Python single-object fixture" in {
    val bytes = Files.readAllBytes(Paths.get(getClass.getResource("/avro/python-recsys-event-v1.bin").toURI))
    val schema = new Schema.Parser().parse(
      new String(readResource("/schemas/recsys-event-v1.avsc"), StandardCharsets.UTF_8)
    )

    bytes.take(10).toSeq shouldBe Seq(
      0xc3.toByte, 0x01.toByte, 0xab.toByte, 0x79.toByte, 0x79.toByte,
      0x48.toByte, 0x5f.toByte, 0x27.toByte, 0x5b.toByte, 0x22.toByte
    )
    ByteBuffer.wrap(bytes, 2, 8).order(ByteOrder.LITTLE_ENDIAN).getLong shouldBe
      SchemaNormalization.parsingFingerprint64(schema)
    EventAvroCodec.fingerprint shouldBe SchemaNormalization.parsingFingerprint64(schema)

    val decoded = EventAvroCodec.decode(bytes).toOption.value
    requiredStringFields.foreach { case (field, value) => decoded.get(field).toString shouldBe value }
    decoded.get("timestamp_ms") shouldBe timestampMs
  }

  it should "keep its checked-in schema semantically identical to the canonical schema" in {
    val resourceSchema = new Schema.Parser().parse(
      new String(readResource("/schemas/recsys-event-v1.avsc"), StandardCharsets.UTF_8)
    )
    val canonicalSchemaPath = Seq(
      Paths.get("recsys-pipeline/schemas/recsys-event-v1.avsc"),
      Paths.get("../../schemas/recsys-event-v1.avsc")
    ).find(path => Files.exists(path)).value
    val canonicalSchema = new Schema.Parser().parse(
      new String(Files.readAllBytes(canonicalSchemaPath), StandardCharsets.UTF_8)
    )

    resourceSchema.toString shouldBe canonicalSchema.toString
  }

  it should "return invalid_marker for a malformed single-object header" in {
    EventAvroCodec.decode(Array[Byte](0, 1, 2)) match {
      case Left(failure) => failure.code shouldBe "invalid_marker"
      case Right(_)      => fail("malformed marker was decoded")
    }
  }

  it should "return unknown_fingerprint for an unregistered writer schema" in {
    val bytes = Array(0xc3.toByte, 0x01.toByte) ++ Array.fill[Byte](8)(0)

    EventAvroCodec.decode(bytes) match {
      case Left(failure) => failure.code shouldBe "unknown_fingerprint"
      case Right(_)      => fail("unknown writer schema was decoded")
    }
  }

  it should "return corrupt_payload when a recognized header has no Avro record" in {
    val bytes = EventAvroCodec.Magic ++
      ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(EventAvroCodec.fingerprint).array()

    EventAvroCodec.decode(bytes) match {
      case Left(failure) => failure.code shouldBe "corrupt_payload"
      case Right(_)      => fail("truncated payload was decoded")
    }
  }

  it should "return corrupt_payload when a record has trailing bytes" in {
    val fixture = Files.readAllBytes(Paths.get(getClass.getResource("/avro/python-recsys-event-v1.bin").toURI))

    EventAvroCodec.decode(fixture ++ Array[Byte](0)) match {
      case Left(failure) => failure.code shouldBe "corrupt_payload"
      case Right(_)      => fail("trailing bytes were accepted")
    }
  }

  it should "encode a complete generic record as a single-object payload" in {
    val record = new GenericData.Record(EventAvroCodec.schema)
    requiredStringFields.foreach { case (field, value) => record.put(field, value) }
    record.put("timestamp_ms", timestampMs)

    val decoded = EventAvroCodec.decode(EventAvroCodec.encode(record)).toOption.value

    requiredStringFields.foreach { case (field, value) => decoded.get(field).toString shouldBe value }
    decoded.get("timestamp_ms") shouldBe timestampMs
  }

  it should "report required_field when encoding a record with a missing required value" in {
    val record = new GenericData.Record(EventAvroCodec.schema)
    requiredStringFields.tail.foreach { case (field, value) => record.put(field, value) }
    record.put("timestamp_ms", timestampMs)

    val exception = intercept[EventAvroCodec.RequiredFieldException] {
      EventAvroCodec.encode(record)
    }

    exception.failure.code shouldBe "required_field"
  }
}
