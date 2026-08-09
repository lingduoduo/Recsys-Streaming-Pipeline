package com.demo.event

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.util.{Failure, Success, Try}

object EventAvroCodec {
  val Magic: Array[Byte] = Array(0xc3.toByte, 0x01.toByte)

  private val RequiredFields = Seq("event_id", "user_id", "item_id", "event_type", "timestamp_ms")

  lazy val schema: Schema = {
    val input = Option(getClass.getResourceAsStream("/schemas/recsys-event-v1.avsc"))
      .getOrElse(throw new IllegalStateException("missing recsys event Avro schema resource"))
    try new Schema.Parser().parse(input)
    finally input.close()
  }

  lazy val fingerprint: Long = SchemaNormalization.parsingFingerprint64(schema)

  sealed trait DecodeFailure {
    def code: String
    def detail: String
  }

  object DecodeFailure {
    final case class InvalidMarker(detail: String) extends DecodeFailure {
      val code = "invalid_marker"
    }

    final case class UnknownFingerprint(detail: String) extends DecodeFailure {
      val code = "unknown_fingerprint"
    }

    final case class CorruptPayload(detail: String) extends DecodeFailure {
      val code = "corrupt_payload"
    }

    final case class RequiredField(detail: String) extends DecodeFailure {
      val code = "required_field"
    }
  }

  final class RequiredFieldException(val failure: DecodeFailure.RequiredField)
      extends IllegalArgumentException(failure.detail)

  def decode(bytes: Array[Byte]): Either[DecodeFailure, GenericRecord] = {
    if (bytes == null || bytes.length < 10 || !bytes.take(2).sameElements(Magic)) {
      Left(DecodeFailure.InvalidMarker("invalid Avro single-object marker"))
    } else if (ByteBuffer.wrap(bytes, 2, 8).order(ByteOrder.LITTLE_ENDIAN).getLong != fingerprint) {
      Left(DecodeFailure.UnknownFingerprint("writer schema is not in the local catalog"))
    } else {
      Try {
        val decoder = DecoderFactory.get.binaryDecoder(bytes, 10, bytes.length - 10, null)
        val record = new GenericDatumReader[GenericRecord](schema).read(null, decoder)
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

  def encode(record: GenericRecord): Array[Byte] = {
    missingRequiredField(record) match {
      case Some(field) =>
        throw new RequiredFieldException(DecodeFailure.RequiredField(s"missing required field $field"))
      case None =>
        val output = new ByteArrayOutputStream()
        output.write(Magic)
        output.write(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(fingerprint).array())
        val encoder = EncoderFactory.get.binaryEncoder(output, null)
        new GenericDatumWriter[GenericRecord](schema).write(record, encoder)
        encoder.flush()
        output.toByteArray
    }
  }

  private def missingRequiredField(record: GenericRecord): Option[String] =
    if (record == null) Some("record")
    else RequiredFields.find(field => record.get(field) == null)

  private def errorDetail(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
}
