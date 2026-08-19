package com.demo.event

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

final case class DecodedEventFrames(valid: DataFrame, deadLetters: DataFrame)

object DecodedEventBatch {

  private lazy val eventSchema: StructType = avroRecordType(EventAvroCodec.schema)
  private lazy val decodedSchema = StructType(Seq(
    StructField("event", eventSchema, nullable = true),
    StructField("schema_fingerprint", LongType, nullable = true),
    StructField("error_code", StringType, nullable = true),
    StructField("error_detail", StringType, nullable = true)
  ))

  def decode(rawKafka: DataFrame): DecodedEventFrames = {
    val decodeUdf = udf(new UDF1[Array[Byte], Row] {
      override def call(bytes: Array[Byte]): Row = safeDecode(bytes)
    }, decodedSchema)
    val decoded = rawKafka.withColumn("decoded", decodeUdf(col("value")))
    val eventColumns = eventSchema.fieldNames.map(name => col(s"decoded.event.$name").as(name))
    val lineageColumns = Seq(
      col("topic").as("kafka_topic"),
      col("partition").as("kafka_partition"),
      col("offset").as("kafka_offset"),
      col("timestamp").as("kafka_timestamp")
    ) ++ optionalHeaders(rawKafka)

    val valid = decoded
      .filter(col("decoded.error_code").isNull)
      .select((eventColumns ++ lineageColumns :+ col("decoded.schema_fingerprint")): _*)

    val deadLetters = decoded
      .filter(col("decoded.error_code").isNotNull)
      .select((lineageColumns ++ Seq(
        col("value").as("raw_value"),
        col("decoded.schema_fingerprint").as("schema_fingerprint"),
        col("decoded.error_code").as("error_code"),
        col("decoded.error_detail").as("error_detail")
      )): _*)

    DecodedEventFrames(valid, deadLetters)
  }

  private def optionalHeaders(rawKafka: DataFrame) =
    if (rawKafka.columns.contains("headers")) Seq(col("headers").as("kafka_headers")) else Seq.empty

  private def safeDecode(bytes: Array[Byte]): Row = {
    // Parse the writer identity before consulting the local schema catalog so unknown writers are
    // still observable and routable in the dead-letter archive.
    val writerFingerprint = parsedWriterFingerprint(bytes)
    try {
      EventAvroCodec.decode(bytes) match {
        case Right(record) => Row(recordRow(record), writerFingerprint, null, null)
        case Left(failure) => Row(null, writerFingerprint, failure.code, failure.detail)
      }
    } catch {
      case NonFatal(error) =>
        Row(null, writerFingerprint, "corrupt_payload",
          Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
    }
  }

  private def parsedWriterFingerprint(bytes: Array[Byte]): java.lang.Long =
    if (bytes == null || bytes.length < 10 || !bytes.take(2).sameElements(EventAvroCodec.Magic)) null
    else java.lang.Long.valueOf(
      ByteBuffer.wrap(bytes, 2, 8).order(ByteOrder.LITTLE_ENDIAN).getLong)

  private def recordRow(record: GenericRecord): Row =
    Row.fromSeq(EventAvroCodec.schema.getFields.asScala.map(field => sparkValue(record.get(field.name))))

  private def sparkValue(value: Any): Any = value match {
    case null => null
    case text: Utf8 => text.toString
    case map: java.util.Map[_, _] =>
      map.asScala.map { case (key, entry) => key.toString -> sparkValue(entry) }.toMap
    case values: java.util.Collection[_] => values.asScala.map(sparkValue).toSeq
    case buffer: java.nio.ByteBuffer =>
      val copy = buffer.asReadOnlyBuffer()
      val bytes = new Array[Byte](copy.remaining())
      copy.get(bytes)
      bytes
    case other => other
  }

  private def avroRecordType(schema: Schema): StructType =
    StructType(schema.getFields.asScala.map { field =>
      StructField(field.name, avroType(field.schema), isNullable(field.schema))
    })

  private def avroType(schema: Schema): DataType = schema.getType match {
    case Schema.Type.RECORD  => avroRecordType(schema)
    case Schema.Type.ARRAY   => ArrayType(avroType(schema.getElementType), containsNull = false)
    case Schema.Type.MAP     => MapType(StringType, avroType(schema.getValueType), valueContainsNull = false)
    case Schema.Type.UNION   => avroType(schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).get)
    case Schema.Type.STRING  => StringType
    case Schema.Type.LONG    => LongType
    case Schema.Type.INT     => IntegerType
    case Schema.Type.DOUBLE  => DoubleType
    case Schema.Type.FLOAT   => FloatType
    case Schema.Type.BOOLEAN => BooleanType
    case Schema.Type.BYTES   => BinaryType
    case Schema.Type.ENUM    => StringType
    case other => throw new IllegalArgumentException(s"unsupported Avro field type $other")
  }

  private def isNullable(schema: Schema): Boolean =
    schema.getType == Schema.Type.UNION && schema.getTypes.asScala.exists(_.getType == Schema.Type.NULL)
}
