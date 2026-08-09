package com.demo.engine

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{
  BooleanType, DateType, LongType, StringType, StructField, StructType, TimestampType
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val durableContext = SinkWriteContext(
    "checkpoint://sink-spec", "query-ns", "sink-spec", "sink-ns", 7L)
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("SinkSpec")
      .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()

  "KafkaSink.payload" should "produce key/value with the value carrying all columns as JSON" in {
    val s = spark; import s.implicits._
    val df = Seq(("k1", "v1")).toDF("id", "other")
    val out = new KafkaSink("localhost:9092", "t", "id").payload(df)
    out.columns should contain allOf ("key", "value")
    val row = out.first()
    row.getAs[String]("key") shouldBe "k1"
    val json = row.getAs[String]("value")
    json should include ("\"id\":\"k1\"")
    json should include ("\"other\":\"v1\"")
  }

  it should "carry the stable query sink and batch identity on durable writes" in {
    val s = spark; import s.implicits._
    val df = Seq(("k1", "v1")).toDF("id", "other")

    val row = new KafkaSink("localhost:9092", "t", "id")
      .durablePayload(df, durableContext).first()
    val headers = row.getAs[Seq[Row]]("headers")
      .map(header => header.getAs[String]("key") ->
        new String(header.getAs[Array[Byte]]("value"), "UTF-8"))
      .toMap

    row.getAs[String]("key") shouldBe "k1"
    headers shouldBe Map(
      "recsys_query_namespace" -> "query-ns",
      "recsys_sink_namespace" -> "sink-ns",
      "recsys_batch_id" -> "7")
  }

  "ParquetSink" should "write at most outputFiles parquet files per partition" in {
    import java.nio.file.Files
    val s = spark; import s.implicits._
    val dir = Files.createTempDirectory("parquet-sink").toFile
    val out = new java.io.File(dir, "samples").getAbsolutePath

    val batch = Seq(
      ("s1", java.sql.Timestamp.valueOf("2026-06-26 00:00:00")),
      ("s2", java.sql.Timestamp.valueOf("2026-06-26 00:00:00")),
      ("s3", java.sql.Timestamp.valueOf("2026-06-26 00:00:00"))
    ).toDF("sample_id", "impression_time")

    val sink = new ParquetSink(out, "date", outputFiles = 1,
      transform = df => df.withColumn("date", to_date(col("impression_time"))))
    sink.write(batch, 0L)

    val partDir = new java.io.File(out, "date=2026-06-26")
    partDir.listFiles().count(_.getName.endsWith(".parquet")) shouldBe 1
  }

  it should "commit one deterministic durable directory per query sink and batch" in {
    import java.nio.file.{Files, Paths}
    val s = spark; import s.implicits._
    val root = Files.createTempDirectory("durable-parquet-sink")
    val batch = Seq(
      ("s1", java.sql.Timestamp.valueOf("2026-06-26 00:00:00")),
      ("s2", java.sql.Timestamp.valueOf("2026-06-26 00:00:00"))
    ).toDF("sample_id", "impression_time")
    val sink = new ParquetSink(root.toString, "date", outputFiles = 1,
      transform = df => df.withColumn("date", to_date(col("impression_time"))))

    sink.writeDurably(batch, durableContext)
    sink.writeDurably(batch, durableContext)

    val committed = Paths.get(sink.committedBatchPath(durableContext))
    Files.exists(committed.resolve("_SUCCESS")) shouldBe true
    Files.exists(committed.resolve("_COMMITTED")) shouldBe true
    spark.read.parquet(committed.toString).count() shouldBe 2L
    val expectedSchema = StructType(Seq(
      StructField("sample_id", StringType, nullable = true),
      StructField("impression_time", TimestampType, nullable = true),
      StructField("date", DateType, nullable = true)))
    val fromConfiguredRoot = DurableParquetCommit.readIdentity(
      spark, root.toString, durableContext.queryNamespace, durableContext.sinkNamespace,
      expectedSchema)
    fromConfiguredRoot.select("sample_id").as[String].collect().toSet shouldBe Set("s1", "s2")
    fromConfiguredRoot.columns should contain allOf ("date", "query", "sink", "batch")
  }

  it should "reject payload schemas that collide with visible identity partition columns" in {
    import java.nio.file.Files
    val s = spark; import s.implicits._

    Seq("query", "sink", "batch").foreach { reserved =>
      val root = Files.createTempDirectory(s"durable-parquet-reserved-$reserved")
      val payload = Seq(("sample-1", "payload-value")).toDF("sample_id", reserved)

      val error = intercept[IllegalArgumentException] {
        DurableParquetCommit.write(payload, root.toString, Seq.empty, durableContext)
      }

      error.getMessage should include (s"reserved Parquet control column '$reserved'")
    }
  }

  it should "read complete incompatible schemas only from an exact shared-root identity" in {
    import java.nio.file.Files
    val s = spark; import s.implicits._
    val root = Files.createTempDirectory("durable-parquet-shared-root")
    val queryA = durableContext.copy(sinkNamespace = "sink-a", sinkIdentity = "sink-a")
    val queryB = durableContext.copy(
      sinkNamespace = "sink-b", sinkIdentity = "sink-b", batchId = 8L)
    val schemaA = StructType(Seq(
      StructField("sample_id", StringType, nullable = true),
      StructField("score", LongType, nullable = false)))
    val schemaB = StructType(Seq(
      StructField("sample_id", StringType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("enabled", BooleanType, nullable = false)))

    DurableParquetCommit.write(
      Seq(("from-a1", 11L), ("from-a2", 12L)).toDF("sample_id", "score"),
      root.toString, Seq.empty, queryA)
    DurableParquetCommit.write(
      Seq(("from-b1", "blue", true), ("from-b2", "green", false))
        .toDF("sample_id", "label", "enabled"),
      root.toString, Seq.empty, queryB)

    val fromA = DurableParquetCommit.readIdentity(
      spark, root.toString, queryA.queryNamespace, queryA.sinkNamespace, schemaA)
    fromA.columns should contain allOf ("sample_id", "score", "query", "sink", "batch")
    fromA.select("sample_id", "score").as[(String, Long)].collect().toSet shouldBe
      Set("from-a1" -> 11L, "from-a2" -> 12L)

    val fromB = DurableParquetCommit.readIdentity(
      spark, root.toString, queryB.queryNamespace, queryB.sinkNamespace, schemaB)
    fromB.columns should contain allOf ("sample_id", "label", "enabled", "query", "sink", "batch")
    fromB.select("sample_id", "label", "enabled").as[(String, String, Boolean)]
      .collect().toSet shouldBe Set(
        ("from-b1", "blue", true),
        ("from-b2", "green", false))
  }
}
