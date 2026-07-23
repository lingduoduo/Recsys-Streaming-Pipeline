package com.demo.process

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{coalesce, col}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OnlineJoinerStreamingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("OnlineJoinerStreamingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "buildTrainingSamples" should "join impressions with click and order labels" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("sess_1", "req_1", "user_1", "item_1", "impression", 100L, 0, Map("tier" -> "gold"), Map("genre" -> "drama"), Map("device" -> "ios")),
      ("sess_1", "req_1", "user_1", "item_2", "impression", 100L, 1, Map("tier" -> "gold"), Map("genre" -> "comedy"), Map("device" -> "ios")),
      ("sess_1", "req_1", "user_1", "item_1", "click", 105L, 0, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String]),
      ("sess_1", "req_1", "user_1", "item_2", "order", 120L, 1, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String])
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position", "user_features", "item_features", "context_features")

    val samples = OnlineJoinerStreamingJob.buildTrainingSamples(events)
    val rows = samples
      .select("item_id", "clicked", "ordered", "label")
      .collect()
      .map(row => row.getString(0) -> (row.getInt(1), row.getInt(2), row.getDouble(3)))
      .toMap

    rows("item_1") shouldBe (1, 0, 1.0)
    rows("item_2") shouldBe (0, 1, 2.0)
    // session_id is carried through from the slate's events
    samples.select("session_id").distinct().collect().map(_.getString(0)) shouldBe Array("sess_1")
  }

  it should "keep unclicked impressions as negative samples" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("sess_2", "req_2", "user_1", "item_9", "exposure", 200L, 3, Map.empty[String, String], Map("genre" -> "news"), Map.empty[String, String])
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position", "user_features", "item_features", "context_features")

    val row = OnlineJoinerStreamingJob.buildTrainingSamples(events).first()

    row.getAs[Int]("clicked") shouldBe 0
    row.getAs[Int]("ordered") shouldBe 0
    row.getAs[Double]("label") shouldBe 0.0
  }

  it should "parse unified schema with timestamp_ms field" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Simulate events arriving with timestamp_ms (millis) and no legacy timestamp.
    // This mirrors what parseEvents produces after normalisation.
    val events = Seq(
      // impression at ms 1718400000000
      ("sess_3", "req_3", "user_5", "movie_3", "impression", Some(1718400000000L), None: Option[Long],
        0, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String]),
      // click at ms 1718400005000
      ("sess_3", "req_3", "user_5", "movie_3", "click", Some(1718400005000L), None: Option[Long],
        0, Map.empty[String, String], Map.empty[String, String], Map.empty[String, String])
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp_ms", "timestamp",
           "position", "user_features", "item_features", "context_features")

    // Apply the same normalisation that parseEvents performs
    val normalised = events
      .withColumn("timestamp", coalesce(col("timestamp_ms") / 1000L, col("timestamp")))
      .drop("timestamp_ms")

    val rows = OnlineJoinerStreamingJob.buildTrainingSamples(normalised).collect()
    rows should have length 1
    rows.head.getAs[Int]("clicked") shouldBe 1

    val impressionTime = rows.head.getAs[java.sql.Timestamp]("impression_time")
    impressionTime should not be null
    // impression_time should be in year 2024, not 54426
    impressionTime.toLocalDateTime.getYear shouldBe 2024
  }

  "enrichWithCatalog" should "attach genres/tags by item_id and fill empty arrays for misses" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq("item_1", "item_2").toDF("item_id")
    val catalog = Seq(
      ("item_1", Seq("drama", "crime"), Seq("classic"))
    ).toDF("item_id", "genres", "tags")

    val out = OnlineJoinerStreamingJob.enrichWithCatalog(samples, catalog)
      .collect()
      .map(r => r.getString(r.fieldIndex("item_id")) ->
        (r.getAs[Seq[String]]("genres"), r.getAs[Seq[String]]("tags")))
      .toMap

    out("item_1") shouldBe (Seq("drama", "crime"), Seq("classic"))
    out("item_2") shouldBe (Seq.empty[String], Seq.empty[String])
  }

  "withCatalog" should "add empty genres/tags columns when no catalog is configured" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val samples = Seq("item_1").toDF("item_id")
    val out = OnlineJoinerStreamingJob.withCatalog(samples, None)

    out.columns should contain allOf ("genres", "tags")
    val row = out.first()
    row.getAs[Seq[String]]("genres") shouldBe Seq.empty[String]
    row.getAs[Seq[String]]("tags") shouldBe Seq.empty[String]
  }

  "loadCatalog" should "parse the object-map catalog JSON into (item_id, genres, tags)" in {
    import java.nio.file.{Files, Paths}
    val dir = Files.createTempDirectory("catalog").toFile
    val file = new java.io.File(dir, "catalog.json")
    Files.write(file.toPath,
      """{"item1":{"title":"A","genres":["sci-fi","adventure"],"tags":["space"]},
        | "item2":{"title":"B","genres":["drama"],"tags":[]}}""".stripMargin.getBytes)

    val rows = OnlineJoinerStreamingJob.loadCatalog(spark, file.getAbsolutePath)
      .collect()
      .map(r => r.getString(0) -> r.getAs[Seq[String]]("genres"))
      .toMap

    rows("item1") shouldBe Seq("sci-fi", "adventure")
    rows("item2") shouldBe Seq("drama")
  }

  "dedupedEvents" should "drop duplicate event_id within the watermark across micro-batches" in {
    val s = spark; import s.implicits._
    implicit val sqlCtx = s.sqlContext
    val input = MemoryStream[String]
    val deduped = OnlineJoinerStreamingJob.dedupedEvents(input.toDF(), "10 minutes")
    val q = deduped.writeStream.format("memory").queryName("oj_out").outputMode("append").start()
    try {
      val e = """{"event_id":"x1","request_id":"r1","user_id":"u1","item_id":"i1","event_type":"impression","timestamp_ms":1718400000000,"position":0}"""
      input.addData(e); q.processAllAvailable()
      input.addData(e); q.processAllAvailable()
      s.table("oj_out").count() shouldBe 1
    } finally q.stop()
  }
}
