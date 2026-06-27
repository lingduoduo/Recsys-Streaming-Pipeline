package com.demo.process

import com.demo.event.{EventParsing, EventSchemas}
import com.demo.util.{BatchMetricsListener, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object OnlineJoinerStreamingJob {

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("ONLINE_JOINER_INPUT_TOPIC", "recsys_events")
    val outputTopic = sys.env.getOrElse("ONLINE_JOINER_OUTPUT_TOPIC", "training_samples")
    val outputPath = sys.env.getOrElse("ONLINE_JOINER_HDFS_OUTPUT_PATH", "/tmp/spark-recsys/training-samples")
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/online-joiner"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")
    val outputFiles = math.max(1, com.demo.util.Env.int("ONLINE_JOINER_OUTPUT_FILES", 1))
    val catalogPath = sys.env.getOrElse("ONLINE_JOINER_CATALOG_PATH", "")

    val spark = SparkSessions.create("OnlineJoinerStreamingJob")
    BatchMetricsListener.register(spark)

    // Load the shared item catalog once (small → broadcast on each join). Empty path = no
    // catalog, in which case the Parquet output still carries empty genres/tags columns so the
    // schema stays stable. Same catalog.json the Java service reads (single source of truth).
    val catalog: Option[DataFrame] = if (catalogPath.nonEmpty) Some(loadCatalog(spark, catalogPath)) else None

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "earliest"))
      .option("kafka.group.id", sys.env.getOrElse("KAFKA_GROUP_ID", "training-joiner"))
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    val watermarkDelay = sys.env.getOrElse("EVENT_WATERMARK_DELAY", "10 minutes")
    val events = dedupedEvents(raw, watermarkDelay)

    events.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        // buildTrainingSamples now does a single-pass groupBy (one shuffle) instead of
        // filter+groupBy+join (two shuffles), so events no longer needs to be persisted.
        val samples = buildTrainingSamples(batch)
          .withColumn("batch_id", lit(batchId))
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        try {
          samples
            .select(
              col("sample_id").as("key"),
              to_json(struct(samples.columns.map(col): _*)).as("value")
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("topic", outputTopic)
            .save()

          // Catalog genres/tags land in Parquet only; the Kafka training_samples value is left
          // unchanged so the downstream ExperienceCollector contract is untouched.
          val parquet = withCatalog(samples, catalog).withColumn("date", to_date(col("impression_time")))
          writeParquet(parquet, outputPath, outputFiles)
        } finally {
          samples.unpersist()
        }
        ()
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }

  /** Read the shared catalog JSON ({itemId: {genres:[...], tags:[...], ...}}) into a long-form
    * DataFrame of (item_id, genres, tags). Reads the whole file as one string and parses it with
    * Spark's own JSON support, so no extra dependency and the same object-map file the Java
    * service consumes works unchanged. */
  def loadCatalog(spark: SparkSession, path: String): DataFrame = {
    val entry = StructType(Seq(
      StructField("genres", ArrayType(StringType), nullable = true),
      StructField("tags", ArrayType(StringType), nullable = true)
    ))
    spark.read.option("wholetext", "true").text(path)
      .select(from_json(col("value"), MapType(StringType, entry)).as("m"))
      .select(explode(col("m")).as(Seq("item_id", "profile")))
      .select(
        col("item_id"),
        col("profile.genres").as("genres"),
        col("profile.tags").as("tags")
      )
  }

  /** Attach genres/tags to the samples. When no catalog is configured, add empty arrays so the
    * Parquet schema is identical with or without a catalog. */
  def withCatalog(samples: DataFrame, catalog: Option[DataFrame]): DataFrame =
    catalog match {
      case Some(cat) => enrichWithCatalog(samples, cat)
      case None =>
        samples
          .withColumn("genres", typedLit(Seq.empty[String]))
          .withColumn("tags", typedLit(Seq.empty[String]))
    }

  /** Broadcast left-join the catalog onto samples by item_id; unmatched items get empty arrays. */
  def enrichWithCatalog(samples: DataFrame, catalog: DataFrame): DataFrame =
    samples
      .join(broadcast(catalog), Seq("item_id"), "left")
      .withColumn("genres", coalesce(col("genres"), typedLit(Seq.empty[String])))
      .withColumn("tags", coalesce(col("tags"), typedLit(Seq.empty[String])))

  def writeParquet(samples: DataFrame, path: String, outputFiles: Int): Unit =
    samples
      .coalesce(math.max(1, outputFiles))   // bound per-batch file count (avoid small-files blowup)
      .write
      .mode("append")
      .partitionBy("date")
      .format("parquet")
      .save(path)

  def dedupedEvents(raw: DataFrame, watermarkDelay: String): DataFrame =
    EventParsing.dedupeWithinWatermark(parseEvents(raw), to_timestamp(from_unixtime(col("timestamp"))), watermarkDelay)

  def parseEvents(rawKafka: DataFrame): DataFrame =
    EventParsing.observeIngest(EventParsing.fromJson(rawKafka, EventSchemas.joiner))
      .withColumn("timestamp",
        coalesce(col("timestamp_ms") / 1000L, col("timestamp")))
      .drop("timestamp_ms")
      .filter(
        col("request_id").isNotNull &&
          col("user_id").isNotNull &&
          col("item_id").isNotNull &&
          col("event_type").isNotNull &&
          col("timestamp").isNotNull
      )

  def buildTrainingSamples(events: DataFrame): DataFrame = {
    val isImpression = col("etype").isin("impression", "exposure")
    val isFeedback   = col("etype").isin("click", "order", "purchase")

    events
      .withColumn("etype", lower(trim(col("event_type"))))
      // Single-pass conditional groupBy: one shuffle replaces (groupBy feedback) + (left join).
      // Because the producer keys behavior events by request_id, all events in a slate
      // co-partition in Kafka → Spark reads them together, making this groupBy partition-local.
      .groupBy("request_id", "user_id", "item_id")
      .agg(
        max(when(isImpression, col("position"))).as("position"),
        max(when(isImpression, col("timestamp"))).as("impression_ts"),
        max(when(isImpression, to_timestamp(from_unixtime(col("timestamp"))))).as("impression_time"),
        first(when(isImpression, col("user_features")),    ignoreNulls = true).as("user_features"),
        first(when(isImpression, col("item_features")),    ignoreNulls = true).as("item_features"),
        first(when(isImpression, col("context_features")), ignoreNulls = true).as("context_features"),
        max(when(col("etype") === "click", lit(1)).otherwise(lit(0))).as("clicked"),
        max(when(col("etype").isin("order", "purchase"), lit(1)).otherwise(lit(0))).as("ordered"),
        max(when(isFeedback, col("timestamp"))).as("last_feedback_ts")
      )
      // Drop groups that have no impression in this batch (pure late-feedback events)
      .filter(col("impression_ts").isNotNull)
      .select(
        concat_ws(":", col("request_id"), col("user_id"), col("item_id")).as("sample_id"),
        col("request_id"),
        col("user_id"),
        col("item_id"),
        coalesce(col("position"), lit(0)).as("position"),
        col("impression_ts"),
        col("impression_time"),
        coalesce(col("clicked"), lit(0)).as("clicked"),
        coalesce(col("ordered"), lit(0)).as("ordered"),
        when(coalesce(col("ordered"), lit(0)) === 1, lit(2.0))
          .when(coalesce(col("clicked"), lit(0)) === 1, lit(1.0))
          .otherwise(lit(0.0)).as("label"),
        col("last_feedback_ts"),
        coalesce(col("user_features"),     typedLit(Map.empty[String, String])).as("user_features"),
        coalesce(col("item_features"),     typedLit(Map.empty[String, String])).as("item_features"),
        coalesce(col("context_features"),  typedLit(Map.empty[String, String])).as("context_features")
      )
  }
}
