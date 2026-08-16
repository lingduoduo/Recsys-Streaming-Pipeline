package com.demo.process

import com.demo.engine.{BatchStage, EngineConfig, ExecutionEngine, KafkaSink, KafkaSource, ParquetSink, RawArchiveSink, Sink, Stage}
import com.demo.event.{DecodedEventBatch, EventParsing}
import com.demo.util.{Env, SparkSessions, TimePartitions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object OnlineJoinerStreamingJob {

  private[process] val MeasurementFields: Seq[(String, DataType)] = Seq(
    "model_version" -> StringType,
    "policy_version" -> StringType,
    "algorithm_version" -> StringType,
    "rating" -> DoubleType,
    "negative_feedback_reason" -> StringType,
    "dwell_millis" -> LongType,
    "completion_rate" -> DoubleType,
    "published_at" -> LongType,
    "new_release" -> BooleanType,
    "filter_reason" -> StringType,
    "unsafe_label" -> BooleanType
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSessions.create("OnlineJoinerStreamingJob")

    val cfg = EngineConfig(
      bootstrapServers     = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      inputTopic           = sys.env.getOrElse("ONLINE_JOINER_INPUT_TOPIC", "recsys_events"),
      startingOffsets      = sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "earliest"),
      groupId              = sys.env.getOrElse("KAFKA_GROUP_ID", "training-joiner"),
      maxOffsetsPerTrigger = Env.int("MAX_OFFSETS_PER_TRIGGER", 5000),
      triggerInterval      = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds"),
      checkpointLocation   = sys.env.getOrElse("SPARK_CHECKPOINT_LOCATION", "/tmp/spark-recsys/online-joiner"),
      watermarkDelay       = sys.env.getOrElse("EVENT_WATERMARK_DELAY", "10 minutes"),
      sinkMaxRetries       = Env.int("SINK_MAX_RETRIES", 0)
    )
    EngineConfig.validate(cfg) match {
      case Left(errors) =>
        errors.foreach(e => System.err.println(s"[config] $e"))
        sys.exit(1)
      case Right(_) => ()
    }

    val outputTopic = sys.env.getOrElse("ONLINE_JOINER_OUTPUT_TOPIC", "training_samples")
    val outputPath  = sys.env.getOrElse("ONLINE_JOINER_HDFS_OUTPUT_PATH", "/tmp/spark-recsys/training-samples")
    val outputFiles = math.max(1, Env.int("ONLINE_JOINER_OUTPUT_FILES", 1))
    val catalogPath = sys.env.getOrElse("ONLINE_JOINER_CATALOG_PATH", "")
    val catalog: Option[DataFrame] = if (catalogPath.nonEmpty) Some(loadCatalog(spark, catalogPath)) else None
    val archiveRoot =
      sys.env.getOrElse("RECSYS_EVENT_ARCHIVE_PATH", "/tmp/spark-recsys/recsys-events-archive")
    val archive = new RawArchiveSink(
      archiveRoot,
      sys.env.getOrElse("RECSYS_EVENT_DEAD_LETTER_PATH", "/tmp/spark-recsys/recsys-events-dead-letter"),
      cfg.checkpointLocation
    )
    // Feedback arriving within this window joins its impression; a sample publishes once, when
    // its window closes. "0 seconds" restores the pre-change per-batch behaviour.
    val lateFeedbackJoin = new LateFeedbackJoin(
      archiveRoot,
      archive.stableQueryNamespace,
      sys.env.getOrElse("FEEDBACK_JOIN_WAIT", "3 minutes"))

    val streamingStages: Seq[Stage] = Seq((df: DataFrame) => dedupedEvents(df, cfg.watermarkDelay))
    val batchStages: Seq[BatchStage] =
      Seq((df: DataFrame, id: Long) =>
        lateFeedbackJoin.process(df, id, System.currentTimeMillis()).withColumn("batch_id", lit(id)))
    val sinks: Seq[Sink] = Seq(
      new KafkaSink(cfg.bootstrapServers, outputTopic, "sample_id"),
      new ParquetSink(outputPath, "date", outputFiles,
        (df: DataFrame) => withDatePartition(df, catalog))
    )

    ExecutionEngine.run(
      spark, cfg, KafkaSource, DecodedEventBatch.decode _, archive, streamingStages, batchStages, sinks)
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

  /** Enrich, then stamp the Parquet partition key.
    *
    * The partition is derived from `impression_ts` (epoch seconds) rather than from `to_date` of
    * the local `impression_time`, so it does not move with the deploy machine's time zone.
    * `CtrRankingModelTrainingJob.splitByDate` uses this value as its train/holdout key, and the
    * raw archive already partitions by UTC — the two must agree. */
  def withDatePartition(samples: DataFrame, catalog: Option[DataFrame]): DataFrame =
    withCatalog(samples, catalog).withColumn("date", TimePartitions.utcDate(col("impression_ts")))

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

  def dedupedEvents(raw: DataFrame, watermarkDelay: String): DataFrame =
    EventParsing.dedupeWithinWatermark(parseEvents(raw), to_timestamp(from_unixtime(col("timestamp"))), watermarkDelay)

  /** `timestamp` (seconds) is the published unit behind `impression_ts` and its six consumers;
    * `timestamp_ms` rides alongside so `feedback_delay_ms` can be a real millisecond delta rather
    * than a second-granularity value wearing a millisecond name. */
  def parseEvents(rawKafka: DataFrame): DataFrame =
    EventParsing.observeIngest(EventParsing.canonicalEvents(rawKafka))
      .withColumn("timestamp", (col("timestamp_ms") / 1000L).cast(LongType))
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

    val withMeasurementFields = MeasurementFields.foldLeft(events) { case (df, (name, dataType)) =>
      if (df.columns.contains(name)) df else df.withColumn(name, lit(null).cast(dataType))
    }
    val withEventId =
      if (withMeasurementFields.columns.contains("event_id")) withMeasurementFields
      else withMeasurementFields.withColumn("event_id", lit(null).cast(StringType))

    // Both an absent column (callers that only carry seconds) and a null value (a snapshot written
    // before timestamp_ms joined SnapshotColumns) fall back to the second-precision timestamp.
    val secondsAsMillis = (col("timestamp") * 1000L).cast(LongType)
    val withTimestampMs =
      if (withEventId.columns.contains("timestamp_ms"))
        withEventId.withColumn("timestamp_ms", coalesce(col("timestamp_ms"), secondsAsMillis))
      else withEventId.withColumn("timestamp_ms", secondsAsMillis)

    withTimestampMs
      .withColumn("etype", lower(trim(col("event_type"))))
      // Single-pass conditional groupBy: one shuffle replaces (groupBy feedback) + (left join).
      // Because the producer keys behavior events by request_id, all events in a slate
      // co-partition in Kafka → Spark reads them together, making this groupBy partition-local.
      .groupBy("request_id", "user_id", "item_id")
      .agg(
        max(when(isImpression, col("position"))).as("position"),
        max(when(isImpression, col("timestamp"))).as("impression_ts"),
        max(when(isImpression, col("timestamp_ms"))).as("impression_ts_ms"),
        max(when(isImpression, to_timestamp(from_unixtime(col("timestamp"))))).as("impression_time"),
        first(when(isImpression, col("user_features")),    ignoreNulls = true).as("user_features"),
        first(when(isImpression, col("item_features")),    ignoreNulls = true).as("item_features"),
        first(when(isImpression, col("context_features")), ignoreNulls = true).as("context_features"),
        max(when(col("etype") === "click", lit(1)).otherwise(lit(0))).as("clicked"),
        max(when(col("etype").isin("order", "purchase"), lit(1)).otherwise(lit(0))).as("ordered"),
        max_by(
          when(isImpression, struct(
            col("model_version"), col("policy_version"), col("algorithm_version"),
            col("published_at"), col("new_release"), col("filter_reason"), col("unsafe_label")
          )),
          when(isImpression, struct(col("timestamp"), coalesce(col("event_id"), lit(""))))
        ).as("impression_measurement"),
        max_by(
          when(isFeedback, struct(
            col("timestamp").as("last_feedback_ts"), col("timestamp_ms").as("last_feedback_ts_ms"),
            col("rating"), col("negative_feedback_reason"),
            col("dwell_millis"), col("completion_rate")
          )),
          when(isFeedback, struct(col("timestamp"), coalesce(col("event_id"), lit(""))))
        ).as("feedback_measurement"),
        // session_id is constant across a slate's events; carry it through (one session per request).
        first(col("session_id"), ignoreNulls = true).as("session_id")
      )
      // Drop groups that have no impression in this batch (pure late-feedback events)
      .filter(col("impression_ts").isNotNull)
      .select(
        concat_ws(":", col("request_id"), col("user_id"), col("item_id")).as("sample_id"),
        col("request_id"),
        col("user_id"),
        col("item_id"),
        // No coalesce: 0 is a real slot, so collapsing "unknown" onto it makes a positionless
        // impression indistinguishable from the top of the slate.
        col("position"),
        col("impression_ts"),
        col("impression_time"),
        coalesce(col("clicked"), lit(0)).as("clicked"),
        coalesce(col("ordered"), lit(0)).as("ordered"),
        when(coalesce(col("ordered"), lit(0)) === 1, lit(2.0))
          .when(coalesce(col("clicked"), lit(0)) === 1, lit(1.0))
          .otherwise(lit(0.0)).as("label"),
        col("feedback_measurement.last_feedback_ts").as("last_feedback_ts"),
        // From the millisecond pair, not (seconds delta) * 1000: the producers happen to emit
        // whole-second feedback offsets, which hides a truncation error real traffic would show.
        when(col("feedback_measurement.last_feedback_ts_ms").isNotNull,
          (col("feedback_measurement.last_feedback_ts_ms") - col("impression_ts_ms")).cast(LongType)
        ).as("feedback_delay_ms"),
        col("impression_measurement.model_version").as("model_version"),
        col("impression_measurement.policy_version").as("policy_version"),
        col("impression_measurement.algorithm_version").as("algorithm_version"),
        col("feedback_measurement.rating").as("rating"),
        col("feedback_measurement.negative_feedback_reason").as("negative_feedback_reason"),
        col("feedback_measurement.dwell_millis").as("dwell_millis"),
        col("feedback_measurement.completion_rate").as("completion_rate"),
        col("impression_measurement.published_at").as("published_at"),
        col("impression_measurement.new_release").as("new_release"),
        col("impression_measurement.filter_reason").as("filter_reason"),
        col("impression_measurement.unsafe_label").as("unsafe_label"),
        coalesce(col("session_id"), lit("")).as("session_id"),
        coalesce(col("user_features"),     typedLit(Map.empty[String, String])).as("user_features"),
        coalesce(col("item_features"),     typedLit(Map.empty[String, String])).as("item_features"),
        coalesce(col("context_features"),  typedLit(Map.empty[String, String])).as("context_features")
      )
  }
}
