package com.demo.task

import com.demo.engine.{BatchStage, DurableSink, EngineConfig, ExecutionEngine, KafkaSource, RawArchiveSink, RedisPopularitySink, Sink, Stage}
import com.demo.event.{DecodedEventBatch, EventParsing, FieldGate, Gated}
import com.demo.sequence.{SequenceBusinessSink, SequenceEncoder, SequenceJobConfig, SequenceSchema, SequenceWriteMode}
import com.demo.util.{DropMetrics, Env, Reporter, SparkSessions}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UserEventStreamingJob {

  private val JobName = "UserEventStreamingJob"

  // Lazy so tests that import spark.implicits don't pay the full SparkSessions.create cost
  // unless they actually need the production session; in tests SparkTestSupport wins.
  lazy val spark: SparkSession =
    SparkSessions.create("UserEventStreamingJob", defaultShufflePartitions = 4)

  /** The actions this job records as one user behavior sequence. */
  val BehavioralActions: Seq[String] = Seq("search", "result_view", "detail_view", "click")

  private def isBlank(field: String): Column = coalesce(trim(col(field)), lit("")) === ""

  /** The v3 context fields the behavioral gate reads, projected as nulls when the decoded
    * frame has no column for them at all. Resolving a missing column is an analysis error,
    * not a null, so without this a frame narrower than the current contract would fail the
    * whole query rather than simply failing the gate the way an absent value does. */
  private def withBehaviorContext(df: DataFrame): DataFrame =
    Seq("query_id", "query_text", "result_set_id").foldLeft(df) { (acc, field) =>
      if (acc.columns.contains(field)) acc else acc.withColumn(field, lit(null).cast("string"))
    }

  /** Keep the behavioral subset and gate each action on the fields it cannot be read without.
    *
    * Non-behavioral actions — recommendation feedback and the rest of `recsys_events` — are
    * filtered out ahead of the gate rather than rejected by it, so they stay uncounted rather
    * than showing up as this job's drops. Within the subset the identity rules come first, so
    * a row with no user is reported as `null_user_id` and not as whatever its action lacked.
    *
    * `item_id` is action-specific now that the contract admits itemless searches: demanding
    * one globally, as this gate used to, would drop every search on the floor.
    */
  def normalize(df: DataFrame): Gated =
    FieldGate(withBehaviorContext(df).filter(col("event_type").isin(BehavioralActions: _*)), Seq(
      "null_user_id"  -> col("user_id").isNull,
      "null_event_id" -> col("event_id").isNull,
      "null_timestamp" -> col("timestamp_ms").isNull,
      "missing_search_query" ->
        (col("event_type") === "search" && (col("query_id").isNull || isBlank("query_text"))),
      "missing_result_identity" ->
        (col("event_type") === "result_view" &&
          (col("query_id").isNull || col("result_set_id").isNull)),
      "missing_behavior_item" ->
        (col("event_type").isin("result_view", "detail_view", "click") && isBlank("item_id"))
    ))

  /** Select the canonical Avro event view and gate unusable business identifiers. */
  def parseEvents(raw: DataFrame): Gated =
    normalize(EventParsing.canonicalEvents(raw))

  /** Per-item click counts for one micro-batch (columns: item_id, count). */
  def itemClickCounts(batch: DataFrame): DataFrame = batch.groupBy("item_id").count()

  /** Projects one behavioral row into the sequence-store event shape.
    *
    * `timestamp_ms` is already milliseconds. rating/genres/release_year are null for every
    * behavioral action but still emitted so the chunk schema is identical across kinds.
    */
  private def sequenceRows(batch: DataFrame, kind: String, itemId: Column): DataFrame =
    batch
      .filter(col("user_id").isNotNull && col("timestamp_ms").isNotNull)
      .select(
        col("user_id"),
        lit(kind).as("kind"),
        itemId.as("item_id"),
        col("timestamp_ms").cast("long").as("ts"),
        col("event_type").as("action"),
        lit(null).cast("double").as("rating"),
        lit(null).cast("array<string>").as("genres"),
        lit(null).cast("int").as("release_year")
      )

  /** The unified behavior sequence: every gated action, in one kind, one row each.
    *
    * A search has no item, but the columnar sequence store keeps its columns aligned by
    * position, so the row carries an empty item as a sentinel. Serving omits it; the
    * canonical event itself keeps `item_id = null`. */
  def buildBehaviorSequenceEvents(batch: DataFrame): DataFrame =
    sequenceRows(batch, SequenceSchema.KindBehavior, coalesce(col("item_id"), lit("")))

  /** The legacy click-only sequence, written alongside `behavior` for the length of the
    * migration so readers that have not moved over keep seeing what they saw before. */
  def buildClickSequenceEvents(batch: DataFrame): DataFrame =
    sequenceRows(batch.filter(col("event_type") === "click" && col("item_id").isNotNull),
      SequenceSchema.KindClick, col("item_id"))

  /** Parse → behavioral gate → watermark-dedup on event_id. event_time derived from millis. */
  def behavioralEvents(
      raw: DataFrame,
      watermarkDelay: String,
      batchId: Long = -1L,
      reporter: Reporter = DropMetrics
  ): DataFrame = {
    val valid = reporter.report(parseEvents(raw), JobName, batchId)
    // timestamp_millis keeps full precision and involves no zone; the old
    // to_timestamp(from_unixtime(...)) round trip collapsed a DST fall-back hour onto one instant.
    EventParsing.dedupeWithinWatermark(valid, timestamp_millis(col("timestamp_ms")), watermarkDelay)
  }

  def businessSinks(
      redisHost: String,
      redisPort: Int,
      redisPoolMaxTotal: Int,
      sequenceConfig: SequenceJobConfig,
      redisPipelineSize: Int = 500,
      ledgerRetentionBatches: Int = 2
  ): Seq[DurableSink] = Seq(
    new RedisPopularitySink(redisHost, redisPort, redisPoolMaxTotal,
      ledgerRetentionBatches = ledgerRetentionBatches),
    new SequenceBusinessSink(
      sequenceConfig,
      redisHost,
      redisPort,
      redisPoolMaxTotal,
      redisPipelineSize,
      SequenceWriteMode.Append,
      (batch: DataFrame) => SequenceEncoder.toColumnChunks(buildBehaviorSequenceEvents(batch)),
      "sequence:user-behavior"),
    // A separate instance, so the two sequences keep independent commit ledgers and neither
    // can mark the other's batch complete. The legacy sink keeps the identity it has always
    // had: renaming it would orphan its ledger and re-append a replayed batch as duplicate
    // click history.
    new SequenceBusinessSink(
      sequenceConfig,
      redisHost,
      redisPort,
      redisPoolMaxTotal,
      redisPipelineSize,
      SequenceWriteMode.Append,
      (batch: DataFrame) => SequenceEncoder.toColumnChunks(buildClickSequenceEvents(batch)),
      "sequence:user-event"))

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaTopic            = sys.env.getOrElse("KAFKA_TOPIC", "recsys_events")
    val redisHost             = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort             = Env.int("REDIS_PORT", 6379)
    val checkpointLocation    = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/user-event-streaming-job"
    )
    val maxOffsetsPerTrigger = Env.int("MAX_OFFSETS_PER_TRIGGER", 5000)
    val triggerInterval      = sys.env.getOrElse("TRIGGER_INTERVAL", "5 seconds")
    val redisPipelineSize    = math.max(3, Env.int("REDIS_PIPELINE_SIZE", 500))
    val redisPoolMaxTotal    = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))
    val ledgerRetentionBatches = math.max(2, Env.int("REDIS_LEDGER_RETENTION_BATCHES", 2))
    val sequenceConfig = SequenceJobConfig.fromEnv()

    val watermarkDelay = sys.env.getOrElse("EVENT_WATERMARK_DELAY", "10 minutes")
    val cfg = EngineConfig(
      bootstrapServers = kafkaBootstrapServers,
      inputTopic = kafkaTopic,
      startingOffsets = "earliest",
      groupId = "training-user-history",
      maxOffsetsPerTrigger = maxOffsetsPerTrigger,
      triggerInterval = triggerInterval,
      checkpointLocation = checkpointLocation,
      watermarkDelay = watermarkDelay,
      sinkMaxRetries = Env.int("SINK_MAX_RETRIES", 0)
    )
    val archive = new RawArchiveSink(
      sys.env.getOrElse("RECSYS_EVENT_ARCHIVE_PATH", "/tmp/spark-recsys/recsys-events-archive"),
      sys.env.getOrElse("RECSYS_EVENT_DEAD_LETTER_PATH", "/tmp/spark-recsys/recsys-events-dead-letter"),
      cfg.checkpointLocation
    )
    // A batch stage rather than a streaming stage so the gate can name its batch.
    val streamingStages: Seq[Stage] = Seq.empty
    val behaviorStage: Seq[BatchStage] =
      Seq((df: DataFrame, id: Long) => behavioralEvents(df, cfg.watermarkDelay, id))
    val sinks: Seq[Sink] = businessSinks(
      redisHost, redisPort, redisPoolMaxTotal, sequenceConfig, redisPipelineSize,
      ledgerRetentionBatches)

    ExecutionEngine.run(
      spark, cfg, KafkaSource, DecodedEventBatch.decode _, archive,
      streamingStages, behaviorStage, sinks)
  }
}
