package com.demo.task

import com.demo.engine.{BatchStage, DurableSink, EngineConfig, ExecutionEngine, KafkaSource, RawArchiveSink, RedisPopularitySink, Sink, Stage}
import com.demo.event.{DecodedEventBatch, EventParsing, FieldGate, Gated}
import com.demo.sequence.{SequenceBusinessSink, SequenceEncoder, SequenceJobConfig, SequenceSchema, SequenceWriteMode}
import com.demo.util.{DropMetrics, Env, Reporter, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UserEventStreamingJob {

  private val JobName = "UserEventStreamingJob"

  // Lazy so tests that import spark.implicits don't pay the full SparkSessions.create cost
  // unless they actually need the production session; in tests SparkTestSupport wins.
  lazy val spark: SparkSession =
    SparkSessions.create("UserEventStreamingJob", defaultShufflePartitions = 4)

  // Decoding guarantees timestamp_ms; business filtering remains local to this consumer.
  def normalize(df: DataFrame): Gated =
    FieldGate(df, Seq(
      "null_user_id" -> col("user_id").isNull,
      "null_item_id" -> col("item_id").isNull
    ))

  /** Select the canonical Avro event view and gate unusable business identifiers. */
  def parseEvents(raw: DataFrame): Gated =
    normalize(EventParsing.canonicalEvents(raw))

  /** Per-item click counts for one micro-batch (columns: item_id, count). */
  def itemClickCounts(batch: DataFrame): DataFrame = batch.groupBy("item_id").count()

  /** Projects click events into the sequence-store event shape. `timestamp_ms` is already
    * milliseconds. rating/genres/release_year are null for clicks but still emitted so the
    * chunk schema is identical across kinds. */
  def buildSequenceEvents(batch: DataFrame): DataFrame =
    batch
      .filter(col("user_id").isNotNull && col("item_id").isNotNull && col("timestamp_ms").isNotNull)
      .select(
        col("user_id"),
        lit(SequenceSchema.KindClick).as("kind"),
        col("item_id"),
        col("timestamp_ms").cast("long").as("ts"),
        lit("click").as("action"),
        lit(null).cast("double").as("rating"),
        lit(null).cast("array<string>").as("genres"),
        lit(null).cast("int").as("release_year")
      )

  /** Parse → watermark-dedup on event_id → keep clicks. event_time derived from millis. */
  def dedupedClicks(
      raw: DataFrame,
      watermarkDelay: String,
      batchId: Long = -1L,
      reporter: Reporter = DropMetrics
  ): DataFrame = {
    val valid = reporter.report(parseEvents(raw), JobName, batchId)
    // timestamp_millis keeps full precision and involves no zone; the old
    // to_timestamp(from_unixtime(...)) round trip collapsed a DST fall-back hour onto one instant.
    EventParsing.dedupeWithinWatermark(valid, timestamp_millis(col("timestamp_ms")), watermarkDelay)
      .filter(col("event_type") === "click")
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
      (batch: DataFrame) => SequenceEncoder.toColumnChunks(buildSequenceEvents(batch)),
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
    val clickStage: Seq[BatchStage] =
      Seq((df: DataFrame, id: Long) => dedupedClicks(df, cfg.watermarkDelay, id))
    val sinks: Seq[Sink] = businessSinks(
      redisHost, redisPort, redisPoolMaxTotal, sequenceConfig, redisPipelineSize,
      ledgerRetentionBatches)

    ExecutionEngine.run(
      spark, cfg, KafkaSource, DecodedEventBatch.decode _, archive,
      streamingStages, clickStage, sinks)
  }
}
