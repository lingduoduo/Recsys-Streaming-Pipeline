package com.demo.task

import com.demo.engine.RedisPool
import com.demo.event.{EventParsing, EventSchemas}
import com.demo.util.{BatchMetricsListener, Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

object UserEventStreamingJob {
  private val log = LoggerFactory.getLogger(getClass)

  // Lazy so tests that import spark.implicits don't pay the full SparkSessions.create cost
  // unless they actually need the production session; in tests SparkTestSupport wins.
  lazy val spark: SparkSession =
    SparkSessions.create("UserEventStreamingJob", defaultShufflePartitions = 4)

  // the coalesce + null-filter tail of the old parseEvents
  def normalize(df: DataFrame): DataFrame =
    df.withColumn("timestamp_ms", coalesce(col("timestamp_ms"), col("timestamp") * 1000L))
      .filter(col("user_id").isNotNull && col("item_id").isNotNull)

  /**
   * Parse a DataFrame that has a string "value" column (raw Kafka JSON payloads)
   * into the unified event schema.  Supports both:
   *   - new schema: timestamp_ms (millis)
   *   - legacy schema: timestamp (seconds) — normalised to millis via timestamp * 1000
   *
   * Filters out rows where user_id or item_id is null.
   */
  def parseEvents(raw: DataFrame): DataFrame =
    normalize(EventParsing.fromJson(raw, EventSchemas.userEvent))

  /** Parse → watermark-dedup on event_id → keep clicks. event_time derived from millis. */
  def dedupedClicks(raw: DataFrame, watermarkDelay: String): DataFrame = {
    val parsedAll = EventParsing.observeIngest(EventParsing.fromJson(raw, EventSchemas.userEvent))
    val valid = normalize(parsedAll)
    EventParsing.dedupeWithinWatermark(valid, to_timestamp(from_unixtime(col("timestamp_ms") / 1000)), watermarkDelay)
      .filter(col("event_type") === "click")
  }

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaTopic            = sys.env.getOrElse("KAFKA_TOPIC", "recsys_events")
    val redisHost             = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort             = Env.int("REDIS_PORT", 6379)
    val checkpointLocation    = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/user-event-streaming-job"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval      = sys.env.getOrElse("TRIGGER_INTERVAL", "5 seconds")
    val redisPipelineSize    = math.max(3, Env.int("REDIS_PIPELINE_SIZE", 500))
    val redisPoolMaxTotal    = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))

    BatchMetricsListener.register(spark)

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("kafka.group.id", "training-user-history")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()                       // raw `value` (binary) — fromJson casts it

    // Only keep click events for item popularity counting, with watermarked dedup.
    val watermarkDelay = sys.env.getOrElse("EVENT_WATERMARK_DELAY", "10 minutes")
    val parsed = dedupedClicks(df, watermarkDelay)

    // Executor-local pool: one pool per JVM (i.e. per executor), shared across micro-batches.
    val poolHost = redisHost
    val poolPort = redisPort
    val poolMax  = redisPoolMaxTotal

    parsed.writeStream.foreachBatch { (batch: DataFrame, _: Long) =>
      batch.foreachPartition { rows: Iterator[Row] =>
        // Aggregate per-item counts in a single pass
        val itemCounts = scala.collection.mutable.Map.empty[String, Int]

        rows.foreach { row =>
          try {
            val item = row.getAs[String]("item_id")
            itemCounts(item) = itemCounts.getOrElse(item, 0) + 1
          } catch {
            case e: Exception =>
              log.warn("Skipping malformed row: {}", e.getMessage)
          }
        }

        val pool  = RedisPool.get(poolHost, poolPort, poolMax)
        val jedis = pool.getResource
        try {
          val pipeline       = jedis.pipelined()
          var pendingCommands = 0

          def flushIfNeeded(): Unit =
            if (pendingCommands >= redisPipelineSize) { pipeline.sync(); pendingCommands = 0 }

          // One ZINCRBY per unique item using aggregated count
          itemCounts.foreach { case (item, count) =>
            pipeline.zincrby("global:item_popularity", count.toDouble, item)
            pendingCommands += 1
            flushIfNeeded()
          }

          if (pendingCommands > 0) pipeline.sync()
        } finally {
          jedis.close()
        }
      }
    }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }
}
