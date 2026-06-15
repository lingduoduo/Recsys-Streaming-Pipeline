package com.demo.task

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

// One JedisPool per executor JVM — avoids a new TCP connection per partition per micro-batch.
private[demo] object RedisPool {
  @volatile private var pool: JedisPool = _

  def get(host: String, port: Int, maxTotal: Int): JedisPool = {
    if (pool == null) synchronized {
      if (pool == null) {
        val cfg = new JedisPoolConfig()
        cfg.setMaxTotal(maxTotal)
        cfg.setMaxIdle(maxTotal)
        cfg.setMinIdle(1)
        pool = new JedisPool(cfg, host, port)
      }
    }
    pool
  }
}

object UserEventStreamingJob {
  private val log = LoggerFactory.getLogger(getClass)

  // Unified schema: timestamp_ms (millis) is primary; timestamp (seconds) is legacy compat.
  private val schema = StructType(Seq(
    StructField("event_id",    StringType, nullable = true),
    StructField("user_id",     StringType, nullable = false),
    StructField("item_id",     StringType, nullable = false),
    StructField("event_type",  StringType, nullable = false),
    StructField("timestamp_ms", LongType,  nullable = true),
    StructField("timestamp",   LongType,   nullable = true)
  ))

  // Lazy so tests that import spark.implicits don't pay the full SparkSessions.create cost
  // unless they actually need the production session; in tests SparkTestSupport wins.
  lazy val spark: SparkSession =
    SparkSessions.create("UserEventStreamingJob", defaultShufflePartitions = 4)

  /**
   * Parse a DataFrame that has a string "value" column (raw Kafka JSON payloads)
   * into the unified event schema.  Supports both:
   *   - new schema: timestamp_ms (millis)
   *   - legacy schema: timestamp (seconds) — normalised to millis via timestamp * 1000
   *
   * Filters out rows where user_id or item_id is null.
   */
  def parseEvents(raw: DataFrame): DataFrame = {
    raw
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")
      .withColumn(
        "timestamp_ms",
        coalesce(col("timestamp_ms"), col("timestamp") * 1000L)
      )
      .filter(col("user_id").isNotNull && col("item_id").isNotNull)
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

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("kafka.group.id", "training-user-history")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .selectExpr("CAST(value AS STRING) as value")

    // Only keep click events for item popularity counting.
    val parsed = parseEvents(df).filter(col("event_type") === "click")

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
