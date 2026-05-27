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

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaTopic = sys.env.getOrElse("KAFKA_TOPIC", "user_events")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = Env.int("REDIS_PORT", 6379)
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/user-event-streaming-job"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "5 seconds")
    val recentItemsLimit = math.max(1, Env.int("RECENT_ITEMS_LIMIT", 20))
    val redisPipelineSize = math.max(3, Env.int("REDIS_PIPELINE_SIZE", 500))
    val recentItemsTtlSeconds = Env.int("RECENT_ITEMS_TTL_SECONDS", 7 * 24 * 3600)
    val redisPoolMaxTotal = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))

    val spark = SparkSessions.create("UserEventStreamingJob", defaultShufflePartitions = 4)

    val schema = StructType(Seq(
      StructField("user_id", StringType, nullable = false),
      StructField("item_id", StringType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    ))

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    val parsed = df.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
      .filter(
        col("user_id").isNotNull &&
          col("item_id").isNotNull &&
          col("event_type") === "click"
      )

    // Executor-local pool: one pool per JVM (i.e. per executor), shared across micro-batches.
    val poolHost = redisHost
    val poolPort = redisPort
    val poolMax = redisPoolMaxTotal

    parsed.writeStream.foreachBatch { (batch: DataFrame, _: Long) =>
      batch.foreachPartition { rows: Iterator[Row] =>
        // Single pass: collect user→items and per-item counts
        val userItems = scala.collection.mutable.Map.empty[String, scala.collection.mutable.ArrayBuffer[String]]
        val itemCounts = scala.collection.mutable.Map.empty[String, Int]

        rows.foreach { row =>
          try {
            val user = row.getAs[String]("user_id")
            val item = row.getAs[String]("item_id")
            userItems.getOrElseUpdate(user, scala.collection.mutable.ArrayBuffer.empty) += item
            itemCounts(item) = itemCounts.getOrElse(item, 0) + 1
          } catch {
            case e: Exception =>
              log.warn("Skipping malformed row: {}", e.getMessage)
          }
        }

        val pool = RedisPool.get(poolHost, poolPort, poolMax)
        val jedis = pool.getResource
        try {
          val pipeline = jedis.pipelined()
          var pendingCommands = 0

          def flushIfNeeded(): Unit =
            if (pendingCommands >= redisPipelineSize) { pipeline.sync(); pendingCommands = 0 }

          // One LPUSH (with all items) + LTRIM + EXPIRE per user instead of per event
          userItems.foreach { case (user, items) =>
            val recentKey = s"user:$user:recent"
            pipeline.lpush(recentKey, items.toSeq: _*)
            pipeline.ltrim(recentKey, 0, recentItemsLimit - 1)
            pipeline.expire(recentKey, recentItemsTtlSeconds)
            pendingCommands += 3
            flushIfNeeded()
          }

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
