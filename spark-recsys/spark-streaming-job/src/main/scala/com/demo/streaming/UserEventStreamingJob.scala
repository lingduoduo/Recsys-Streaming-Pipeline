package com.demo.streaming

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis

object UserEventStreamingJob {

  def main(args: Array[String]): Unit = {
    val appName = sys.env.getOrElse("SPARK_APP_NAME", "UserEventStreamingJob")
    val master = sys.env.getOrElse("SPARK_MASTER", "local[*]")
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaTopic = sys.env.getOrElse("KAFKA_TOPIC", "user_events")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = sys.env.get("REDIS_PORT").flatMap(toIntOption).getOrElse(6379)
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/user-event-streaming-job"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "5 seconds")
    val recentItemsLimit = math.max(1, sys.env.get("RECENT_ITEMS_LIMIT").flatMap(toIntOption).getOrElse(20))
    val redisPipelineSize = math.max(3, sys.env.get("REDIS_PIPELINE_SIZE").flatMap(toIntOption).getOrElse(500))

    val spark = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.shuffle.partitions", sys.env.getOrElse("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
      .getOrCreate()

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
      .option("startingOffsets", "latest")
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
            case _: Exception => // skip malformed rows
          }
        }

        val jedis = new Jedis(redisHost, redisPort)
        val pipeline = jedis.pipelined()
        var pendingCommands = 0

        def flushIfNeeded(): Unit =
          if (pendingCommands >= redisPipelineSize) { pipeline.sync(); pendingCommands = 0 }

        try {
          // One LPUSH (with all items) + one LTRIM per user instead of per event
          userItems.foreach { case (user, items) =>
            val recentKey = s"user:$user:recent"
            pipeline.lpush(recentKey, items.toSeq: _*)
            pipeline.ltrim(recentKey, 0, recentItemsLimit - 1)
            pendingCommands += 2
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

  private def toIntOption(value: String): Option[Int] =
    try {
      Some(value.toInt)
    } catch {
      case _: NumberFormatException => None
    }
}
