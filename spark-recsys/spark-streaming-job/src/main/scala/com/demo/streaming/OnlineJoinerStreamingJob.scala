package com.demo.streaming

import com.demo.common.SparkSessions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object OnlineJoinerStreamingJob {

  val EventSchema: StructType = StructType(Seq(
    StructField("request_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("item_id", StringType, nullable = false),
    StructField("event_type", StringType, nullable = false),
    StructField("timestamp", LongType, nullable = false),
    StructField("position", IntegerType, nullable = true),
    StructField("user_features", MapType(StringType, StringType), nullable = true),
    StructField("item_features", MapType(StringType, StringType), nullable = true),
    StructField("context_features", MapType(StringType, StringType), nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("ONLINE_JOINER_INPUT_TOPIC", "behavior_logs")
    val outputTopic = sys.env.getOrElse("ONLINE_JOINER_OUTPUT_TOPIC", "training_samples")
    val outputPath = sys.env.getOrElse("ONLINE_JOINER_HDFS_OUTPUT_PATH", "/tmp/spark-recsys/training-samples")
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/online-joiner"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")

    val spark = SparkSessions.create("OnlineJoinerStreamingJob")

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    raw.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        // Persist parsed events so buildTrainingSamples doesn't scan the Kafka
        // batch twice (once for impressions, once for feedback groupBy).
        val events = parseEvents(batch).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val samples = buildTrainingSamples(events)
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

          samples
            .withColumn("date", to_date(col("impression_time")))
            .write
            .mode("append")
            .partitionBy("date")
            .format("parquet")
            .save(outputPath)
        } finally {
          samples.unpersist()
          events.unpersist()
        }
        ()
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }

  def parseEvents(rawKafka: DataFrame): DataFrame =
    rawKafka.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), EventSchema).as("data"))
      .select("data.*")
      .filter(
        col("request_id").isNotNull &&
          col("user_id").isNotNull &&
          col("item_id").isNotNull &&
          col("event_type").isNotNull &&
          col("timestamp").isNotNull
      )

  def buildTrainingSamples(events: DataFrame): DataFrame = {
    val normalized = events
      .withColumn("event_type_normalized", lower(trim(col("event_type"))))
      .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))

    val impressions = normalized
      .filter(col("event_type_normalized").isin("impression", "exposure"))
      .select(
        col("request_id"),
        col("user_id"),
        col("item_id"),
        coalesce(col("position"), lit(0)).as("position"),
        col("timestamp").as("impression_ts"),
        col("event_time").as("impression_time"),
        coalesce(col("user_features"), typedLit(Map.empty[String, String])).as("user_features"),
        coalesce(col("item_features"), typedLit(Map.empty[String, String])).as("item_features"),
        coalesce(col("context_features"), typedLit(Map.empty[String, String])).as("context_features")
      )

    val feedback = normalized
      .filter(col("event_type_normalized").isin("click", "order", "purchase"))
      .groupBy("request_id", "user_id", "item_id")
      .agg(
        max(when(col("event_type_normalized") === "click", 1).otherwise(0)).as("clicked"),
        max(when(col("event_type_normalized").isin("order", "purchase"), 1).otherwise(0)).as("ordered"),
        max(col("timestamp")).as("last_feedback_ts")
      )

    impressions
      .join(feedback, Seq("request_id", "user_id", "item_id"), "left")
      .select(
        concat_ws(":", col("request_id"), col("user_id"), col("item_id")).as("sample_id"),
        col("request_id"),
        col("user_id"),
        col("item_id"),
        col("position"),
        col("impression_ts"),
        col("impression_time"),
        coalesce(col("clicked"), lit(0)).as("clicked"),
        coalesce(col("ordered"), lit(0)).as("ordered"),
        when(coalesce(col("ordered"), lit(0)) === 1, lit(2.0))
          .when(coalesce(col("clicked"), lit(0)) === 1, lit(1.0))
          .otherwise(lit(0.0)).as("label"),
        col("last_feedback_ts"),
        col("user_features"),
        col("item_features"),
        col("context_features")
      )
  }
}
