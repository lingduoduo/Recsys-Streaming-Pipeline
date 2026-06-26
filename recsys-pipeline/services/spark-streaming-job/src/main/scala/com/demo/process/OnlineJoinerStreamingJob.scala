package com.demo.process

import com.demo.event.{EventParsing, EventSchemas}
import com.demo.util.SparkSessions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
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

    val spark = SparkSessions.create("OnlineJoinerStreamingJob")

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "earliest"))
      .option("kafka.group.id", sys.env.getOrElse("KAFKA_GROUP_ID", "training-joiner"))
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    raw.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        // buildTrainingSamples now does a single-pass groupBy (one shuffle) instead of
        // filter+groupBy+join (two shuffles), so events no longer needs to be persisted.
        val samples = buildTrainingSamples(parseEvents(batch))
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
        }
        ()
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }

  def parseEvents(rawKafka: DataFrame): DataFrame =
    EventParsing.fromJson(rawKafka, EventSchemas.joiner)
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
