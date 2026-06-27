package com.demo.process

import com.demo.event.EventParsing
import com.demo.util.{BatchMetricsListener, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

/** Derives per-impression **recall records** from `training_samples` and emits them to the
  * `recall_samples` Kafka topic. One row per recommended movie:
  *   user_id, session_id, event_ts (time), recommended_movie_id,
  *   click_movie_id (the movie iff it was clicked, else null), rating (the click/order label).
  *
  * Consumes the OnlineJoiner output (same source as ExperienceCollector), which already joins a
  * slate's impressions + feedback per (request, user, item) and carries session_id + label.
  */
object RecallSampleStreamingJob {

  /** Reshape training samples into per-impression recall rows. */
  def buildRecallSamples(samples: DataFrame): DataFrame =
    samples.select(
      col("user_id"),
      col("session_id"),
      col("impression_ts").as("event_ts"),
      col("item_id").as("recommended_movie_id"),
      // null unless this recommended movie was the clicked one
      when(col("clicked") === 1, col("item_id")).as("click_movie_id"),
      col("label").as("rating")  // implicit label: click -> 1.0, order -> 2.0, else 0.0
    )

  def parseSamples(rawKafka: DataFrame): DataFrame =
    EventParsing.fromJson(rawKafka, ExperienceCollectorStreamingJob.TrainingSampleSchema)
      .filter(col("user_id").isNotNull && col("item_id").isNotNull)

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("RECALL_INPUT_TOPIC", "training_samples")
    val outputTopic = sys.env.getOrElse("RECALL_OUTPUT_TOPIC", "recall_samples")
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/recall-samples"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")

    val spark: SparkSession = SparkSessions.create("RecallSampleStreamingJob")
    BatchMetricsListener.register(spark)

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "earliest"))
      .option("kafka.group.id", sys.env.getOrElse("KAFKA_GROUP_ID", "recall-samples"))
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    buildRecallSamples(parseSamples(raw)).writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch
          .select(
            concat_ws(":", col("user_id"), coalesce(col("session_id"), lit("")),
              col("recommended_movie_id")).as("key"),
            to_json(struct(batch.columns.map(col): _*)).as("value")
          )
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaBootstrapServers)
          .option("topic", outputTopic)
          .save()
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }
}
