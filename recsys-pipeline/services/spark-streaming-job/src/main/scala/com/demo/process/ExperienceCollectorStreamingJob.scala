package com.demo.process

import com.demo.event.EventParsing
import com.demo.util.SparkSessions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object ExperienceCollectorStreamingJob {

  val TrainingSampleSchema: StructType = StructType(Seq(
    StructField("sample_id", StringType, nullable = false),
    StructField("request_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("item_id", StringType, nullable = false),
    StructField("position", IntegerType, nullable = false),
    StructField("impression_ts", LongType, nullable = false),
    StructField("clicked", IntegerType, nullable = false),
    StructField("ordered", IntegerType, nullable = false),
    StructField("label", DoubleType, nullable = false),
    StructField("user_features", MapType(StringType, StringType), nullable = true),
    StructField("item_features", MapType(StringType, StringType), nullable = true),
    StructField("context_features", MapType(StringType, StringType), nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("EXPERIENCE_COLLECTOR_INPUT_TOPIC", "training_samples")
    val outputTopic = sys.env.getOrElse("EXPERIENCE_COLLECTOR_OUTPUT_TOPIC", "training_experiences")
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/experience-collector"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")

    val spark = SparkSessions.create("ExperienceCollectorStreamingJob")

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "earliest"))
      .option("kafka.group.id", sys.env.getOrElse("KAFKA_GROUP_ID", "training-experience"))
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    raw.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val slates = buildSlates(parseSamples(batch))
          .withColumn("batch_id", lit(batchId))

        slates
          .select(
            col("slate_id").as("key"),
            to_json(struct(slates.columns.map(col): _*)).as("value")
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

  def parseSamples(rawKafka: DataFrame): DataFrame =
    EventParsing.fromJson(rawKafka, TrainingSampleSchema)
      .filter(
        col("request_id").isNotNull &&
          col("user_id").isNotNull &&
          col("item_id").isNotNull
      )

  def buildSlates(samples: DataFrame): DataFrame =
    samples
      .groupBy("request_id", "user_id")
      .agg(
        min(col("impression_ts")).as("request_ts"),
        first(coalesce(col("user_features"), typedLit(Map.empty[String, String])), ignoreNulls = true).as("user_features"),
        first(coalesce(col("context_features"), typedLit(Map.empty[String, String])), ignoreNulls = true).as("context_features"),
        max(col("clicked")).as("slate_clicked"),
        max(col("ordered")).as("slate_ordered"),
        sum(col("label")).as("slate_reward"),
        array_sort(collect_list(struct(
          col("position"),
          col("item_id"),
          col("clicked"),
          col("ordered"),
          col("label"),
          coalesce(col("item_features"), typedLit(Map.empty[String, String])).as("item_features")
        )), (left, right) =>
          when(left.getField("position") < right.getField("position"), -1)
            .when(left.getField("position") > right.getField("position"), 1)
            .otherwise(0)
        ).as("items")
      )
      .select(
        concat_ws(":", col("request_id"), col("user_id")).as("slate_id"),
        col("request_id"),
        col("user_id"),
        col("request_ts"),
        col("slate_clicked"),
        col("slate_ordered"),
        col("slate_reward"),
        size(col("items")).as("slate_size"),
        col("user_features"),
        col("context_features"),
        col("items")
      )
}
