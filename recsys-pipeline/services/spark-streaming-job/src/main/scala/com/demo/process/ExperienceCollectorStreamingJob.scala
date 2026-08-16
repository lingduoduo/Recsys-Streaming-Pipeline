package com.demo.process

import com.demo.engine.ParquetSink
import com.demo.event.{EventParsing, FieldGate, Gated}
import com.demo.util.{BatchMetricsListener, DropMetrics, SparkSessions, TimePartitions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object ExperienceCollectorStreamingJob {

  private val JobName = "ExperienceCollectorStreamingJob"

  private val MeasurementFields: Seq[(String, DataType)] = Seq(
    "last_feedback_ts" -> LongType,
    "feedback_delay_ms" -> LongType,
    "model_version" -> StringType,
    "policy_version" -> StringType,
    "algorithm_version" -> StringType,
    "rating" -> DoubleType,
    "negative_feedback_reason" -> StringType,
    "dwell_millis" -> LongType,
    "completion_rate" -> DoubleType,
    "published_at" -> LongType,
    "new_release" -> BooleanType,
    "filter_reason" -> StringType,
    "unsafe_label" -> BooleanType
  )

  val TrainingSampleSchema: StructType = StructType(Seq(
    StructField("sample_id", StringType, nullable = false),
    StructField("session_id", StringType, nullable = true),
    StructField("request_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("item_id", StringType, nullable = false),
    StructField("position", IntegerType, nullable = false),
    StructField("impression_ts", LongType, nullable = false),
    StructField("clicked", IntegerType, nullable = false),
    StructField("ordered", IntegerType, nullable = false),
    StructField("label", DoubleType, nullable = false),
    StructField("last_feedback_ts", LongType, nullable = true),
    StructField("feedback_delay_ms", LongType, nullable = true),
    StructField("model_version", StringType, nullable = true),
    StructField("policy_version", StringType, nullable = true),
    StructField("algorithm_version", StringType, nullable = true),
    StructField("rating", DoubleType, nullable = true),
    StructField("negative_feedback_reason", StringType, nullable = true),
    StructField("dwell_millis", LongType, nullable = true),
    StructField("completion_rate", DoubleType, nullable = true),
    StructField("published_at", LongType, nullable = true),
    StructField("new_release", BooleanType, nullable = true),
    StructField("filter_reason", StringType, nullable = true),
    StructField("unsafe_label", BooleanType, nullable = true),
    StructField("user_features", MapType(StringType, StringType), nullable = true),
    StructField("item_features", MapType(StringType, StringType), nullable = true),
    StructField("context_features", MapType(StringType, StringType), nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("EXPERIENCE_COLLECTOR_INPUT_TOPIC", "training_samples")
    val outputTopic = sys.env.getOrElse("EXPERIENCE_COLLECTOR_OUTPUT_TOPIC", "training_experiences")
    val outputPath  = sys.env.getOrElse("EXPERIENCE_COLLECTOR_OUTPUT_PATH", "")
    val outputFiles = sys.env.get("EXPERIENCE_COLLECTOR_OUTPUT_FILES").map(_.toInt).getOrElse(1)
    val slateSink   = parquetSink(outputPath, outputFiles)
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/experience-collector"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")

    val spark = SparkSessions.create("ExperienceCollectorStreamingJob")
    BatchMetricsListener.register(spark)

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
        val slates = buildSlates(DropMetrics.report(parseSamples(batch), JobName, batchId))
          .withColumn("batch_id", lit(batchId))
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        try {
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

          slateSink.foreach(_.write(slates, batchId))
        } finally {
          slates.unpersist()
        }
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }

  /** Optional Parquet sink for slate experiences, mirroring the joiner's training-sample
    * sink. Returns None when no path is configured, leaving the Kafka path untouched. */
  def parquetSink(outputPath: String, outputFiles: Int): Option[ParquetSink] =
    if (outputPath.isEmpty) None
    else Some(new ParquetSink(outputPath, "date", math.max(1, outputFiles), withDatePartition))

  /** Stamp the Parquet partition key from `request_ts` (epoch seconds).
    *
    * Epoch-derived rather than `to_date(from_unixtime(...))`, so the partition does not move with
    * the session time zone. Mirrors the joiner's training-sample sink, which this dataset is
    * cross-referenced against by date. */
  def withDatePartition(slates: DataFrame): DataFrame =
    slates.withColumn("date", TimePartitions.utcDate(col("request_ts")))

  def parseSamples(rawKafka: DataFrame): Gated =
    FieldGate(EventParsing.fromJson(rawKafka, TrainingSampleSchema), Seq(
      "null_request_id" -> col("request_id").isNull,
      "null_user_id" -> col("user_id").isNull,
      "null_item_id" -> col("item_id").isNull
    ))

  def buildSlates(samples: DataFrame): DataFrame =
    MeasurementFields.foldLeft(samples) { case (df, (name, dataType)) =>
      if (df.columns.contains(name)) df else df.withColumn(name, lit(null).cast(dataType))
    }
      .groupBy("request_id", "user_id")
      .agg(
        min(col("impression_ts")).as("request_ts"),
        first(col("session_id"), ignoreNulls = true).as("session_id"),  // one session per slate
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
          col("last_feedback_ts"),
          col("feedback_delay_ms"),
          col("model_version"),
          col("policy_version"),
          col("algorithm_version"),
          col("rating"),
          col("negative_feedback_reason"),
          col("dwell_millis"),
          col("completion_rate"),
          col("published_at"),
          col("new_release"),
          col("filter_reason"),
          col("unsafe_label"),
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
        col("session_id"),
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
