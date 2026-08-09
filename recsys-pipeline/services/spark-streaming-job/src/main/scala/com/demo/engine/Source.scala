package com.demo.engine

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Source {
  def read(spark: SparkSession, cfg: EngineConfig): DataFrame
}

object KafkaSource extends Source {
  def read(spark: SparkSession, cfg: EngineConfig): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrapServers)
      .option("subscribe", cfg.inputTopic)
      .option("startingOffsets", cfg.startingOffsets)
      .option("kafka.group.id", cfg.groupId)
      .option("includeHeaders", "true")
      .option("failOnDataLoss", "true")
      .option("maxOffsetsPerTrigger", cfg.maxOffsetsPerTrigger.toString)
      .load()
}
