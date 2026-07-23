package com.demo.engine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, to_json}

trait Sink {
  def write(batch: DataFrame, batchId: Long): Unit
}

/** Writes each row as key=keyCol, value=JSON of ALL columns, to a Kafka topic. */
class KafkaSink(bootstrapServers: String, topic: String, keyCol: String) extends Sink {
  def payload(df: DataFrame): DataFrame =
    df.select(
      col(keyCol).as("key"),
      to_json(struct(df.columns.map(col): _*)).as("value")
    )

  def write(batch: DataFrame, batchId: Long): Unit =
    payload(batch).write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .save()
}

/** Applies a pre-write transform, then appends partitioned Parquet, bounding file count. */
class ParquetSink(path: String, partitionCol: String, outputFiles: Int,
                  transform: DataFrame => DataFrame) extends Sink {
  def write(batch: DataFrame, batchId: Long): Unit =
    transform(batch)
      .coalesce(math.max(1, outputFiles))
      .write
      .mode("append")
      .partitionBy(partitionCol)
      .format("parquet")
      .save(path)
}
