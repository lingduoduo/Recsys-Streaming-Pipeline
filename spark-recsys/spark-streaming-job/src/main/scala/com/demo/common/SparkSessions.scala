package com.demo.common

import org.apache.spark.sql.SparkSession

object SparkSessions {
  def create(defaultAppName: String, defaultShufflePartitions: Int = 8): SparkSession =
    SparkSession.builder()
      .appName(sys.env.getOrElse("SPARK_APP_NAME", defaultAppName))
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config(
        "spark.sql.shuffle.partitions",
        sys.env.getOrElse("SPARK_SQL_SHUFFLE_PARTITIONS", defaultShufflePartitions.toString)
      )
      .getOrCreate()
}
