package com.demo.util

import org.apache.spark.sql.SparkSession

object SparkSessions {

  /** AQE settings applied to every session; env-overridable per key. */
  val adaptiveConfigs: Map[String, String] = Map(
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.adaptive.coalescePartitions.enabled" -> "true"
  )

  def create(defaultAppName: String, defaultShufflePartitions: Int = 8): SparkSession = {
    val builder = SparkSession.builder()
      .appName(sys.env.getOrElse("SPARK_APP_NAME", defaultAppName))
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config(
        "spark.sql.shuffle.partitions",
        sys.env.getOrElse("SPARK_SQL_SHUFFLE_PARTITIONS", defaultShufflePartitions.toString)
      )
    adaptiveConfigs.foreach { case (k, v) => builder.config(k, sys.env.getOrElse(envKeyFor(k), v)) }
    builder.getOrCreate()
  }

  // spark.sql.adaptive.enabled -> SPARK_SQL_ADAPTIVE_ENABLED
  private def envKeyFor(confKey: String): String =
    confKey.toUpperCase.replace('.', '_')
}
