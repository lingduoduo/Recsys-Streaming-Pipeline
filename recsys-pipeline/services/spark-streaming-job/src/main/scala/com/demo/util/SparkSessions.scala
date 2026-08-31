package com.demo.util

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}

object SparkSessions {

  private val log = LoggerFactory.getLogger(getClass)

  /** AQE settings applied to every session; env-overridable per key. */
  val adaptiveConfigs: Map[String, String] = Map(
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.adaptive.coalescePartitions.enabled" -> "true"
  )

  /** Session time zone applied to every session; override with `SPARK_SQL_SESSION_TIMEZONE`.
    *
    * Defence in depth only, so timestamp formatting does not vary by deploy host. The date
    * projections in [[TimePartitions]] deliberately do not rely on it — a `spark-submit` override
    * or a bare `SparkSession.builder` in a test would silently undo it. */
  val defaultTimeZone: String = "UTC"

  /** Shuffle partitions for a non-local master. */
  val ClusterShufflePartitions: Int = 200

  /** Partition count chosen from the master, not from a constant.
    *
    * The old flat default of 8 is right for `local[*]` -- the default master, and the only thing
    * that executes in this repo today -- and wrong for a cluster, where every wide shuffle would
    * funnel through 8 partitions and spill. Raising it flat would instead slow every local run and
    * every test. An unrecognised master is treated as a cluster: guessing "local" wrongly causes
    * the spill this exists to prevent, while guessing "cluster" wrongly costs a laptop some
    * scheduling overhead.
    */
  def shufflePartitionsFor(master: String, localDefault: Int): Int =
    if (master == "local" || master.startsWith("local[")) localDefault
    else ClusterShufflePartitions

  /** Make sure Spark's event-log directory exists, returning it only if it is usable.
    *
    * Spark throws at session creation when `spark.eventLog.dir` is missing -- it does not create
    * the directory. Propagating that would mean enabling event logging on a fresh machine breaks
    * every job at startup, turning an observability feature into an outage. So a directory that
    * cannot be created disables event logging with a warning instead.
    */
  def ensureEventLogDir(dir: String): Option[String] =
    try {
      Files.createDirectories(Paths.get(dir))
      Some(dir)
    } catch {
      case scala.util.control.NonFatal(e) =>
        log.warn(s"event log dir '$dir' is unusable, event logging stays off: ${e.getMessage}")
        None
    }

  def create(defaultAppName: String, defaultShufflePartitions: Int = 8): SparkSession = {
    val appName = sys.env.getOrElse("SPARK_APP_NAME", defaultAppName)
    val master = sys.env.getOrElse("SPARK_MASTER", "local[*]")
    val builder = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config(
        "spark.sql.shuffle.partitions",
        sys.env.getOrElse(
          "SPARK_SQL_SHUFFLE_PARTITIONS",
          shufflePartitionsFor(master, defaultShufflePartitions).toString)
      )
      .config(
        "spark.sql.session.timeZone",
        sys.env.getOrElse("SPARK_SQL_SESSION_TIMEZONE", defaultTimeZone)
      )
    adaptiveConfigs.foreach { case (k, v) => builder.config(k, sys.env.getOrElse(envKeyFor(k), v)) }

    if (sys.env.get("SPARK_EVENT_LOG_ENABLED").contains("true")) {
      ensureEventLogDir(sys.env.getOrElse("SPARK_EVENT_LOG_DIR", "/tmp/spark-events")).foreach { dir =>
        builder.config("spark.eventLog.enabled", "true")
        builder.config("spark.eventLog.dir", dir)
      }
    }

    val spark = builder.getOrCreate()
    SpillMetricsListener.register(spark, appName)
    spark
  }

  // spark.sql.adaptive.enabled -> SPARK_SQL_ADAPTIVE_ENABLED
  private def envKeyFor(confKey: String): String =
    confKey.toUpperCase.replace('.', '_')
}
