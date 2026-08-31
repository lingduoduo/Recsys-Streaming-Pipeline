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

  /** Whether event logging is enabled, and if so, which directory to use.
    *
    * Pure, and deliberately split out of `create`: `create` calls `getOrCreate`, so a test that
    * builds a session and asserts on its config is unreliable -- it may get back whatever session
    * another suite already built in this JVM. Isolating the enabled/directory decision here is
    * what makes it testable at all.
    *
    * The env var is matched case-insensitively and trimmed: the only moment anyone sets
    * `SPARK_EVENT_LOG_ENABLED` is mid-incident while diagnosing a failure, and a diagnostic switch
    * that silently no-ops on `TRUE` or `" true "` is worse than useless then.
    */
  def eventLogSettings(env: Map[String, String]): Option[String] =
    if (env.get("SPARK_EVENT_LOG_ENABLED").exists(_.trim.equalsIgnoreCase("true")))
      Some(env.getOrElse("SPARK_EVENT_LOG_DIR", "/tmp/spark-events"))
    else
      None

  /** Make sure Spark's event-log directory exists, returning it only if it is usable.
    *
    * Spark throws at session creation when `spark.eventLog.dir` is missing -- it does not create
    * the directory. Propagating that would mean enabling event logging on a fresh machine breaks
    * every job at startup, turning an observability feature into an outage. So a directory that
    * cannot be created disables event logging with a warning instead.
    *
    * A `dir` carrying a URI authority (`hdfs://...`, `s3a://...`) names a location Spark resolves
    * itself, not a local path -- creating it here would leave a junk directory literally named
    * e.g. `hdfs:` in the working directory and still report success. Those are passed through
    * untouched and left for Spark to resolve.
    *
    * Detected by the `://` separator, not by `java.net.URI(dir).getScheme`: URI parsing reads any
    * leading `word:` as a scheme, so it misreads an ordinary local path with a colon in it --
    * `foo:bar`, or a Windows drive letter like `C:/foo` -- as remote, skipping directory creation
    * for a path that was never remote and turning this back into the startup crash it exists to
    * prevent. `://` is the actual authority separator and cannot fire on either of those.
    */
  def ensureEventLogDir(dir: String): Option[String] = {
    if (dir.contains("://")) Some(dir)
    else
      try {
        Files.createDirectories(Paths.get(dir))
        Some(dir)
      } catch {
        case scala.util.control.NonFatal(e) =>
          log.warn(s"event log dir '$dir' is unusable, event logging stays off: ${e.getMessage}")
          None
      }
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

    eventLogSettings(sys.env).foreach { dir =>
      ensureEventLogDir(dir).foreach { resolved =>
        builder.config("spark.eventLog.enabled", "true")
        builder.config("spark.eventLog.dir", resolved)
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
