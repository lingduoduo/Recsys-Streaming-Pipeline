package com.demo.report

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/** Session-level engagement report (pure-Spark port of `session_report.py`).
  *
  * Reads the `training_samples` Parquet (which carries `session_id`, threaded through by
  * [[com.demo.process.OnlineJoinerStreamingJob]]) and aggregates engagement by session:
  *   per session : slates (distinct request_id), impressions, clicks, orders, session CTR
  *   rollups     : sessions, users, sessions/user, slates/session, impressions/session,
  *                 clicks/session, mean session CTR
  *
  * Run through the project's pinned Spark:
  *   "$SPARK_HOME/bin/spark-submit" --class com.demo.report.SessionReportJob \
  *     spark-recsys-job.jar <parquet-input> [<outdir>]
  */
object SessionReportJob {

  /** Rollup metrics over the per-session frame (field order = the printed report order). */
  case class SessionSummary(
      sessions: Long,
      users: Long,
      sessionsPerUser: Double,
      slatesPerSession: Double,
      impressionsPerSession: Double,
      clicksPerSession: Double,
      meanSessionCtr: Double) {
    def orderedLines: Seq[(String, Any)] = Seq(
      "sessions" -> sessions,
      "users" -> users,
      "sessions_per_user" -> sessionsPerUser,
      "slates_per_session" -> slatesPerSession,
      "impressions_per_session" -> impressionsPerSession,
      "clicks_per_session" -> clicksPerSession,
      "mean_session_ctr" -> meanSessionCtr)
  }

  def main(args: Array[String]): Unit = {
    val input = Env.argOrEnv(args, 0, "SESSION_REPORT_INPUT_PATH")
      .getOrElse("/tmp/spark-recsys/engagement-sim/training-samples")
    val outdir = Env.argOrEnv(args, 1, "SESSION_REPORT_OUTPUT_PATH")
      .getOrElse(s"$input/../report-sessions")

    val lookbackDays = Env.int("SESSION_REPORT_LOOKBACK_DAYS", 30)

    val spark = SparkSessions.create("SessionReportJob")
    try {
      val df = ReportWindow.withinLookback(spark.read.parquet(input), lookbackDays).cache()
      val sessions = perSession(df).cache()

      println("=== session-level engagement summary ===")
      summary(sessions).orderedLines.foreach { case (k, v) => println(s"  $k: $v") }

      // No coalesce: one row per session can be large, so keep the write partitioned.
      sessions.write.mode("overwrite").option("header", "true").csv(s"$outdir/by_session")
      println(s"\nwrote per-session CSV under $outdir")
    } finally {
      spark.stop()
    }
  }

  /** One row per (session_id, user_id) with its engagement; ignores rows without a session. */
  def perSession(df: DataFrame): DataFrame =
    df.filter(col("session_id").isNotNull && col("session_id") =!= "")
      .groupBy("session_id", "user_id")
      .agg(
        countDistinct("request_id").as("slates"),
        count(lit(1)).as("impressions"),
        sum("clicked").as("clicks"),
        sum("ordered").as("orders"))
      .withColumn("ctr", round(col("clicks") / col("impressions"), 4))

  def summary(sessions: DataFrame): SessionSummary = {
    val a = sessions.agg(
      count(lit(1)).as("sessions"),
      countDistinct("user_id").as("users"),
      sum("slates").as("slates"),
      sum("impressions").as("impressions"),
      sum("clicks").as("clicks"),
      sum("orders").as("orders"),
      avg("ctr").as("mean_session_ctr")).first()

    val sess = getLong(a, "sessions")
    val users = getLong(a, "users")
    SessionSummary(
      sessions = sess,
      users = users,
      sessionsPerUser = if (users > 0) round3(sess.toDouble / users) else 0.0,
      slatesPerSession = if (sess > 0) round3(getLong(a, "slates").toDouble / sess) else 0.0,
      impressionsPerSession = if (sess > 0) round2(getLong(a, "impressions").toDouble / sess) else 0.0,
      clicksPerSession = if (sess > 0) round3(getLong(a, "clicks").toDouble / sess) else 0.0,
      meanSessionCtr = round4(getDouble(a, "mean_session_ctr")))
  }

  private def getLong(r: Row, name: String): Long =
    if (r.isNullAt(r.fieldIndex(name))) 0L else r.getAs[Number](name).longValue()

  private def getDouble(r: Row, name: String): Double =
    if (r.isNullAt(r.fieldIndex(name))) 0.0 else r.getAs[Number](name).doubleValue()

  private def round2(v: Double): Double = math.round(v * 100.0) / 100.0
  private def round3(v: Double): Double = math.round(v * 1000.0) / 1000.0
  private def round4(v: Double): Double = math.round(v * 10000.0) / 10000.0
}
