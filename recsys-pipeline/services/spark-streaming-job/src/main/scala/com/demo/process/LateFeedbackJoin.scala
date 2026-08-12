package com.demo.process

import com.demo.engine.CommitProtocol
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.storage.StorageLevel

/** Joins feedback to its impression across micro-batch boundaries.
  *
  * A slate's raw events are held in a durable per-batch snapshot until its feedback window closes,
  * then handed to `buildTrainingSamples` unchanged. Holding raw events — rather than merged
  * partial aggregates — means the aggregation, including `max_by` last-feedback-wins and its
  * `(timestamp, event_id)` tiebreak, is reused exactly as it is already tested.
  *
  * `queryNamespace` must be the archive's own namespace so the store shares its query root, and
  * must not be shared by concurrently active queries.
  */
class LateFeedbackJoin(archiveRoot: String, queryNamespace: String, feedbackJoinWait: String) {

  require(Option(queryNamespace).exists(_.trim.nonEmpty), "queryNamespace must not be blank")

  private val waitSeconds: Long = LateFeedbackJoin.waitSeconds(feedbackJoinWait)

  private val pendingRoot: Path =
    new Path(new Path(new Path(archiveRoot), s"_queries/$queryNamespace"), "_pending")

  private val slateKeys: Seq[String] = Seq("request_id", "user_id", "item_id")

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  @volatile private var lastOrphanSlates: Long = 0L
  @volatile private var lastOrphanEvents: Long = 0L

  /** The most recent batch's (slates, events) orphan counts. Exposed for tests and diagnostics. */
  def orphanCounts: (Long, Long) = (lastOrphanSlates, lastOrphanEvents)

  /** Publish the slates whose window has closed; hold the rest for a later batch.
    *
    * `nowMs` is the caller's wall clock, passed in so the close rule stays testable.
    */
  def process(events: DataFrame, batchId: Long, nowMs: Long): DataFrame = {
    val prepared = normalize(events, nowMs)
    val all = prepared.unionByName(readPending(prepared, batchId))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      val open = commitOpenSlates(all, batchId, nowMs)
      // localCheckpoint materializes the due rows and cuts their lineage, so the samples this
      // returns carry no dependency on a snapshot directory that compaction may later delete.
      // Its blocks are released when the RDD is garbage collected; losing them costs a batch
      // retry, which recomputes from the committed snapshot anyway.
      val due = all.join(open.select(slateKeys.map(col): _*).distinct(), slateKeys, "left_anti")
        .drop("first_seen_ms")
        .localCheckpoint(eager = true)
      countOrphans(due, batchId)
      compactOlderSnapshots(all, batchId)
      OnlineJoinerStreamingJob.buildTrainingSamples(due)
    } finally all.unpersist()
  }

  private def countOrphans(due: DataFrame, batchId: Long): Unit = {
    val orphanKeys = due.groupBy(slateKeys.map(col): _*)
      .agg(max(when(LateFeedbackJoin.isImpression, lit(1)).otherwise(lit(0))).as("has_impression"))
      .filter(col("has_impression") === 0)
      .select(slateKeys.map(col): _*)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      lastOrphanSlates = orphanKeys.count()
      // Only pay for the second count when there is something to report.
      lastOrphanEvents =
        if (lastOrphanSlates == 0L) 0L
        else due.join(orphanKeys, slateKeys, "left_semi").count()
      if (lastOrphanSlates > 0L)
        log.info(LateFeedbackJoin.formatOrphans(batchId, lastOrphanSlates, lastOrphanEvents))
    } finally orphanKeys.unpersist()
  }

  /** Project to the stable snapshot schema and stamp arrival time on rows entering the store. */
  private def normalize(events: DataFrame, nowMs: Long): DataFrame = {
    val withEventId =
      if (events.columns.contains("event_id")) events
      else events.withColumn("event_id", lit(null).cast(StringType))
    val complete = OnlineJoinerStreamingJob.MeasurementFields.foldLeft(withEventId) {
      case (df, (name, dataType)) =>
        if (df.columns.contains(name)) df else df.withColumn(name, lit(null).cast(dataType))
    }
    complete
      .select(LateFeedbackJoin.SnapshotColumns.map(col): _*)
      .withColumn("first_seen_ms", lit(nowMs).cast(LongType))
  }

  /** Commit this batch's snapshot of still-open slates, or reuse the one a prior attempt wrote. */
  private def commitOpenSlates(all: DataFrame, batchId: Long, nowMs: Long): DataFrame = {
    val path = new Path(pendingRoot, batchId.toString)
    val fileSystem = path.getFileSystem(all.sparkSession.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(path)) {
      val slates = all.groupBy(slateKeys.map(col): _*).agg(
        max(when(LateFeedbackJoin.isImpression, col("timestamp"))).as("impression_ts"),
        min(col("timestamp")).as("first_event_ts"),
        min(col("first_seen_ms")).as("slate_first_seen_ms"))
      val latest = all.agg(max(col("timestamp"))).first()
      val deadline = coalesce(col("impression_ts"), col("first_event_ts")) + lit(waitSeconds)
      val eventTimeDue = if (latest.isNullAt(0)) lit(false) else lit(latest.getLong(0)) >= deadline
      val wallClockDue = lit(nowMs) - col("slate_first_seen_ms") >= lit(waitSeconds * 1000L)
      val openKeys = slates
        .filter(not(coalesce(eventTimeDue || wallClockDue, lit(false))))
        .select(slateKeys.map(col): _*)
      writeSnapshot(all.join(openKeys, slateKeys, "left_semi"), batchId)
    }
    readSnapshot(all, path, batchId)
  }

  private def writeSnapshot(open: DataFrame, batchId: Long): Unit =
    CommitProtocol.writeDirectory(
      open,
      new Path(pendingRoot, s"_attempts/$batchId"),
      new Path(pendingRoot, batchId.toString),
      partitionByDate = false,
      s"pending slate snapshot $batchId",
      manifest(batchId))

  /** Read the snapshot written by the previous batch; empty before the first one exists. */
  private def readPending(template: DataFrame, batchId: Long): DataFrame = {
    if (batchId <= 0L) return template.limit(0)
    val path = new Path(pendingRoot, (batchId - 1L).toString)
    val fileSystem = path.getFileSystem(template.sparkSession.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(path)) template.limit(0) else readSnapshot(template, path, batchId - 1L)
  }

  private def readSnapshot(template: DataFrame, path: Path, batchId: Long): DataFrame = {
    val fileSystem = path.getFileSystem(template.sparkSession.sparkContext.hadoopConfiguration)
    CommitProtocol.validateDataCommitted(fileSystem, path, manifest(batchId), None)
    if (!CommitProtocol.hasParquetData(fileSystem, path)) template.limit(0)
    else template.sparkSession.read.parquet(path.toString)
      .select(template.columns.map(col): _*)
  }

  /** Batch N needs only N-1 and N: a restart replays N and rereads N-1. */
  private def compactOlderSnapshots(df: DataFrame, batchId: Long): Unit =
    try {
      val fileSystem = pendingRoot.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
      if (fileSystem.exists(pendingRoot))
        fileSystem.listStatus(pendingRoot).iterator
          .filter(_.isDirectory)
          .filter(status => status.getPath.getName.forall(_.isDigit))
          .filter(_.getPath.getName.toLong < batchId - 1L)
          .foreach(status => fileSystem.delete(status.getPath, true))
    } catch {
      case scala.util.control.NonFatal(_) => ()
    }

  private def manifest(batchId: Long): String =
    s"version=2\nquery=$queryNamespace\nkind=pending\nbatch_id=$batchId\n"
}

object LateFeedbackJoin {

  /** The columns a snapshot carries: everything `buildTrainingSamples` reads, in a fixed order so
    * a snapshot written by one batch unions cleanly with the next. */
  val SnapshotColumns: Seq[String] =
    Seq("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position",
      "event_id", "user_features", "item_features", "context_features") ++
      OnlineJoinerStreamingJob.MeasurementFields.map(_._1)

  def isImpression: org.apache.spark.sql.Column =
    lower(trim(col("event_type"))).isin("impression", "exposure")

  def formatOrphans(batchId: Long, slates: Long, events: Long): String =
    s"[late-feedback] batch=$batchId orphan_slates=$slates orphan_events=$events"

  /** Parse the wait using the same interval syntax as EVENT_WATERMARK_DELAY. */
  def waitSeconds(interval: String): Long = {
    val parsed = IntervalUtils.fromIntervalString(interval)
    require(parsed.months == 0,
      s"FEEDBACK_JOIN_WAIT must not use months, whose length is ambiguous: $interval")
    val seconds = parsed.days.toLong * 86400L + parsed.microseconds / 1000000L
    require(seconds >= 0L, s"FEEDBACK_JOIN_WAIT must not be negative: $interval")
    seconds
  }
}
