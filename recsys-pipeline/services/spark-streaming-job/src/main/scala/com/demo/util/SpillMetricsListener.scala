package com.demo.util

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

// NOTE: SparkContext.listenerBus is private[spark] and cannot be read from this package. Do not
// try to ask Spark which listeners are attached -- it will not compile.

/** One line per completed stage: what it spilled, what it shuffled, what failed, which attempt.
  *
  * Complements BatchMetricsListener rather than replacing it. That one is a
  * StreamingQueryListener, whose progress event carries no spill or shuffle-byte fields at all, and
  * which never fires for the batch report jobs -- where the most spill-prone aggregation in this
  * codebase lives.
  */
class SpillMetricsListener(jobName: String) extends SparkListener {

  private val log = LoggerFactory.getLogger(classOf[SpillMetricsListener])
  private val failures = new FailureTally

  // TaskInfo.successful rather than matching on `reason`: it is plain public API, where the
  // TaskEndReason subclasses are a DeveloperApi that has moved between Spark versions.
  override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
    if (!event.taskInfo.successful)
      failures.recordFailure(event.stageId, event.stageAttemptId)

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val info = event.stageInfo
    val metrics = info.taskMetrics
    val cost = SpillMetrics.StageCost(
      job = jobName,
      stageId = info.stageId,
      attempt = info.attemptNumber(),
      tasks = info.numTasks,
      spillMemBytes = metrics.memoryBytesSpilled,
      spillDiskBytes = metrics.diskBytesSpilled,
      shuffleWriteBytes = metrics.shuffleWriteMetrics.bytesWritten,
      shuffleReadBytes = metrics.shuffleReadMetrics.totalBytesRead,
      failedTasks = failures.drain(info.stageId, info.attemptNumber()))

    val line = SpillMetrics.format(cost)
    if (SpillMetrics.worthInfo(cost)) log.info(line) else log.debug(line)
  }
}

object SpillMetricsListener {

  // Which sessions already have a listener. Weak keys so a stopped session can still be collected
  // -- this object outlives any one session, and holding them strongly would leak every session
  // the JVM ever created. SparkContext.listenerBus is private[spark], so asking Spark what is
  // already attached is not an option from here.
  private val registered: java.util.Set[SparkSession] =
    java.util.Collections.newSetFromMap(new java.util.WeakHashMap[SparkSession, java.lang.Boolean]())

  /** Attach one listener per session. Idempotent: a job that opens two queries must not
    * double-register and double-log every stage. */
  def register(spark: SparkSession, jobName: String): Unit =
    registered.synchronized {
      if (registered.add(spark)) spark.sparkContext.addSparkListener(new SpillMetricsListener(jobName))
    }
}
