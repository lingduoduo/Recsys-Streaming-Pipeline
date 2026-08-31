package com.demo.util

/** What one completed stage cost, and whether that is worth saying out loud.
  *
  * Separated from the listener so the rendering and the emission rule are testable without a
  * SparkSession — `SparkSessions.create` calls `getOrCreate`, so session-level assertions in this
  * module are unreliable by construction.
  */
object SpillMetrics {

  /** The shuffle and spill cost of a single completed stage attempt. */
  final case class StageCost(
      job: String,
      stageId: Int,
      attempt: Int,
      tasks: Int,
      spillMemBytes: Long,
      spillDiskBytes: Long,
      shuffleWriteBytes: Long,
      shuffleReadBytes: Long,
      failedTasks: Int)

  private val Unit1K = 1024L
  private val Unit1M = Unit1K * 1024L
  private val Unit1G = Unit1M * 1024L

  /** Raw byte counts are unreadable at the scale that matters: 1288490188 versus 1.2G. */
  def humanBytes(n: Long): String =
    if (n >= Unit1G) f"${n.toDouble / Unit1G}%.1fG"
    else if (n >= Unit1M) f"${n.toDouble / Unit1M}%.1fM"
    else if (n >= Unit1K) f"${n.toDouble / Unit1K}%.1fK"
    else s"${n}B"

  def format(c: StageCost): String =
    s"[spill-metrics] job=${c.job} stage=${c.stageId} attempt=${c.attempt} tasks=${c.tasks} " +
      s"spillMem=${humanBytes(c.spillMemBytes)} spillDisk=${humanBytes(c.spillDiskBytes)} " +
      s"shuffleWrite=${humanBytes(c.shuffleWriteBytes)} shuffleRead=${humanBytes(c.shuffleReadBytes)} " +
      s"failedTasks=${c.failedTasks}"

  /** Emit at INFO only when something happened.
    *
    * This deliberately breaks the rule DropMetrics states -- that a silent counter is
    * indistinguishable from a broken one -- and the reason is cardinality, not disagreement.
    * DropMetrics fires once per micro-batch per gate; this fires once per STAGE, and a streaming
    * query produces stages continuously and forever. Emitting every clean stage at INFO would bury
    * the lines this exists to surface. The caller logs the quiet case at DEBUG, which keeps the
    * counter provably alive.
    */
  def worthInfo(c: StageCost): Boolean =
    c.spillMemBytes > 0L || c.spillDiskBytes > 0L || c.failedTasks > 0 || c.attempt > 0
}
