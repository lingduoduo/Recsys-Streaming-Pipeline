package com.demo.util

import com.demo.event.Gated
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

/** Emission seam for drop accounting, so a test can assert on the counts a job produced without
  * capturing logs. `DropMetrics` is the logging implementation and the default everywhere; this
  * follows `LateFeedbackJoin.process`, which already takes its wall clock as a parameter to stay
  * testable. */
trait Reporter {

  /** Count the gate, emit a line, and return the surviving rows. */
  def report(gated: Gated, job: String, batchId: Long): DataFrame

  /** Count decode outcomes by Avro error code and emit a line. */
  def reportDecode(deadLetters: DataFrame, validCount: Long, job: String, batchId: Long): Unit
}

/** One `[drop-metrics]` line per gate per micro-batch.
  *
  * Every declared reason is emitted every batch, zeros included, and the line is emitted even when
  * nothing was dropped. A steady `dropped=0` is the positive evidence that the counter is alive: a
  * metric that stays silent when it has nothing to report is indistinguishable from one that is
  * broken, which is exactly how the old `corrupt` field went unnoticed for two months.
  */
object DropMetrics extends Reporter {

  private val log = LoggerFactory.getLogger(getClass)

  /** The four outcomes `EventAvroCodec.decode` can reject a payload with. */
  val DecodeReasons: Seq[String] =
    Seq("invalid_marker", "unknown_fingerprint", "corrupt_payload", "required_field")

  def format(job: String, batchId: Long, kept: Long, reasons: Seq[(String, Long)]): String = {
    val dropped = reasons.map(_._2).sum
    val detail = reasons.map { case (reason, count) => s"$reason=$count" }.mkString(" ")
    s"[drop-metrics] job=$job batch=$batchId kept=$kept dropped=$dropped $detail"
  }

  def report(gated: Gated, job: String, batchId: Long): DataFrame = {
    // Counting is a driver-side action, which a streaming plan cannot run — the reason every gate
    // sits inside foreachBatch. Gate a streaming frame and you still get the filtering, but the
    // counts are unavailable, so say so rather than failing the query or dropping the line
    // silently.
    if (gated.tagged.isStreaming) {
      log.warn(s"[drop-metrics] job=$job batch=$batchId counts unavailable on a streaming plan")
      gated.kept
    } else {
      val (kept, reasons) = gated.counts
      log.info(format(job, batchId, kept, reasons))
      gated.kept
    }
  }

  /** Dead-letter tallies per Avro error code, every declared code present including zeros.
    * Split out from `reportDecode` for the same reason as `format`: the logic is asserted on
    * directly rather than through a captured log line. */
  def decodeCounts(deadLetters: DataFrame): Seq[(String, Long)] = {
    val byCode = deadLetters.groupBy(col("error_code")).count().collect()
      .map(row => row.getString(0) -> row.getLong(1)).toMap
    DecodeReasons.map(code => code -> byCode.getOrElse(code, 0L))
  }

  def reportDecode(deadLetters: DataFrame, validCount: Long, job: String, batchId: Long): Unit =
    log.info(format(job, batchId, validCount, decodeCounts(deadLetters)))
}
