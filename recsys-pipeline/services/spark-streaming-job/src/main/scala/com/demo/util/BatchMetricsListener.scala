package com.demo.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.slf4j.LoggerFactory

/** Logs one line per micro-batch: input rows, throughput, and batch duration. */
object BatchMetricsListener {
  private val log = LoggerFactory.getLogger(getClass)

  def format(name: String, numInputRows: Long, rowsPerSecond: Double, durationMs: Long, corrupt: Long): String =
    s"[batch-metrics] query=$name rows=$numInputRows rps=$rowsPerSecond batchMs=$durationMs corrupt=$corrupt"

  def register(spark: SparkSession): Unit =
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(e: StreamingQueryListener.QueryStartedEvent): Unit = ()
      override def onQueryTerminated(e: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
      override def onQueryProgress(e: StreamingQueryListener.QueryProgressEvent): Unit = {
        val p = e.progress
        // durationMs is a Map of phase -> millis; "triggerExecution" is the batch wall time.
        val batchMs = Option(p.durationMs.get("triggerExecution")).map(_.toLong).getOrElse(0L)
        val corrupt = Option(p.observedMetrics.get("ingest")).map(_.getAs[Long]("corrupt")).getOrElse(0L)
        log.info(format(p.name, p.numInputRows, p.processedRowsPerSecond, batchMs, corrupt))
      }
    })
}
