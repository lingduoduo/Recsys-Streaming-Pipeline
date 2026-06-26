package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BatchMetricsListenerSpec extends AnyFlatSpec with Matchers {

  "BatchMetricsListener.format" should "render the per-batch metrics line incl. corrupt count" in {
    val line = BatchMetricsListener.format("UserEventStreamingJob", 5000L, 1234.5, 405L, 7L)
    line should include ("UserEventStreamingJob")
    line should include ("rows=5000")
    line should include ("rps=1234.5")
    line should include ("batchMs=405")
    line should include ("corrupt=7")
  }
}
