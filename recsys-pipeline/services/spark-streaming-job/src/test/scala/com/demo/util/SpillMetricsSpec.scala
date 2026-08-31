package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpillMetricsSpec extends AnyFlatSpec with Matchers {

  private def cost(
      spillMem: Long = 0L, spillDisk: Long = 0L, failed: Int = 0, attempt: Int = 0) =
    SpillMetrics.StageCost(
      job = "SessionReportJob", stageId = 4, attempt = attempt, tasks = 8,
      spillMemBytes = spillMem, spillDiskBytes = spillDisk,
      shuffleWriteBytes = 2100000000L, shuffleReadBytes = 2100000000L, failedTasks = failed)

  "humanBytes" should "render plain bytes below a kilobyte" in {
    SpillMetrics.humanBytes(0L) shouldBe "0B"
    SpillMetrics.humanBytes(512L) shouldBe "512B"
  }

  it should "step through K, M and G at the boundaries" in {
    SpillMetrics.humanBytes(1024L) shouldBe "1.0K"
    SpillMetrics.humanBytes(1024L * 1024L) shouldBe "1.0M"
    SpillMetrics.humanBytes(1024L * 1024L * 1024L) shouldBe "1.0G"
  }

  it should "not overflow on a value larger than Int.MaxValue" in {
    // 8 GiB. Any intermediate Int arithmetic wraps negative here.
    SpillMetrics.humanBytes(8L * 1024L * 1024L * 1024L) shouldBe "8.0G"
  }

  "format" should "name the job, stage, attempt and every cost" in {
    // Exact match, not `include`: a loose per-field `include` check let the design spec's example
    // line drift from the code's actual formatting (840M vs the real 840.0M) through four reviews.
    val line = SpillMetrics.format(cost(spillMem = 1288490188L, spillDisk = 880803840L))
    line shouldBe
      "[spill-metrics] job=SessionReportJob stage=4 attempt=0 tasks=8 spillMem=1.2G " +
        "spillDisk=840.0M shuffleWrite=2.0G shuffleRead=2.0G failedTasks=0"
  }

  "worthInfo" should "stay quiet for a clean stage" in {
    // A streaming query produces stages forever; an INFO line per clean stage would bury the
    // lines this listener exists to surface.
    SpillMetrics.worthInfo(cost()) shouldBe false
  }

  it should "fire on memory spill" in {
    SpillMetrics.worthInfo(cost(spillMem = 1L)) shouldBe true
  }

  it should "fire on disk spill" in {
    SpillMetrics.worthInfo(cost(spillDisk = 1L)) shouldBe true
  }

  it should "fire on a failed task" in {
    SpillMetrics.worthInfo(cost(failed = 1)) shouldBe true
  }

  it should "fire on a retried stage that spilled nothing" in {
    // attemptNumber > 0 IS a stage retry, and it is the direct answer to "are stages retrying".
    // A retry with no spill is exactly the case a spill-only rule would hide.
    SpillMetrics.worthInfo(cost(attempt = 1)) shouldBe true
  }
}
