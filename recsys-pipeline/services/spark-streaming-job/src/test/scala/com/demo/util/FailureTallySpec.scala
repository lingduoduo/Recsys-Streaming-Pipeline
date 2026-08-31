package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FailureTallySpec extends AnyFlatSpec with Matchers {

  "drain" should "return zero for a stage that never failed" in {
    new FailureTally().drain(1, 0) shouldBe 0
  }

  it should "count failures for one stage attempt" in {
    val tally = new FailureTally()
    tally.recordFailure(1, 0)
    tally.recordFailure(1, 0)
    tally.drain(1, 0) shouldBe 2
  }

  it should "keep attempts of the same stage separate" in {
    // A retried stage is a different attempt; merging them would misreport both.
    val tally = new FailureTally()
    tally.recordFailure(1, 0)
    tally.recordFailure(1, 1)
    tally.drain(1, 0) shouldBe 1
    tally.drain(1, 1) shouldBe 1
  }

  it should "release the key once drained" in {
    // A listener that accumulates per-stage state for the life of a long-running streaming query
    // and never releases it is itself a memory leak -- an absurd way for an observability feature
    // to fail.
    val tally = new FailureTally()
    tally.recordFailure(1, 0)
    tally.size shouldBe 1
    tally.drain(1, 0)
    tally.size shouldBe 0
  }

  it should "return zero on a second drain of the same key" in {
    val tally = new FailureTally()
    tally.recordFailure(1, 0)
    tally.drain(1, 0) shouldBe 1
    tally.drain(1, 0) shouldBe 0
  }

  it should "not grow without bound when a TaskEnd arrives after its stage already drained" in {
    // Stage cancellation, speculative kills, and zombie tasksets after a FetchFailed can all
    // deliver a TaskEnd after onStageCompleted has already drained that stage attempt. Each one
    // used to recreate the key with nothing left to ever drain it again -- exactly the leak a
    // long-running streaming query would hit. maxTracked is set small here so the cap engages
    // within a handful of stages instead of ten thousand.
    val tally = new FailureTally(maxTracked = 3)
    (0 until 20).foreach { stageId =>
      tally.recordFailure(stageId, 0)
      tally.drain(stageId, 0)
      tally.recordFailure(stageId, 0) // the late TaskEnd, arriving after the stage was drained
    }
    tally.size should be <= 3
  }
}
