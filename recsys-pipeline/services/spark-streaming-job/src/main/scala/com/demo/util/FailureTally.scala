package com.demo.util

import java.util.concurrent.ConcurrentHashMap

/** Counts failed tasks per stage attempt, and forgets each one as soon as it is reported.
  *
  * Spark's StageInfo carries no failed-task count -- `failureReason` is set only when the whole
  * stage failed, which is not the same thing and is absent for a stage that succeeded after
  * retrying tasks. So failures have to be accumulated from task-end events and matched up when the
  * stage completes.
  *
  * `drain` removes the key rather than merely reading it: the listener holding this runs for the
  * life of the query, and a streaming query's stages are unbounded.
  *
  * Spark delivers listener events on a single dispatch thread, but a ConcurrentHashMap costs
  * nothing here and removes the question.
  */
class FailureTally {

  private val counts = new ConcurrentHashMap[(Int, Int), Int]()

  def recordFailure(stageId: Int, attempt: Int): Unit = {
    counts.merge((stageId, attempt), 1, (a: Int, b: Int) => a + b)
    ()
  }

  /** The failure count for this stage attempt, forgetting it in the same step. */
  def drain(stageId: Int, attempt: Int): Int =
    Option(counts.remove((stageId, attempt))).getOrElse(0)

  /** Keys currently held. Exists so a test can prove `drain` releases state. */
  def size: Int = counts.size()
}
