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
  *
  * A `SparkListenerTaskEnd` can still arrive after its stage's `SparkListenerStageCompleted` --
  * stage cancellation, speculative kills, and zombie tasksets after a `FetchFailed` all do this.
  * `recordFailure` would otherwise recreate a key that `drain` has already removed and that nothing
  * will ever drain again, growing forever in a job designed to run forever. `maxTracked` caps the
  * number of distinct stage attempts tracked at once: once at the cap, a late failure for a stage
  * attempt not already present is dropped rather than starting a new entry that leaks.
  */
class FailureTally(maxTracked: Int = FailureTally.DefaultMaxTracked) {

  private val counts = new ConcurrentHashMap[(Int, Int), Int]()

  def recordFailure(stageId: Int, attempt: Int): Unit = {
    val key = (stageId, attempt)
    // Only refuse a *new* key at capacity -- a key already being tracked must still be free to
    // accumulate further failures.
    if (counts.containsKey(key) || counts.size() < maxTracked) {
      counts.merge(key, 1, (a: Int, b: Int) => a + b)
    }
    ()
  }

  /** The failure count for this stage attempt, forgetting it in the same step. */
  def drain(stageId: Int, attempt: Int): Int =
    Option(counts.remove((stageId, attempt))).getOrElse(0)

  /** Keys currently held. Exists so a test can prove `drain` releases state. */
  def size: Int = counts.size()
}

object FailureTally {

  /** Generous relative to any real job's concurrent stage count, so it never engages in practice --
    * it exists only to bound the damage from the late-event leak described above. */
  val DefaultMaxTracked: Int = 10000
}
