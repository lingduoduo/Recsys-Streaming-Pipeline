package com.demo.engine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RedisSinkSpec extends AnyFlatSpec with Matchers {

  "foreachWithFlush" should "apply onRow to every element and flush on the cadence" in {
    def run(n: Int, size: Int): (Int, Int) = {
      var rows = 0
      var flushes = 0
      RedisSink.foreachWithFlush((1 to n).iterator, size)(_ => rows += 1)(() => flushes += 1)
      (rows, flushes)
    }
    run(3, 2) shouldBe (3, 2)  // flush after 2, final flush for the 3rd
    run(4, 2) shouldBe (4, 2)  // exact multiple, no extra final flush
    run(1, 2) shouldBe (1, 1)  // one pending -> final flush
    run(0, 2) shouldBe (0, 0)  // nothing to do
  }
}
