package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SegmentFeaturesSpec extends AnyFlatSpec with Matchers {

  "deriveAgeBand" should "bucket ages and fall back to unknown" in {
    SegmentFeatures.deriveAgeBand("20") shouldBe "18-24"
    SegmentFeatures.deriveAgeBand("30") shouldBe "25-34"
    SegmentFeatures.deriveAgeBand("60") shouldBe "55+"
    SegmentFeatures.deriveAgeBand("33") shouldBe "25-34"
    SegmentFeatures.deriveAgeBand(null) shouldBe "unknown"
    SegmentFeatures.deriveAgeBand("") shouldBe "unknown"
  }

  "deriveGeo" should "map ZIP first digit to a region, else unknown" in {
    SegmentFeatures.deriveGeo("90210") shouldBe "West"
    SegmentFeatures.deriveGeo("02139") shouldBe "Northeast"
    SegmentFeatures.deriveGeo("70001") shouldBe "South-Central"
    SegmentFeatures.deriveGeo("") shouldBe "unknown"
    SegmentFeatures.deriveGeo(null) shouldBe "unknown"
  }
}
