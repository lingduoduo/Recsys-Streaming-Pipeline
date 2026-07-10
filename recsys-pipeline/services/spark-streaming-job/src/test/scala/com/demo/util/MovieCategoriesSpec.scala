package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MovieCategoriesSpec extends AnyFlatSpec with Matchers {

  "MovieCategories" should "derive l1/l2/l3 from comma-joined genres + year" in {
    MovieCategories.l1("Sci-Fi,Action") shouldBe "SciFi&Fantasy"
    MovieCategories.l2("Sci-Fi,Action") shouldBe "Sci-Fi"
    MovieCategories.l3("Sci-Fi,Action", "2011") shouldBe "Sci-Fi·2010s"
  }

  it should "use the first genre as the primary and map unknown families to Other" in {
    MovieCategories.l1("Documentary") shouldBe "Other"
    MovieCategories.l2("Comedy,Drama") shouldBe "Comedy"
  }

  it should "fall back to unknown for missing/blank genres and years" in {
    MovieCategories.l2("") shouldBe "unknown"
    MovieCategories.l2(null) shouldBe "unknown"
    MovieCategories.l3("Drama", "") shouldBe "Drama·unknown"
    MovieCategories.l3("Drama", "notayear") shouldBe "Drama·unknown"
  }
}
