package com.demo.util

/** Pure feature-derivation helpers for the MovieLens segment report.
  *
  * Scala port of `services/python-modeling/segment_features.py`. Derives segment buckets from the
  * demographics stored in Redis `user:{id}:features` (age, zipCode).
  */
object SegmentFeatures {

  // (loInclusive, hiInclusive, label)
  private val AgeBins = Seq(
    (0, 24, "18-24"), (25, 34, "25-34"), (35, 44, "35-44"), (45, 54, "45-54"), (55, 200, "55+"))

  // US ZIP first digit → coarse region.
  private val ZipRegion: Map[String, String] = Map(
    "0" -> "Northeast", "1" -> "Northeast", "2" -> "Mid-Atlantic", "3" -> "Southeast",
    "4" -> "Midwest", "5" -> "Midwest", "6" -> "South-Central", "7" -> "South-Central",
    "8" -> "Mountain", "9" -> "West")

  def deriveAgeBand(age: String): String =
    scala.util.Try(age.trim.toInt).toOption match {
      case Some(a) => AgeBins.collectFirst { case (lo, hi, label) if a >= lo && a <= hi => label }
        .getOrElse("unknown")
      case None => "unknown"
    }

  def deriveGeo(zipCode: String): String =
    ZipRegion.getOrElse(Option(zipCode).getOrElse("").take(1), "unknown")
}
