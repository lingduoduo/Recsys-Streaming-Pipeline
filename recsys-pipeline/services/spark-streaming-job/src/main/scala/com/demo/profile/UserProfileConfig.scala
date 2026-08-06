package com.demo.profile

/** Immutable controls for explainable behavioral-profile derivation. */
final case class ProfileConfig(
    referenceEpochSeconds: Long,
    halfLifeSeconds: Long,
    impressionWeight: Double = -0.1,
    clickWeight: Double = 1.0,
    orderWeight: Double = 3.0,
    ratingMin: Double = 1.0,
    ratingMidpoint: Double = 3.0,
    ratingMax: Double = 5.0,
    shrinkage: Double = 5.0,
    minimumEvidence: Long = 5L,
    maxGenres: Int = 10,
    maxTags: Int = 20,
    genreEnthusiastThreshold: Double = 0.6,
    genreExplorerDiversityThreshold: Double = 0.6,
    genreExplorerMaxConcentration: Double = 0.5,
    focusedViewerConcentrationThreshold: Double = 0.7,
    recentReleaseAffinityThreshold: Double = 0.6,
    highIntentEngagementThreshold: Double = 0.4,
    highIntentConversionThreshold: Double = 0.1,
    casualBrowserEngagementThreshold: Double = 0.1,
    casualBrowserConversionThreshold: Double = 0.02,
    sourceLookbackSeconds: Long = 30L * 24L * 60L * 60L,
    recentReleaseAgeSeconds: Long = 365L * 24L * 60L * 60L
) {
  require(halfLifeSeconds > 0L, "halfLifeSeconds must be positive")
  require(ratingMin < ratingMidpoint && ratingMidpoint < ratingMax,
    "ratingMin < ratingMidpoint < ratingMax is required")
  require(shrinkage >= 0.0 && !shrinkage.isInfinite && !shrinkage.isNaN,
    "shrinkage must be finite and non-negative")
  require(minimumEvidence >= 0L, "minimumEvidence must be non-negative")
  require(maxGenres > 0 && maxTags > 0, "preference limits must be positive")
  require(sourceLookbackSeconds > 0L, "sourceLookbackSeconds must be positive")
  require(recentReleaseAgeSeconds > 0L, "recentReleaseAgeSeconds must be positive")
}

final case class EvidenceInput(
    eventEpochSeconds: Long,
    clicked: Boolean,
    ordered: Boolean,
    rating: Option[Double]
)

final case class Preference(value: String, score: Double, evidenceCount: Long)

final case class BehavioralFeatures(
    evidenceCount: Long,
    impressions: Long = 0L,
    clicks: Long = 0L,
    orders: Long = 0L,
    genrePreferences: Seq[Preference] = Seq.empty,
    tagPreferences: Seq[Preference] = Seq.empty,
    engagementRate: Option[Double] = None,
    conversionRate: Option[Double] = None,
    averageRating: Option[Double] = None,
    genreDiversity: Option[Double] = None,
    preferenceConcentration: Option[Double] = None,
    recentReleaseAffinity: Option[Double] = None,
    activityLevel: String = "unknown"
)

final case class Persona(
    personaType: String,
    label: String,
    confidence: Double,
    evidence: Map[String, Double]
)
