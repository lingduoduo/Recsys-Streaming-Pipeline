package com.demo.profile

object UserProfileDerivations {

  /** Chooses exactly one strongest outcome before applying recency decay. */
  def eventWeight(input: EvidenceInput, config: ProfileConfig): Double = {
    val base = input.rating match {
      case Some(value) => ratingWeight(value, config)
      case None if input.ordered => config.orderWeight
      case None if input.clicked => config.clickWeight
      case None => config.impressionWeight
    }
    val age = math.max(0L, config.referenceEpochSeconds - input.eventEpochSeconds)
    base * math.pow(0.5, age.toDouble / config.halfLifeSeconds.toDouble)
  }

  private def ratingWeight(value: Double, config: ProfileConfig): Double = {
    val bounded = math.max(config.ratingMin, math.min(config.ratingMax, value))
    if (bounded >= config.ratingMidpoint)
      (bounded - config.ratingMidpoint) / (config.ratingMax - config.ratingMidpoint)
    else
      (bounded - config.ratingMidpoint) / (config.ratingMidpoint - config.ratingMin)
  }

  /** Shrinks feature evidence toward zero, bounds it, and gives tied scores a stable name order. */
  def normalizePreferences(
      rawPreferences: Map[String, (Double, Long)],
      config: ProfileConfig
  ): Seq[Preference] =
    rawPreferences.iterator.collect {
      case (value, (rawScore, count)) if value != null && value.nonEmpty && count > 0L && finite(rawScore) =>
        val denominator = count.toDouble + config.shrinkage
        val score = if (denominator == 0.0) 0.0 else clamp(rawScore / denominator, -1.0, 1.0)
        Preference(value, score, count)
    }.toSeq.sortBy(preference => (-preference.score, preference.value))

  /** Version-one serving uses affinities only; non-positive preferences remain explanatory. */
  def positivePreferences(preferences: Seq[Preference]): Seq[Preference] =
    preferences.filter(_.score > 0.0)

  /** Assigns deterministic, explainable multi-label personas from already-derived behavioral features. */
  def classifyPersonas(features: BehavioralFeatures, config: ProfileConfig): Seq[Persona] = {
    if (features.evidenceCount < config.minimumEvidence) {
      Seq(Persona(
        personaType = "new_or_unknown",
        label = "New or unknown",
        confidence = 1.0,
        evidence = Map("evidence_count" -> features.evidenceCount.toDouble,
          "minimum_evidence" -> config.minimumEvidence.toDouble)
      ))
    } else {
      val personas = Vector.newBuilder[Persona]
      positivePreferences(features.genrePreferences).sortBy(preference => (-preference.score, preference.value)).headOption
        .filter(_.score >= config.genreEnthusiastThreshold)
        .foreach { preference =>
          personas += Persona(
            "genre_enthusiast",
            s"${titleCase(preference.value)} enthusiast",
            aboveThreshold(preference.score, config.genreEnthusiastThreshold),
            Map("preference_score" -> preference.score, "evidence_count" -> preference.evidenceCount.toDouble)
          )
        }

      for {
        diversity <- features.genreDiversity
        concentration <- features.preferenceConcentration
        if diversity >= config.genreExplorerDiversityThreshold && concentration <= config.genreExplorerMaxConcentration
      } personas += Persona(
        "genre_explorer",
        "Genre explorer",
        math.min(
          aboveThreshold(diversity, config.genreExplorerDiversityThreshold),
          belowThreshold(concentration, config.genreExplorerMaxConcentration)
        ),
        Map("genre_diversity" -> diversity, "preference_concentration" -> concentration)
      )

      features.preferenceConcentration
        .filter(_ >= config.focusedViewerConcentrationThreshold)
        .foreach { concentration =>
          personas += Persona(
            "focused_viewer",
            "Focused viewer",
            aboveThreshold(concentration, config.focusedViewerConcentrationThreshold),
            Map("preference_concentration" -> concentration)
          )
        }

      features.recentReleaseAffinity
        .filter(_ >= config.recentReleaseAffinityThreshold)
        .foreach { affinity =>
          personas += Persona(
            "recent_release_seeker",
            "Recent release seeker",
            aboveThreshold(affinity, config.recentReleaseAffinityThreshold),
            Map("recent_release_affinity" -> affinity)
          )
        }

      for {
        engagement <- features.engagementRate
        conversion <- features.conversionRate
        if engagement >= config.highIntentEngagementThreshold && conversion >= config.highIntentConversionThreshold
      } personas += Persona(
        "high_intent_engager",
        "High-intent engager",
        math.min(
          aboveThreshold(engagement, config.highIntentEngagementThreshold),
          aboveThreshold(conversion, config.highIntentConversionThreshold)
        ),
        Map("engagement_rate" -> engagement, "conversion_rate" -> conversion,
          "clicks" -> features.clicks.toDouble, "orders" -> features.orders.toDouble)
      )

      for {
        engagement <- features.engagementRate
        conversion <- features.conversionRate
        if features.impressions >= config.minimumEvidence &&
          engagement <= config.casualBrowserEngagementThreshold &&
          conversion <= config.casualBrowserConversionThreshold
      } personas += Persona(
        "casual_browser",
        "Casual browser",
        math.min(
          belowThreshold(engagement, config.casualBrowserEngagementThreshold),
          belowThreshold(conversion, config.casualBrowserConversionThreshold)
        ),
        Map("impressions" -> features.impressions.toDouble, "engagement_rate" -> engagement,
          "conversion_rate" -> conversion)
      )

      personas.result()
    }
  }

  private def aboveThreshold(value: Double, threshold: Double): Double =
    if (threshold >= 1.0) if (value >= threshold) 1.0 else 0.0
    else clamp((value - threshold) / (1.0 - threshold), 0.0, 1.0)

  private def belowThreshold(value: Double, threshold: Double): Double =
    if (threshold <= 0.0) if (value <= threshold) 1.0 else 0.0
    else clamp((threshold - value) / threshold, 0.0, 1.0)

  private def clamp(value: Double, lower: Double, upper: Double): Double =
    math.max(lower, math.min(upper, value))

  private def finite(value: Double): Boolean = !value.isNaN && !value.isInfinite

  private def titleCase(value: String): String =
    value.split("[-_ ]+").filter(_.nonEmpty).map { word =>
      word.substring(0, 1).toUpperCase + word.substring(1)
    }.mkString(" ")
}
