package com.demo.profile

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserProfileDerivationsSpec extends AnyFlatSpec with Matchers {

  import UserProfileDerivations._

  "eventWeight" should "choose the strongest signal before applying recency decay" in {
    val cfg = ProfileConfig(referenceEpochSeconds = 1000L, halfLifeSeconds = 100L)

    val cases = Seq(
      EvidenceInput(900L, clicked = true, ordered = true, rating = None) -> 1.5,
      EvidenceInput(900L, clicked = false, ordered = false, rating = None) -> -0.05,
      EvidenceInput(1000L, clicked = true, ordered = false, rating = Some(5.0)) -> 1.0
    )

    cases.foreach { case (input, expected) =>
      eventWeight(input, cfg) shouldBe expected +- 1e-9
    }
  }

  "normalizePreferences" should "shrink, bound, and order explanatory preferences deterministically" in {
    val cfg = ProfileConfig(referenceEpochSeconds = 1000L, halfLifeSeconds = 100L)
    val preferences = normalizePreferences(Map(
      "beta" -> (7.5, 5L),
      "alpha" -> (2.5, 5L),
      "negative" -> (-20.0, 5L),
      "huge" -> (100.0, 1L)
    ), cfg)

    preferences shouldBe Seq(
      Preference("huge", 1.0, 1L),
      Preference("beta", 0.75, 5L),
      Preference("alpha", 0.25, 5L),
      Preference("negative", -1.0, 5L)
    )
    positivePreferences(preferences).map(_.value) shouldBe Seq("huge", "beta", "alpha")
  }

  "classifyPersonas" should "return only new_or_unknown below the minimum evidence boundary" in {
    val cfg = ProfileConfig(referenceEpochSeconds = 1000L, halfLifeSeconds = 100L)
    val lowEvidence = BehavioralFeatures(evidenceCount = 4L)

    classifyPersonas(lowEvidence, cfg).map(_.personaType) shouldBe Seq("new_or_unknown")
  }

  it should "apply taxonomy rules at their boundaries in fixed order" in {
    val cfg = ProfileConfig(referenceEpochSeconds = 1000L, halfLifeSeconds = 100L)
    val explorerAndRecent = BehavioralFeatures(
      evidenceCount = 5L,
      genreDiversity = Some(cfg.genreExplorerDiversityThreshold),
      preferenceConcentration = Some(cfg.genreExplorerMaxConcentration),
      recentReleaseAffinity = Some(cfg.recentReleaseAffinityThreshold)
    )
    val enthusiast = BehavioralFeatures(
      evidenceCount = 5L,
      genrePreferences = Seq(Preference("sci-fi", cfg.genreEnthusiastThreshold, 5L))
    )
    val focused = BehavioralFeatures(
      evidenceCount = 5L,
      preferenceConcentration = Some(cfg.focusedViewerConcentrationThreshold)
    )
    val highIntent = BehavioralFeatures(
      evidenceCount = 5L,
      impressions = 5L,
      clicks = 2L,
      orders = 1L,
      engagementRate = Some(cfg.highIntentEngagementThreshold),
      conversionRate = Some(cfg.highIntentConversionThreshold)
    )
    val casual = BehavioralFeatures(
      evidenceCount = 5L,
      impressions = 5L,
      engagementRate = Some(cfg.casualBrowserEngagementThreshold),
      conversionRate = Some(cfg.casualBrowserConversionThreshold)
    )

    classifyPersonas(explorerAndRecent, cfg).map(_.personaType) shouldBe
      Seq("genre_explorer", "recent_release_seeker")
    classifyPersonas(enthusiast, cfg).map(_.personaType) shouldBe Seq("genre_enthusiast")
    classifyPersonas(focused, cfg).map(_.personaType) shouldBe Seq("focused_viewer")
    classifyPersonas(highIntent, cfg).map(_.personaType) shouldBe Seq("high_intent_engager")
    classifyPersonas(casual, cfg).map(_.personaType) shouldBe Seq("casual_browser")
  }

  it should "attach named evidence and a bounded threshold-distance confidence to every persona" in {
    val cfg = ProfileConfig(referenceEpochSeconds = 1000L, halfLifeSeconds = 100L)
    val persona = classifyPersonas(BehavioralFeatures(
      evidenceCount = 5L,
      genrePreferences = Seq(Preference("sci-fi", 1.0, 5L))
    ), cfg).head

    persona.evidence("preference_score") shouldBe 1.0
    persona.confidence shouldBe 1.0 +- 1e-9
  }

  it should "select the highest-scoring genre enthusiast regardless of caller preference order" in {
    val cfg = ProfileConfig(referenceEpochSeconds = 1000L, halfLifeSeconds = 100L)
    val unsortedPreferences = Seq(
      Preference("drama", cfg.genreEnthusiastThreshold - 0.1, 10L),
      Preference("sci-fi", cfg.genreEnthusiastThreshold + 0.1, 10L)
    )

    val persona = classifyPersonas(BehavioralFeatures(
      evidenceCount = 10L,
      genrePreferences = unsortedPreferences
    ), cfg).head

    persona.personaType shouldBe "genre_enthusiast"
    persona.label shouldBe "Sci Fi enthusiast"
    persona.evidence("preference_score") shouldBe cfg.genreEnthusiastThreshold + 0.1
  }

  "ProfileConfig" should "reject non-positive source lookback and recent-release ages" in {
    an[IllegalArgumentException] should be thrownBy ProfileConfig(
      referenceEpochSeconds = 1000L, halfLifeSeconds = 100L, sourceLookbackSeconds = 0L)
    an[IllegalArgumentException] should be thrownBy ProfileConfig(
      referenceEpochSeconds = 1000L, halfLifeSeconds = 100L, recentReleaseAgeSeconds = 0L)
  }
}
