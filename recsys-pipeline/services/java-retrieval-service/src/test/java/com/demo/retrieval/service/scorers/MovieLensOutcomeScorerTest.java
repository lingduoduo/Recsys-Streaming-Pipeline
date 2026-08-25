package com.demo.retrieval.service.scorers;

import com.demo.retrieval.service.scorers.MovieLensOutcomeScorer.MovieLensOutcomeProbabilities;
import com.demo.retrieval.service.scorers.MovieLensOutcomeScorer.ScoringInput;
import com.demo.retrieval.service.scorers.MovieLensOutcomeScorer.ScoringResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MovieLensOutcomeScorerTest {
    private final MovieLensOutcomeScorer scorer = new MovieLensOutcomeScorer();

    @Test
    void strongerMovieLensSignalsProduceHigherOutcomeScore() {
        ScoringResult weak = scorer.score(input(0.1, 0.1, 0.1, 100, 1, 0.0));
        ScoringResult strong = scorer.score(input(0.9, 0.9, 0.8, 100, 40, 0.8));

        assertTrue(strong.weightedOutcomeScore() > weak.weightedOutcomeScore());
        assertTrue(strong.finalScore() > weak.finalScore());
    }

    @Test
    void outcomeProbabilitiesUseMovieLensSpecificNames() {
        MovieLensOutcomeProbabilities outcomes =
            new MovieLensOutcomeProbabilities(0.8, 0.7, 0.6, 0.9, 0.5, 0.1);

        assertTrue(outcomes.positiveRating() > outcomes.negativeFeedback());
        assertTrue(outcomes.watch() > outcomes.click());
    }

    @Test
    void convexBlendWeightsSumToOne() {
        assertEquals(1.0,
            MovieLensOutcomeScorer.EXPLOITATION_BANDIT_WEIGHT
            + MovieLensOutcomeScorer.EXPLOITATION_OUTCOME_WEIGHT
            + MovieLensOutcomeScorer.EXPLOITATION_DL_WEIGHT
            + MovieLensOutcomeScorer.EXPLOITATION_Q_WEIGHT, 1e-9,
            "exploitation blend must stay convex");
        assertEquals(1.0,
            MovieLensOutcomeScorer.ESTIMATED_REWARD_POSTERIOR_WEIGHT
            + MovieLensOutcomeScorer.ESTIMATED_REWARD_OUTCOME_WEIGHT, 1e-9,
            "estimated-reward blend must stay convex");
        assertEquals(1.0,
            MovieLensOutcomeScorer.OUTCOME_POSITIVE_RATING_WEIGHT
            + MovieLensOutcomeScorer.OUTCOME_PREFERENCE_WEIGHT
            + MovieLensOutcomeScorer.OUTCOME_CLICK_WEIGHT
            + MovieLensOutcomeScorer.OUTCOME_WATCH_WEIGHT
            + MovieLensOutcomeScorer.OUTCOME_NOVEL_DISCOVERY_WEIGHT, 1e-9,
            "positive-outcome weights must stay convex");
    }

    // ---- Characterization: pinned before the deep-learning terms are removed ------------------
    // Unlike blendOfflineScore, this class is NOT gated by deepLearningWeight -- it applies a
    // hardcoded 0.15 to input.dlScore() directly. It is inert only because dlScore is always 0.0
    // at runtime (the deepLearningEnabled gate stops predictBatch from ever running). So the
    // input below uses dlScore = 0.0, the only value that actually occurs, and pins the result.
    // Removing the dl terms must leave both numbers untouched.
    @Test
    void scoringCharacterizedAtTheOnlyDeepLearningScoreThatOccurs() {
        MovieLensOutcomeScorer scorer = new MovieLensOutcomeScorer();
        ScoringResult result = scorer.score(
            new ScoringInput(
                "m1",
                0.70,   // relevance
                0.50,   // content
                0.40,   // popularity
                0.30,   // posteriorMean
                0.60,   // banditRankingScore
                0.05,   // explorationBonus
                0.20,   // noveltyScore
                0.0,    // dlScore -- the only value the gate permits
                0.10,   // qValue
                40L,    // impressions
                8L      // clicks
            ));

        assertEquals(0.5182932849671755, result.predictionScore(), 1e-9);
        assertEquals(0.38161059895404564, result.estimatedReward(), 1e-9);
    }

    private static ScoringInput input(
        double relevance,
        double content,
        double popularity,
        long impressions,
        long clicks,
        double novelty
    ) {
        return new ScoringInput(
            "movie",
            relevance,
            content,
            popularity,
            0.5,
            0.5,
            0.1,
            novelty,
            0.0,
            0.0,
            impressions,
            clicks
        );
    }
}
