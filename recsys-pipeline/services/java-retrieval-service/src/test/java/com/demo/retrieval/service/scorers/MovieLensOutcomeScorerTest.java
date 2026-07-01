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
        assertTrue(strong.outcomeProbability() > weak.outcomeProbability());
        assertTrue(strong.finalScore() > weak.finalScore());
    }

    @Test
    void outcomeProbabilitiesUseMovieLensSpecificNames() {
        MovieLensOutcomeProbabilities outcomes =
            new MovieLensOutcomeProbabilities(0.8, 0.7, 0.6, 0.9, 0.5, 0.1);

        assertTrue(outcomes.positiveRating() > outcomes.negativeFeedback());
        assertTrue(outcomes.watch() > outcomes.click());
        assertTrue(outcomes.overall() > 0.0);
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
