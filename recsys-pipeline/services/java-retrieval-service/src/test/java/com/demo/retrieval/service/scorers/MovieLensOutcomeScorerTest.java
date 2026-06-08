package com.demo.retrieval.service.scorers;

import com.demo.retrieval.service.scorers.MovieLensOutcomeScorer.MovieLensOutcomeProbabilities;
import com.demo.retrieval.service.scorers.MovieLensOutcomeScorer.ScoringInput;
import com.demo.retrieval.service.scorers.MovieLensOutcomeScorer.ScoringResult;
import org.junit.jupiter.api.Test;

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
