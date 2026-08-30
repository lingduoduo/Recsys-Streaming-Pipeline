package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoFeaturesTest {

    private ServedMovie movie(double banditScore, long impressions, long clicks, Map<String, Object> predictions) {
        return new ServedMovie("m1", 0.4, 0.3, 0.05, banditScore, false, impressions, clicks, predictions);
    }

    @Test
    void producesExactlyDimValues() {
        double[] x = GrpoFeatures.of(movie(0.7, 10, 2, Map.of()), 0, 5);
        assertEquals(GrpoFeatures.DIM, x.length);
    }

    @Test
    void firstDimensionIsTheBiasTerm() {
        double[] x = GrpoFeatures.of(movie(0.7, 10, 2, Map.of()), 3, 5);
        assertEquals(1.0, x[0]);
    }

    @Test
    void countsEnterLogarithmicallySoAPopularItemDoesNotDominate() {
        double[] few = GrpoFeatures.of(movie(0.5, 10, 0, Map.of()), 0, 5);
        double[] many = GrpoFeatures.of(movie(0.5, 100_000, 0, Map.of()), 0, 5);
        // log1p keeps a 10,000x impression gap inside one order of magnitude.
        assertTrue(many[6] < 5.0 * few[6], "impressions must be compressed, got " + many[6] + " vs " + few[6]);
    }

    @Test
    void positionIsNormalizedBySlateSize() {
        double[] x = GrpoFeatures.of(movie(0.5, 0, 0, Map.of()), 2, 4);
        assertEquals(0.5, x[8]);
    }

    @Test
    void aSingletonSlateDoesNotDivideByZero() {
        double[] x = GrpoFeatures.of(movie(0.5, 0, 0, Map.of()), 0, 0);
        assertEquals(0.0, x[8]);
    }

    @Test
    void packCarriesTheVersionPrefix() {
        String packed = GrpoFeatures.pack(new double[] {1.0, 0.5});
        assertTrue(packed.startsWith(GrpoFeatures.VERSION + ":"), packed);
        assertEquals("v1:1.0,0.5", packed);
    }

    @Test
    void predictionScoreComesFromTheModelPredictionsMap() {
        assertEquals(0.83, GrpoFeatures.predictionScore(movie(0.5, 0, 0, Map.of("predictionScore", 0.83))));
    }

    @Test
    void predictionScoreFallsBackToBanditScoreWhenAbsent() {
        // An absent key must not silently become 0.0: a zero logit is a real, wrong policy claim,
        // whereas banditScore is the score predictionScore is derived from.
        assertEquals(0.61, GrpoFeatures.predictionScore(movie(0.61, 0, 0, Map.of())));
    }
}
