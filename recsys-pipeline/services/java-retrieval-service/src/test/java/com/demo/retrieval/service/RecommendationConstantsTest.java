package com.demo.retrieval.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecommendationConstantsTest {

    // Finding 1: default weights sum to 1.15, so a raw weighted sum of saturated components
    // reaches 1.15 and is clipped downstream. Normalizing by the weight sum keeps it in [0, 1].
    @Test
    void offlineScoreStaysInRangeWhenWeightsSumAboveOne() {
        double score = RecommendationConstants.blendOfflineScore(
            0.6, 1.0,   // relevance
            0.25, 1.0,  // content
            0.15, 1.0,  // popularity
            0.15, 1.0   // deep learning  -> weights sum to 1.15
        );
        assertEquals(1.0, score, 1e-9, "all components saturated must yield exactly the upper bound");
    }

    // Finding 2: the deep-learning ONNX score has no bounded activation. A raw out-of-range value
    // must not push the blend outside [0, 1] nor dominate the other (normalized) components.
    @Test
    void deepLearningScoreIsClampedIntoRange() {
        double high = RecommendationConstants.blendOfflineScore(
            0.6, 0.0, 0.25, 0.0, 0.15, 0.0, 0.15, 42.0);
        double low = RecommendationConstants.blendOfflineScore(
            0.6, 0.0, 0.25, 0.0, 0.15, 0.0, 0.15, -7.0);

        // dlScore 42 clamps to 1.0 -> 0.15 / 1.15 of the range; -7 clamps to 0.0.
        assertEquals(0.15 / 1.15, high, 1e-9);
        assertEquals(0.0, low, 1e-9);
    }

    @Test
    void normalizationPreservesConfiguredRatios() {
        // Only relevance active: normalized contribution is relevance itself regardless of the
        // other zero-valued components, because we divide by the full weight sum.
        double score = RecommendationConstants.blendOfflineScore(
            0.6, 0.8, 0.25, 0.0, 0.15, 0.0, 0.15, 0.0);
        assertEquals(0.6 * 0.8 / 1.15, score, 1e-9);
    }

    @Test
    void zeroWeightsCollapseToLowerBound() {
        assertEquals(0.0, RecommendationConstants.blendOfflineScore(
            0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0), 1e-9);
    }

    @Test
    void clampRejectsNonFiniteAndOutOfRange() {
        assertEquals(0.0, RecommendationConstants.clamp(Double.NaN));
        assertEquals(0.0, RecommendationConstants.clamp(Double.NEGATIVE_INFINITY));
        assertEquals(0.0, RecommendationConstants.clamp(-1.0));
        assertEquals(1.0, RecommendationConstants.clamp(2.0));
        assertTrue(RecommendationConstants.clamp(0.42) == 0.42);
    }
}
