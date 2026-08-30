package com.demo.retrieval.service.grpo;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class GrpoPolicyScorerTest {

    private final ServedMovie movie =
        new ServedMovie("m1", 0.4, 0.3, 0.05, 0.7, false, 10, 2, Map.of("predictionScore", 0.7));

    private GrpoPolicyScorer scorer(String mode, String weights, String version) {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getGrpo().setMode(mode);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations hashOps = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(anyString())).thenReturn(weights == null ? Map.of()
            : Map.of("weights", weights, "feature_version", version, "dim", "10"));
        return new GrpoPolicyScorer(redis, properties);
    }

    private String ones() {
        return String.join(",", java.util.Collections.nCopies(GrpoFeatures.DIM, "1.0"));
    }

    @Test
    void offModeIsDisabledAndScoresZero() {
        GrpoPolicyScorer s = scorer("off", ones(), "v1");
        assertFalse(s.enabled());
        assertEquals(0.0, s.score(movie, 0, 5));
    }

    @Test
    void shadowModeScoresButClaimsNoBlendWeight() {
        GrpoPolicyScorer s = scorer("shadow", ones(), "v1");
        assertTrue(s.enabled());
        assertEquals(0.0, s.blendWeight(), "shadow must never move a recommendation");
        double expected = java.util.Arrays.stream(GrpoFeatures.of(movie, 0, 5)).sum();
        assertEquals(expected, s.score(movie, 0, 5), 1e-9);
    }

    @Test
    void onModeClaimsOnlyTheUnclaimedBlendWeight() {
        GrpoPolicyScorer s = scorer("on", ones(), "v1");
        assertEquals(GrpoPolicyScorer.ON_BLEND_WEIGHT, s.blendWeight());
        // MovieLensOutcomeScorer's weights sum to 0.85; taking more than 0.15 would silently
        // change every existing score.
        assertTrue(s.blendWeight() <= 0.15);
    }

    @Test
    void anUnrecognisedModeIsTreatedAsOff() {
        assertFalse(scorer("enabled", ones(), "v1").enabled());
    }

    @Test
    void weightsOfADifferentFeatureVersionAreIgnored() {
        assertEquals(0.0, scorer("shadow", ones(), "v0").score(movie, 0, 5));
    }

    @Test
    void aMissingWeightVectorScoresZeroRatherThanFailing() {
        assertEquals(0.0, scorer("shadow", null, "v1").score(movie, 0, 5));
    }
}
