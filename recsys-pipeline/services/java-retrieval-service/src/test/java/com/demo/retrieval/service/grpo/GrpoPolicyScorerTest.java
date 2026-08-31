package com.demo.retrieval.service.grpo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class GrpoPolicyScorerTest {

    private final ServedMovie movie =
        new ServedMovie("m1", 0.4, 0.3, 0.05, 0.7, false, 10, 2, Map.of("predictionScore", 0.7));

    private StringRedisTemplate lastRedis;

    private GrpoPolicyScorer scorer(String mode, String weights, String version) {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getGrpo().setMode(mode);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations hashOps = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(anyString())).thenReturn(weights == null ? Map.of()
            : Map.of("weights", weights, "feature_version", version, "dim", "10"));
        lastRedis = redis;
        return new GrpoPolicyScorer(redis, properties);
    }

    /**
     * Distinct weights, not all-ones: with a uniform vector the dot product collapses to the sum of
     * the features and any permutation of weight-to-feature indices scores identically, so the test
     * could not see a misaligned loop.
     */
    private String distinctWeights() {
        return java.util.stream.IntStream.rangeClosed(1, GrpoFeatures.DIM)
            .mapToObj(i -> Double.toString((double) i))
            .collect(Collectors.joining(","));
    }

    /**
     * Isolates feature 1 (banditScore) so a slate's GRPO order is exactly its banditScore order.
     * With the 1..10 vector, feature 8 (position/slateSize) carries weight 9 and would dominate,
     * making every slate score monotonically increasing in position regardless of the items.
     */
    private String weightsOnBanditScoreOnly() {
        String[] w = new String[GrpoFeatures.DIM];
        java.util.Arrays.fill(w, "0.0");
        w[1] = "1.0";
        return String.join(",", w);
    }

    private double expectedScore(int position, int slateSize) {
        double[] x = GrpoFeatures.of(movie, position, slateSize);
        double sum = 0.0;
        for (int i = 0; i < GrpoFeatures.DIM; i++) {
            sum += (i + 1) * x[i];
        }
        return sum;
    }

    private ServingSideEffectRequest slate(List<ServedMovie> selected) {
        return new ServingSideEffectRequest(
            "req-1", "u1", "ucb", Map.of(), selected, selected,
            List.of(), List.of(), 0, 0, selected.size(), 0.0, 0.0, 0.0);
    }

    private ServedMovie itemWithBanditScore(String id, double banditScore) {
        return new ServedMovie(id, 0.0, 0.0, 0.0, banditScore, false, 0, 0, Map.of());
    }

    /** Captures what GrpoPolicyScorer logs while the block runs. */
    private List<String> capturingLogs(Consumer<Void> block) {
        ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GrpoPolicyScorer.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            block.accept(null);
        } finally {
            logger.detachAppender(appender);
        }
        return appender.list.stream()
            .filter(e -> e.getLevel() == Level.INFO)
            .map(ILoggingEvent::getFormattedMessage)
            .toList();
    }

    @Test
    void offModeIsDisabledAndScoresZero() {
        GrpoPolicyScorer s = scorer("off", distinctWeights(), "v1");
        assertFalse(s.enabled());
        assertEquals(0.0, s.score(movie, 0, 5));
    }

    @Test
    void offModeReadsNothingFromRedis() {
        // The rollout's first promise: with the flag off, the serving path pays nothing at all.
        GrpoPolicyScorer s = scorer("off", distinctWeights(), "v1");
        s.score(movie, 0, 5);
        s.recordShadowSlate(slate(List.of(itemWithBanditScore("m1", 0.9), itemWithBanditScore("m2", 0.1))));
        verify(lastRedis, never()).opsForHash();
    }

    @Test
    void shadowModeScoresButClaimsNoBlendWeight() {
        GrpoPolicyScorer s = scorer("shadow", distinctWeights(), "v1");
        assertTrue(s.enabled());
        assertEquals(0.0, s.blendWeight(), "shadow must never move a recommendation");
        assertEquals(expectedScore(0, 5), s.score(movie, 0, 5), 1e-9);
    }

    @Test
    void scoreUsesEachWeightAgainstItsOwnFeature() {
        // Position is the only input that differs, so a score that ignored the weight/feature
        // pairing on feature 8 would return the same number for both positions.
        GrpoPolicyScorer s = scorer("shadow", distinctWeights(), "v1");
        assertEquals(expectedScore(3, 5), s.score(movie, 3, 5), 1e-9);
        assertEquals(expectedScore(0, 5), s.score(movie, 0, 5), 1e-9);
    }

    @Test
    void onModeClaimsOnlyTheUnclaimedBlendWeight() {
        GrpoPolicyScorer s = scorer("on", distinctWeights(), "v1");
        assertEquals(GrpoPolicyScorer.ON_BLEND_WEIGHT, s.blendWeight());
        // MovieLensOutcomeScorer's weights sum to 0.85; taking more than 0.15 would silently
        // change every existing score.
        assertTrue(s.blendWeight() <= 0.15);
    }

    @Test
    void anUnrecognisedModeIsTreatedAsOff() {
        assertFalse(scorer("enabled", distinctWeights(), "v1").enabled());
    }

    @Test
    void weightsOfADifferentFeatureVersionAreIgnored() {
        assertEquals(0.0, scorer("shadow", distinctWeights(), "v0").score(movie, 0, 5));
    }

    @Test
    void aMissingWeightVectorScoresZeroRatherThanFailing() {
        assertEquals(0.0, scorer("shadow", null, "v1").score(movie, 0, 5));
    }

    @Test
    void aNonFiniteWeightIsUnusable() {
        // Double.parseDouble accepts "NaN": without the finite check the score would be NaN, which
        // is worse than no score at all because it sorts unpredictably.
        String withNaN = "NaN," + String.join(",", java.util.Collections.nCopies(GrpoFeatures.DIM - 1, "1.0"));
        assertEquals(0.0, scorer("shadow", withNaN, "v1").score(movie, 0, 5));
        String withInf = "Infinity," + String.join(",", java.util.Collections.nCopies(GrpoFeatures.DIM - 1, "1.0"));
        assertEquals(0.0, scorer("shadow", withInf, "v1").score(movie, 0, 5));
    }

    @Test
    void pairwiseConcordanceIsOneWhenTheScoresReproduceTheServedOrder() {
        assertEquals(1.0, GrpoPolicyScorer.pairwiseConcordance(new double[] {3.0, 2.0, 1.0}), 1e-9);
    }

    @Test
    void pairwiseConcordanceIsZeroOnAReversedOrder() {
        assertEquals(0.0, GrpoPolicyScorer.pairwiseConcordance(new double[] {1.0, 2.0, 3.0}), 1e-9);
    }

    @Test
    void pairwiseConcordanceCountsOnlyStrictlyOrderedPairs() {
        // One of three pairs agrees: (0,1) ties, (0,2) and (1,2) — 2.0 > 1.0 for both.
        assertEquals(2.0 / 3.0, GrpoPolicyScorer.pairwiseConcordance(new double[] {2.0, 2.0, 1.0}), 1e-9);
        // A tie is not agreement: the served order picked a winner and the policy did not.
        assertEquals(0.0, GrpoPolicyScorer.pairwiseConcordance(new double[] {1.0, 1.0}), 1e-9);
    }

    @Test
    void shadowModeLogsOneLinePerSlate() {
        GrpoPolicyScorer s = scorer("shadow", weightsOnBanditScoreOnly(), "v1");
        // Descending banditScore under a banditScore-only weight vector: the GRPO order reproduces
        // the served order exactly, so concordance is 1.0.
        List<String> lines = capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9),
            itemWithBanditScore("m2", 0.5),
            itemWithBanditScore("m3", 0.1)))));
        assertEquals(1, lines.size(), "one line per slate, not one per item");
        assertTrue(lines.get(0).contains("requestId=req-1"), lines.get(0));
        assertTrue(lines.get(0).contains("slateSize=3"), lines.get(0));
        assertTrue(lines.get(0).contains("pairwiseConcordance=1.0"), lines.get(0));
    }

    @Test
    void shadowModeLogsADisagreeingSlateAsSuch() {
        GrpoPolicyScorer s = scorer("shadow", weightsOnBanditScoreOnly(), "v1");
        List<String> lines = capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.1),
            itemWithBanditScore("m2", 0.9)))));
        assertTrue(lines.get(0).contains("pairwiseConcordance=0.0"), lines.get(0));
    }

    @Test
    void offModeLogsNothing() {
        GrpoPolicyScorer s = scorer("off", distinctWeights(), "v1");
        assertTrue(capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9),
            itemWithBanditScore("m2", 0.1))))).isEmpty());
    }

    @Test
    void onModeLogsNothing() {
        // In `on` the score already drives the order it would be graded against.
        GrpoPolicyScorer s = scorer("on", distinctWeights(), "v1");
        assertTrue(capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9),
            itemWithBanditScore("m2", 0.1))))).isEmpty());
    }

    @Test
    void aSlateWithNoUsableWeightsLogsNothing() {
        GrpoPolicyScorer s = scorer("shadow", null, "v1");
        assertTrue(capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9),
            itemWithBanditScore("m2", 0.1))))).isEmpty());
    }

    @Test
    void aSingleItemSlateLogsNothing() {
        GrpoPolicyScorer s = scorer("shadow", weightsOnBanditScoreOnly(), "v1");
        assertTrue(capturingLogs(ignored ->
            s.recordShadowSlate(slate(List.of(itemWithBanditScore("m1", 0.9))))).isEmpty());
    }

    @Test
    void aFailingSlateNeverThrowsIntoTheServingPath() {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getGrpo().setMode("shadow");
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForHash()).thenThrow(new IllegalStateException("redis down"));
        GrpoPolicyScorer s = new GrpoPolicyScorer(redis, properties);
        s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9), itemWithBanditScore("m2", 0.1))));
    }
}
