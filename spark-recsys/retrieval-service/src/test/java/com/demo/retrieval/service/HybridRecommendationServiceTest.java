package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.mockito.ArgumentCaptor;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "null"})
class HybridRecommendationServiceTest {
    private StringRedisTemplate redis;
    private ValueOperations<String, String> valueOps;
    private ListOperations<String, String> listOps;
    private ZSetOperations<String, String> zSetOps;
    private SetOperations<String, String> setOps;
    private HashOperations<String, Object, Object> hashOps;
    private HybridRecommendationService service;

    @BeforeEach
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        listOps = mock(ListOperations.class);
        zSetOps = mock(ZSetOperations.class);
        setOps = mock(SetOperations.class);
        hashOps = mock(HashOperations.class);

        when(redis.opsForValue()).thenReturn(valueOps);
        when(redis.opsForList()).thenReturn(listOps);
        when(redis.opsForZSet()).thenReturn(zSetOps);
        when(redis.opsForSet()).thenReturn(setOps);
        when(redis.opsForHash()).thenReturn(hashOps);
        when(setOps.size(anyString())).thenReturn(1L);
        when(hashOps.get(anyString(), anyString())).thenAnswer(invocation -> switch (invocation.getArgument(0).toString()) {
            case "bandit:metrics:ucb" -> "10";
            case "bandit:metrics:thompson" -> "7";
            default -> "17";
        });
        when(hashOps.entries(anyString())).thenAnswer(invocation -> switch (invocation.getArgument(0).toString()) {
            case "bandit:metrics" -> Map.of(
                "requests", "5",
                "recommendations_served", "9",
                "clicks", "3",
                "pseudo_regret_total", "0.5",
                "reward_total", "2.0",
                "estimated_reward_total", "5.0",
                "novelty_total", "2.4",
                "cold_start_impressions", "2",
                "exploratory_impressions", "4"
            );
            case "bandit:metrics:ucb" -> Map.of(
                "requests", "2",
                "recommendations_served", "4",
                "clicks", "1",
                "pseudo_regret_total", "0.2",
                "reward_total", "1.0",
                "estimated_reward_total", "2.5",
                "novelty_total", "1.2",
                "cold_start_impressions", "1",
                "exploratory_impressions", "2"
            );
            case "bandit:metrics:thompson" -> Map.of(
                "requests", "3",
                "recommendations_served", "5",
                "clicks", "2",
                "pseudo_regret_total", "0.3",
                "reward_total", "1.0",
                "estimated_reward_total", "2.5",
                "novelty_total", "1.2",
                "cold_start_impressions", "1",
                "exploratory_impressions", "2"
            );
            default -> Map.of();
        });
        // Execute the SessionCallback against the same mock so individual operation
        // verify() calls work after recordFeedback moves all writes into the pipeline.
        when(redis.executePipelined(any(SessionCallback.class))).thenAnswer(invocation -> {
            SessionCallback<?> cb = invocation.getArgument(0);
            cb.execute(redis);
            return List.of();
        });

        RecommendationProperties properties = new RecommendationProperties();
        properties.getCandidateGeneration().setTopNRandomizationPool(1);
        properties.getCandidateGeneration().setColdStartPoolSize(10);
        properties.getCandidateGeneration().setPopularityFetchMultiplier(3);
        properties.getBandit().setColdStartExposureThreshold(3);
        properties.getBandit().setAlgorithm("ucb");

        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("item1", movie(List.of("sci-fi", "adventure"), List.of("space"), false));
        catalog.put("item2", movie(List.of("action"), List.of("crime"), false));
        catalog.put("item4", movie(List.of("sci-fi", "thriller"), List.of("future", "ai"), true));
        catalog.put("item7", movie(List.of("comedy"), List.of("robots"), true));
        properties.setCatalog(catalog);

        FeatureCache featureCache = new FeatureCache(properties);
        OnlineLearningService onlineLearningService = new OnlineLearningService(redis, properties, featureCache);
        DeepLearningPredictionService predictionService = mock(DeepLearningPredictionService.class);
        when(predictionService.predict(anyString(), anyString())).thenReturn(Optional.empty());
        service = new HybridRecommendationService(redis, properties, predictionService, onlineLearningService, featureCache);
    }

    @Test
    void recommendInjectsColdStartCandidatesAndReturnsDiagnostics() {
        // recent items — fetched with RECENT_HISTORY_SIZE window
        when(listOps.range(eq("user:u1:recent"), eq(0L), anyLong())).thenReturn(List.of("item1"));

        // popularity — single ZREVRANGE WITHSCORES call
        Set<ZSetOperations.TypedTuple<String>> popularTuples = new LinkedHashSet<>(List.of(
            new DefaultTypedTuple<>("item2", 10.0),
            new DefaultTypedTuple<>("item4", 2.0)
        ));
        when(zSetOps.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(popularTuples);

        // user vector — individual GET; no direct embedding stored, triggers recent-items fallback
        when(valueOps.get("uEmb:u1")).thenReturn(null);

        // item vectors served via MGET (batch warm) + bandit counters via MGET (batchFetchCounters)
        when(valueOps.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> switch (key) {
                case "i2vEmb:item1" -> "1.0 0.0";
                case "i2vEmb:item2" -> "0.0 1.0";
                case "i2vEmb:item4" -> "1.0 0.0";
                case "i2vEmb:item7" -> "0.7 0.1";
                case "bandit:item:item2:impressions" -> "12";
                case "bandit:item:item2:clicks" -> "3";
                case "bandit:item:item4:impressions" -> "0";
                case "bandit:item:item4:clicks" -> "0";
                case "bandit:item:item7:impressions" -> "1";
                case "bandit:item:item7:clicks" -> "0";
                default -> null;
            }).toList();
        });

        RecommendationResult result = service.recommend("u1", 2);

        assertEquals("u1", result.user());
        assertEquals(List.of("item1"), result.recent());
        assertEquals(2, result.recommendations().size());
        assertTrue(result.recommendations().contains("item4"));
        assertTrue(result.recommendations().contains("item7"));
        assertFalse(result.recommendations().contains("item1")); // recently viewed — must be excluded
        assertFalse(result.candidateDiagnostics().isEmpty());
        assertTrue(result.candidateDiagnostics().get(0).containsKey("rewardModelScore"));
        assertEquals("ucb", result.metrics().get("algorithm"));
        // reward-stat warm + tracking writes each use executePipelined
        verify(redis, atLeastOnce()).executePipelined(any(SessionCallback.class));

        ArgumentCaptor<String> pendingPayload = ArgumentCaptor.forClass(String.class);
        verify(valueOps, atLeastOnce()).set(anyString(), pendingPayload.capture());
        assertTrue(pendingPayload.getAllValues().stream().anyMatch(payload ->
            payload.contains("\"type\":\"rl_experience\"") &&
                payload.contains("\"state\"") &&
                payload.contains("\"actionSpace\"") &&
                payload.contains("\"modelPredictions\"") &&
                payload.contains("\"policy\"")
        ));
    }

    @Test
    void feedbackAggregatesBusinessMetrics() {
        when(valueOps.get("replay:pending:u1:item4")).thenReturn("""
            {"user":"u1","context":{"recent":["item1"]},"candidates":["item4","item2"],"action":"item4"}
            """);

        Map<String, Object> result = service.recordFeedback(new FeedbackRequest("u1", "item4", true, 1.0));

        assertEquals("ok", result.get("status"));
        assertEquals(Boolean.TRUE, result.get("clicked"));
        assertTrue(result.containsKey("metrics"));
        verify(hashOps).increment("bandit:metrics", "clicks", 1L);
        verify(hashOps).increment("bandit:metrics:ucb", "clicks", 1L);
        verify(valueOps).increment("bandit:item:item4:clicks", 1);
        verify(hashOps).increment("bandit:metrics", "reward_total", 1.0);
        verify(hashOps).increment("bandit:metrics:ucb", "reward_total", 1.0);
        verify(hashOps).increment("reward-model:global", "count", 1L);
        verify(hashOps).increment("reward-model:item:item4", "reward_total", 1.0);
        verify(hashOps).increment("reward-model:genre:sci-fi", "reward_total", 1.0);
        verify(hashOps).increment("reward-model:tag:future", "reward_total", 1.0);
        verify(listOps).trim("replay:recommendations", -10000L, -1L);

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(listOps).rightPush(eq("replay:recommendations"), payload.capture());
        assertTrue(payload.getValue().contains("\"reward\":1.0"));
        assertTrue(payload.getValue().contains("\"clicked\":true"));
        assertTrue(payload.getValue().contains("\"nextState\""));
    }

    @Test
    void feedbackAppliesQLearningTemporalDifferenceUpdate() {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getBandit().setAlgorithm("q-learning");
        properties.getBandit().setQLearningAlpha(0.1);
        properties.getBandit().setQLearningGamma(0.9);

        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("item1", movie(List.of("sci-fi"), List.of("space"), false));
        catalog.put("item4", movie(List.of("sci-fi", "thriller"), List.of("future"), true));
        properties.setCatalog(catalog);

        FeatureCache localCache = new FeatureCache(properties);
        HybridRecommendationService localService = new HybridRecommendationService(
            redis, properties, mock(DeepLearningPredictionService.class),
            new OnlineLearningService(redis, properties, localCache), localCache);

        when(valueOps.get("replay:pending:u1:item4")).thenReturn("""
            {"type":"rl_experience","user":"u1","state":{"recent":["item1"],"genres":["sci-fi"],"tags":["space"]},"action":"item4"}
            """);
        when(listOps.range(eq("user:u1:recent"), eq(0L), anyLong())).thenReturn(List.of("item1"));
        when(hashOps.get(anyString(), eq("item4"))).thenAnswer(invocation ->
            invocation.getArgument(0).toString().startsWith("q-learning:q:") ? "0.2" : "0"
        );
        when(hashOps.entries(anyString())).thenAnswer(invocation ->
            invocation.getArgument(0).toString().startsWith("q-learning:q:")
                ? Map.of("item2", "0.5", "item4", "0.4")
                : Map.of()
        );

        localService.recordFeedback(new FeedbackRequest("u1", "item4", true, 1.0));

        ArgumentCaptor<String> qKey = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> qValue = ArgumentCaptor.forClass(String.class);
        verify(hashOps).put(qKey.capture(), eq("item4"), qValue.capture());
        assertTrue(qKey.getValue().startsWith("q-learning:q:"));
        assertEquals(0.325, Double.parseDouble(qValue.getValue()), 1.0e-9);
        verify(hashOps).increment("bandit:metrics:q-learning", "q_updates", 1L);
    }

    @Test
    void feedbackAppliesSarsaTemporalDifferenceUpdateWithPolicyNextAction() {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getBandit().setAlgorithm("sarsa");
        properties.getBandit().setQLearningAlpha(0.1);
        properties.getBandit().setQLearningGamma(0.9);
        properties.getBandit().setQLearningEpsilon(0.0);

        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("item1", movie(List.of("sci-fi"), List.of("space"), false));
        catalog.put("item2", movie(List.of("drama"), List.of("slow"), false));
        catalog.put("item4", movie(List.of("sci-fi", "thriller"), List.of("future"), true));
        properties.setCatalog(catalog);

        FeatureCache localCache = new FeatureCache(properties);
        HybridRecommendationService localService = new HybridRecommendationService(
            redis, properties, mock(DeepLearningPredictionService.class),
            new OnlineLearningService(redis, properties, localCache), localCache);

        when(valueOps.get("replay:pending:u1:item4")).thenReturn("""
            {"type":"rl_experience","user":"u1","state":{"recent":["item1"],"genres":["sci-fi"],"tags":["space"]},"action":"item4"}
            """);
        when(listOps.range(eq("user:u1:recent"), eq(0L), anyLong())).thenReturn(List.of("item1"));
        when(hashOps.get(anyString(), eq("item4"))).thenAnswer(invocation ->
            invocation.getArgument(0).toString().startsWith("sarsa:q:") ? "0.2" : "0"
        );
        when(hashOps.entries(anyString())).thenAnswer(invocation ->
            invocation.getArgument(0).toString().startsWith("sarsa:q:")
                ? Map.of("item2", "0.3", "item4", "0.7")
                : Map.of()
        );

        localService.recordFeedback(new FeedbackRequest("u1", "item4", true, 1.0));

        ArgumentCaptor<String> qKey = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> qValue = ArgumentCaptor.forClass(String.class);
        verify(hashOps).put(qKey.capture(), eq("item4"), qValue.capture());
        assertTrue(qKey.getValue().startsWith("sarsa:q:"));
        assertEquals(0.343, Double.parseDouble(qValue.getValue()), 1.0e-9);
        verify(hashOps).increment("bandit:metrics:sarsa", "q_updates", 1L);

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(listOps).rightPush(eq("replay:recommendations"), payload.capture());
        assertTrue(payload.getValue().contains("\"nextAction\":\"item4\""));
    }

    @Test
    void aggregateMetricsExposePerAlgorithmBreakdown() {
        Map<String, Object> metrics = service.getAggregateMetrics();

        assertEquals("ucb", metrics.get("algorithm"));
        assertEquals(2L, metrics.get("requests"));
        assertEquals(0.2, metrics.get("cumulativePseudoRegret"));
        Map<String, Object> allAlgorithms = (Map<String, Object>) metrics.get("allAlgorithms");
        assertTrue(allAlgorithms.containsKey("ucb"));
        assertTrue(allAlgorithms.containsKey("thompson"));
        Map<String, Object> thompsonMetrics = (Map<String, Object>) allAlgorithms.get("thompson");
        assertEquals(3L, thompsonMetrics.get("requests"));
        Map<String, Object> globalMetrics = (Map<String, Object>) metrics.get("global");
        assertEquals("all", globalMetrics.get("algorithm"));
        assertEquals(5L, globalMetrics.get("requests"));
    }

    @Test
    void ucbExplorationBonusMatchesConfiguredFormula() throws Exception {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getBandit().setAlgorithm("ucb");
        properties.getBandit().setExplorationAlpha(0.75);
        properties.getBandit().setColdStartBoost(1.35);
        properties.getBandit().setMaxExplorationBonus(1.0);

        FeatureCache localCache = new FeatureCache(properties);
        HybridRecommendationService localService = new HybridRecommendationService(
            redis, properties, mock(DeepLearningPredictionService.class),
            new OnlineLearningService(redis, properties, localCache), localCache);
        Object armScore = invokeBanditArmScore(localService, 0.6, 4L, 1L, 100L, true);
        double posteriorMean = invokeArmScoreDouble(armScore, "posteriorMean");
        double bonus = invokeArmScoreDouble(armScore, "explorationBonus");
        double rankingScore = invokeArmScoreDouble(armScore, "rankingScore");

        double priorStrength = Math.max(2.0, 1.35 * 4.0);
        double posteriorAlpha = 1.0 + (0.6 * priorStrength) + 1.0;
        double posteriorBeta = 1.0 + (0.4 * priorStrength) + 3.0;
        double expectedMean = posteriorAlpha / (posteriorAlpha + posteriorBeta);
        double expectedBonus = Math.min(0.75 * Math.sqrt(Math.log(102.0) / (2.0 * (4.0 + priorStrength + 1.0))) * 1.35, 1.0);

        assertEquals(expectedMean, posteriorMean, 1.0e-9);
        assertEquals(expectedBonus, bonus, 1.0e-9);
        assertEquals(expectedMean + expectedBonus, rankingScore, 1.0e-9);
    }

    @Test
    void thompsonSamplingBonusStaysWithinBoundsAndProducesSamples() throws Exception {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getBandit().setAlgorithm("thompson");
        properties.getBandit().setExplorationAlpha(0.75);
        properties.getBandit().setMaxExplorationBonus(0.25);

        FeatureCache localCache = new FeatureCache(properties);
        HybridRecommendationService localService = new HybridRecommendationService(
            redis, properties, mock(DeepLearningPredictionService.class),
            new OnlineLearningService(redis, properties, localCache), localCache);
        Object firstArmScore = invokeBanditArmScore(localService, 0.6, 4L, 1L, 100L, true);
        double first = invokeArmScoreDouble(firstArmScore, "rankingScore");
        double firstBonus = invokeArmScoreDouble(firstArmScore, "explorationBonus");
        double posteriorMean = invokeArmScoreDouble(firstArmScore, "posteriorMean");
        boolean sawDifferentSample = false;

        for (int i = 0; i < 20; i++) {
            Object armScore = invokeBanditArmScore(localService, 0.6, 4L, 1L, 100L, true);
            double sample = invokeArmScoreDouble(armScore, "rankingScore");
            double bonus = invokeArmScoreDouble(armScore, "explorationBonus");
            assertTrue(sample >= 0.0);
            assertTrue(sample <= 1.0);
            assertTrue(bonus >= 0.0);
            assertTrue(bonus <= 0.25);
            if (Double.compare(first, sample) != 0) {
                sawDifferentSample = true;
            }
        }

        assertTrue(posteriorMean > 0.0);
        assertTrue(firstBonus <= 0.25);
        assertTrue(sawDifferentSample, "expected Thompson sampling to produce non-identical draws");
        assertNotEquals(0.0, first);
    }

    private MovieProfile movie(List<String> genres, List<String> tags, boolean newRelease) {
        MovieProfile profile = new MovieProfile();
        profile.setGenres(genres);
        profile.setTags(tags);
        profile.setNewRelease(newRelease);
        return profile;
    }

    private Object invokeBanditArmScore(
        HybridRecommendationService localService,
        double baseScore,
        long itemImpressions,
        long clicks,
        long totalImpressions,
        boolean coldStart
    ) throws Exception {
        Method method = HybridRecommendationService.class.getDeclaredMethod(
            "computeBanditArmScore",
            double.class,
            long.class,
            long.class,
            long.class,
            boolean.class
        );
        method.setAccessible(true);
        return method.invoke(localService, baseScore, itemImpressions, clicks, totalImpressions, coldStart);
    }

    private double invokeArmScoreDouble(Object armScore, String methodName) throws Exception {
        Method accessor = armScore.getClass().getDeclaredMethod(methodName);
        accessor.setAccessible(true);
        return (double) accessor.invoke(armScore);
    }
}
