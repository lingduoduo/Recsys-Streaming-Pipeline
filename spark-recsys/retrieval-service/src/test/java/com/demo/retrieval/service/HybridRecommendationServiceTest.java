package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.query_hydrators.MovieLensUserHistoryQueryHydrator;
import com.demo.retrieval.service.query_hydrators.QueryHydrator;
import com.demo.retrieval.service.query_hydrators.UserMovieFeaturesQueryHydrator;
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
    private RecommendationProperties properties;

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

        properties = new RecommendationProperties();
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
        service = new HybridRecommendationService(
            redis, properties, predictionService, onlineLearningService, featureCache, hydrators());
    }

    @Test
    void recommendInjectsColdStartCandidatesAndReturnsDiagnostics() {
        // recent items — fetched with RECENT_HISTORY_SIZE window
        when(listOps.range(eq("user:u1:recent"), eq(0L), anyLong())).thenReturn(List.of("item1"));
        when(listOps.range(eq("user:u1:rated"), eq(0L), anyLong())).thenReturn(List.of("item2"));

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
        assertFalse(result.recommendations().contains("item2")); // already rated — must be excluded
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
    void recommendFiltersDuplicatesExpiredMutedAndBlockedCandidates() {
        MovieProfile expired = movie(List.of("sci-fi"), List.of("space"), false);
        expired.setExpiresAtEpochMillis(System.currentTimeMillis() - 1_000L);
        MovieProfile mutedType = movie(List.of("comedy"), List.of("family"), false);
        mutedType.setProductType("ads");
        MovieProfile mutedKeyword = movie(List.of("drama"), List.of("spoiler"), false);
        MovieProfile mutedCoreDataText = movie(List.of("drama"), List.of("quiet"), false);
        mutedCoreDataText.setCoreDataText("contains spoiler from core data");
        MovieProfile mutedLanguage = movie(List.of("drama"), List.of("foreign"), false);
        mutedLanguage.setLanguageCode("es");
        MovieProfile blockedVisibility = movie(List.of("drama"), List.of("unsafe"), false);
        blockedVisibility.setVisibilityReason("safety_drop");
        MovieProfile ancillaryDrop = movie(List.of("drama"), List.of("reply"), false);
        ancillaryDrop.setAncestorMovieIds(List.of("blocked_visibility"));
        MovieProfile quotedBlocked = movie(List.of("drama"), List.of("quote"), false);
        quotedBlocked.setQuotedAuthorBlocksViewer(true);
        quotedBlocked.setHasMedia(true);
        MovieProfile noMedia = movie(List.of("drama"), List.of("plain"), false);
        noMedia.setHasMedia(false);
        MovieProfile blockedAuthor = movie(List.of("drama"), List.of("blocked-author"), false);
        blockedAuthor.setAuthorBlocksViewer(true);
        blockedAuthor.setHasMedia(true);
        MovieProfile allowed = movie(List.of("sci-fi"), List.of("fresh"), true);
        allowed.setHasMedia(true);
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("recent", movie(List.of("action"), List.of("seen"), false));
        catalog.put("expired", expired);
        catalog.put("muted_type", mutedType);
        catalog.put("muted_keyword", mutedKeyword);
        catalog.put("muted_core_text", mutedCoreDataText);
        catalog.put("muted_language", mutedLanguage);
        catalog.put("blocked_visibility", blockedVisibility);
        catalog.put("ancillary_drop", ancillaryDrop);
        catalog.put("quoted_blocked", quotedBlocked);
        catalog.put("no_media", noMedia);
        catalog.put("blocked_author", blockedAuthor);
        catalog.put("allowed", allowed);
        properties.setCatalog(catalog);
        properties.getFiltering().setBlockedUsers(List.of("blocked_user"));
        properties.getFiltering().setMutedProductTypes(List.of("ads"));
        properties.getFiltering().setMutedKeywords(List.of("spoiler"));
        properties.getFiltering().setMutedLanguageCodes(List.of("es"));
        properties.getFiltering().setBlockedVisibilityReasons(List.of("safety_drop"));
        properties.getFiltering().setDropAncillaryCandidates(true);
        properties.getFiltering().setDropBlockedQuotes(true);
        properties.getFiltering().setRequireMediaCandidates(true);
        properties.getFiltering().setDropAuthorsBlockingViewer(true);

        RecommendationResult blocked = service.recommend("blocked_user", 3);
        assertTrue(blocked.recommendations().isEmpty());
        assertEquals("blocked_user", blocked.metrics().get("filterReason"));

        when(listOps.range(eq("user:u2:recent"), eq(0L), anyLong())).thenReturn(List.of("recent"));
        when(zSetOps.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                new DefaultTypedTuple<>("recent", 100.0),
                new DefaultTypedTuple<>("expired", 90.0),
                new DefaultTypedTuple<>("muted_type", 80.0),
                new DefaultTypedTuple<>("muted_keyword", 70.0),
                new DefaultTypedTuple<>("muted_core_text", 65.0),
                new DefaultTypedTuple<>("muted_language", 64.0),
                new DefaultTypedTuple<>("blocked_visibility", 63.0),
                new DefaultTypedTuple<>("ancillary_drop", 62.0),
                new DefaultTypedTuple<>("quoted_blocked", 61.0),
                new DefaultTypedTuple<>("no_media", 60.5),
                new DefaultTypedTuple<>("blocked_author", 60.25),
                new DefaultTypedTuple<>("allowed", 60.0),
                new DefaultTypedTuple<>("allowed", 50.0)
            )));
        when(valueOps.get("uEmb:u2")).thenReturn(null);
        when(valueOps.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> switch (key) {
                case "i2vEmb:recent", "i2vEmb:allowed" -> "1.0 0.0";
                case "bandit:item:allowed:impressions", "bandit:item:allowed:clicks" -> "0";
                default -> null;
            }).toList();
        });

        RecommendationResult result = service.recommend("u2", 5);

        assertEquals(List.of("allowed"), result.recommendations());
        assertEquals(1, result.metrics().get("eligibleCandidateCount"));
    }

    @Test
    void recommendUsesHydratedUserFeaturesForColdStartAndExclusion() {
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("recently_rated", movie(List.of("sci-fi"), List.of("space"), false));
        catalog.put("sci_fi_candidate", movie(List.of("sci-fi"), List.of("future"), true));
        catalog.put("drama_candidate", movie(List.of("drama"), List.of("slow"), true));
        properties.setCatalog(catalog);

        when(listOps.range(eq("user:u3:recent"), eq(0L), anyLong())).thenReturn(List.of());
        when(listOps.range(eq("user:u3:rated"), eq(0L), anyLong())).thenReturn(List.of());
        when(hashOps.entries("user:u3:features")).thenReturn(Map.of(
            "favoriteGenres", "sci-fi, adventure",
            "avgRating", "4.25",
            "ratingCount", "12",
            "recentlyRatedMovieIds", "recently_rated"
        ));
        when(zSetOps.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                new DefaultTypedTuple<>("recently_rated", 100.0),
                new DefaultTypedTuple<>("drama_candidate", 20.0)
            )));
        when(valueOps.get("uEmb:u3")).thenReturn(null);
        when(valueOps.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> switch (key) {
                case "i2vEmb:sci_fi_candidate" -> "1.0 0.0";
                case "i2vEmb:drama_candidate" -> "0.0 1.0";
                case "bandit:item:sci_fi_candidate:impressions", "bandit:item:sci_fi_candidate:clicks",
                     "bandit:item:drama_candidate:impressions", "bandit:item:drama_candidate:clicks" -> "0";
                default -> null;
            }).toList();
        });

        RecommendationResult result = service.recommend("u3", 2);

        assertTrue(result.recommendations().contains("sci_fi_candidate"));
        assertFalse(result.recommendations().contains("recently_rated"));
    }

    @Test
    void recommendPreRanksSubscribedAuthorCandidatesFromHydrator() {
        properties.getCandidateGeneration().setCandidatePoolSize(1);

        MovieProfile popular = movie(List.of("drama"), List.of("popular"), false);
        MovieProfile subscribed = movie(List.of("drama"), List.of("creator"), false);
        subscribed.setSubscriptionAuthorId("creator1");
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("popular", popular);
        catalog.put("subscribed", subscribed);
        properties.setCatalog(catalog);

        when(listOps.range(eq("user:u_sub:recent"), eq(0L), anyLong())).thenReturn(List.of());
        when(listOps.range(eq("user:u_sub:rated"), eq(0L), anyLong())).thenReturn(List.of());
        when(hashOps.entries("user:u_sub:features")).thenReturn(Map.of("subscribedUserIds", "creator1"));
        when(zSetOps.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                new DefaultTypedTuple<>("popular", 100.0),
                new DefaultTypedTuple<>("subscribed", 99.0)
            )));
        when(valueOps.get("uEmb:u_sub")).thenReturn(null);
        when(valueOps.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> switch (key) {
                case "i2vEmb:popular", "i2vEmb:subscribed" -> "1.0 0.0";
                case "bandit:item:subscribed:impressions", "bandit:item:subscribed:clicks" -> "0";
                default -> null;
            }).toList();
        });

        RecommendationResult result = service.recommend("u_sub", 1);

        assertEquals(List.of("subscribed"), result.recommendations());
    }

    @Test
    void recommendPreRanksMutualFollowJaccardCandidatesFromHydrator() {
        properties.getCandidateGeneration().setCandidatePoolSize(1);

        MovieProfile popular = movie(List.of("drama"), List.of("popular"), false);
        MovieProfile mutual = movie(List.of("drama"), List.of("mutual"), false);
        mutual.setMutualFollowJaccard(1.0);
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("popular", popular);
        catalog.put("mutual", mutual);
        properties.setCatalog(catalog);

        when(listOps.range(eq("user:u_mutual:recent"), eq(0L), anyLong())).thenReturn(List.of());
        when(listOps.range(eq("user:u_mutual:rated"), eq(0L), anyLong())).thenReturn(List.of());
        when(hashOps.entries("user:u_mutual:features")).thenReturn(Map.of());
        when(zSetOps.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                new DefaultTypedTuple<>("popular", 100.0),
                new DefaultTypedTuple<>("mutual", 99.0)
            )));
        when(valueOps.get("uEmb:u_mutual")).thenReturn(null);
        when(valueOps.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> switch (key) {
                case "i2vEmb:popular", "i2vEmb:mutual" -> "1.0 0.0";
                case "bandit:item:mutual:impressions", "bandit:item:mutual:clicks" -> "0";
                default -> null;
            }).toList();
        });

        RecommendationResult result = service.recommend("u_mutual", 1);

        assertEquals(List.of("mutual"), result.recommendations());
    }

    @Test
    void recommendPreRanksContextHydratedCandidates() {
        properties.getCandidateGeneration().setCandidatePoolSize(1);

        MovieProfile popular = movie(List.of("drama"), List.of("popular"), false);
        MovieProfile contextual = movie(List.of("drama"), List.of("contextual"), false);
        contextual.setFavoriteCount(1_000L);
        contextual.setReplyCount(100L);
        contextual.setFilteredTopicIds(List.of(42));
        contextual.setInNetwork(true);
        contextual.setAuthorFollowersCount(100_000);
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("popular", popular);
        catalog.put("contextual", contextual);
        properties.setCatalog(catalog);

        when(listOps.range(eq("user:u_context:recent"), eq(0L), anyLong())).thenReturn(List.of());
        when(listOps.range(eq("user:u_context:rated"), eq(0L), anyLong())).thenReturn(List.of());
        when(hashOps.entries("user:u_context:features")).thenReturn(Map.of("followedGrokTopics", "42"));
        when(zSetOps.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                new DefaultTypedTuple<>("popular", 100.0),
                new DefaultTypedTuple<>("contextual", 99.0)
            )));
        when(valueOps.get("uEmb:u_context")).thenReturn(null);
        when(valueOps.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> switch (key) {
                case "i2vEmb:popular", "i2vEmb:contextual" -> "1.0 0.0";
                case "bandit:item:contextual:impressions", "bandit:item:contextual:clicks" -> "0";
                default -> null;
            }).toList();
        });

        RecommendationResult result = service.recommend("u_context", 1);

        assertEquals(List.of("contextual"), result.recommendations());
    }

    @Test
    void coldStartGenerationFetchesImpressionsOnlyForBoundedProbeSet() {
        properties.getCandidateGeneration().setColdStartPoolSize(2);
        properties.getCandidateGeneration().setPopularityFetchMultiplier(3);

        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        for (int i = 0; i < 20; i++) {
            catalog.put("item_" + i, movie(List.of("sci-fi"), List.of("space"), false));
        }
        properties.setCatalog(catalog);

        when(listOps.range(eq("user:u4:recent"), eq(0L), anyLong())).thenReturn(List.of());
        when(listOps.range(eq("user:u4:rated"), eq(0L), anyLong())).thenReturn(List.of());
        when(hashOps.entries("user:u4:features")).thenReturn(Map.of("favoriteGenres", "sci-fi"));
        when(zSetOps.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(Set.of());
        when(valueOps.get("uEmb:u4")).thenReturn(null);
        when(valueOps.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> {
                if (key.startsWith("i2vEmb:")) {
                    return "1.0 0.0";
                }
                return "0";
            }).toList();
        });

        RecommendationResult result = service.recommend("u4", 2);

        assertEquals(2, result.recommendations().size());
        ArgumentCaptor<List<String>> keysCaptor = ArgumentCaptor.forClass(List.class);
        verify(valueOps, atLeastOnce()).multiGet(keysCaptor.capture());
        List<List<String>> coldStartImpressionFetches = keysCaptor.getAllValues().stream()
            .filter(keys -> keys.stream().allMatch(key ->
                key.startsWith("bandit:item:") && key.endsWith(":impressions")))
            .toList();
        assertEquals(1, coldStartImpressionFetches.size());
        assertEquals(6, coldStartImpressionFetches.get(0).size());
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
            new OnlineLearningService(redis, properties, localCache), localCache, hydrators());

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
            new OnlineLearningService(redis, properties, localCache), localCache, hydrators());

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
            new OnlineLearningService(redis, properties, localCache), localCache, hydrators());
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
            new OnlineLearningService(redis, properties, localCache), localCache, hydrators());
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

    private List<QueryHydrator<ScoredMoviesQuery>> hydrators() {
        return List.of(
            new MovieLensUserHistoryQueryHydrator(new RedisUserMovieHistoryClient(redis)),
            new UserMovieFeaturesQueryHydrator(new RedisMovieLensFeatureClient(redis))
        );
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
