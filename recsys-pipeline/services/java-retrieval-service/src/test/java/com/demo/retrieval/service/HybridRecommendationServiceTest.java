package com.demo.retrieval.service;

import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.measurement.RecommendationMeasurementService;
import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.RecommendationResult;
import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.clients.UserMovieHistoryClient.UserMovieHistory;
import com.demo.retrieval.service.clients.UserProfileClient;
import com.demo.retrieval.service.query_hydrators.MovieLensUserHistoryQueryHydrator;
import com.demo.retrieval.service.query_hydrators.UserBehaviorProfileQueryHydrator;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "null"})
class HybridRecommendationServiceTest {
    @Test
    void recommendsFreshMoviesFromMovieLensBehaviorSignals() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        ListOperations<String, String> lists = mock(ListOperations.class);
        SetOperations<String, String> sets = mock(SetOperations.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        ZSetOperations<String, String> sortedSets = mock(ZSetOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        when(redis.opsForList()).thenReturn(lists);
        when(redis.opsForSet()).thenReturn(sets);
        when(redis.opsForValue()).thenReturn(values);
        when(redis.opsForZSet()).thenReturn(sortedSets);
        when(sets.size(any())).thenReturn(1L);

        when(sortedSets.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                ZSetOperations.TypedTuple.of("watched", 100.0),
                ZSetOperations.TypedTuple.of("rated", 90.0),
                ZSetOperations.TypedTuple.of("fresh", 80.0),
                ZSetOperations.TypedTuple.of("unclassified", 1.0)
            )));
        when(values.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> {
                if (key.startsWith("i2vEmb:")) {
                    return "1.0 0.0";
                }
                return "0";
            }).toList();
        });

        RecommendationProperties properties = new RecommendationProperties();
        properties.getCandidateGeneration().setColdStartPoolSize(1);
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("watched", movie("drama"));
        catalog.put("rated", movie("comedy"));
        catalog.put("fresh", movie("sci-fi"));
        properties.setCatalog(catalog);

        FeatureCache featureCache = new FeatureCache(properties);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        HybridRecommendationService service = new HybridRecommendationService(
            redis,
            properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache,
            List.of(new MovieLensUserHistoryQueryHydrator(
                userId -> new UserMovieHistory(List.of("watched"), List.of("rated"))
            )),
            new RecommendationMeasurementService(meterRegistry, properties, featureCache)
        );

        RecommendationResult result = service.recommend("u1", 1);

        assertEquals(List.of("fresh"), result.recommendations());
        assertFalse(result.recommendations().contains("watched"));
        assertFalse(result.recommendations().contains("rated"));
        assertEquals(1.0, meterRegistry.find("recommendation.filter.decisions")
            .tag("reason", "unknown").counter().count());
        for (String stage : List.of("hydration", "redis_fetch", "scoring", "selection", "side_effects")) {
            assertEquals(1L, meterRegistry.find("recommendation.stage.latency").tag("stage", stage).timer().count());
        }
    }

    @Test
    void deriveTasteProfilePullsGenresFromSeedItemsCatalog() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RecommendationProperties properties = new RecommendationProperties();
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("watched", movie("drama"));
        properties.setCatalog(catalog);
        FeatureCache featureCache = new FeatureCache(properties);
        HybridRecommendationService service = new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache, List.of());

        HybridRecommendationService.TasteProfile profile = service.deriveTasteProfile(
            List.of("watched"), List.of(), MovieLensUserFeatures.forUser("u1"), List.of());

        assertTrue(profile.genres().containsKey("drama"));
    }

    @Test
    void deriveTasteProfileKeepsExplicitAndSeedWeightsAheadOfBehavioralPreferences() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RecommendationProperties properties = new RecommendationProperties();
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("watched", movie("sci-fi"));
        properties.setCatalog(catalog);
        FeatureCache featureCache = new FeatureCache(properties);
        HybridRecommendationService service = new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache, List.of());
        MovieLensUserFeatures features = new MovieLensUserFeatures("u1", List.of("sci-fi"), 0.0, 0, List.of())
            .withBehaviorPreferences(Map.of("sci-fi", 0.2, "drama", 0.3), Map.of("space", 0.4));

        HybridRecommendationService.TasteProfile profile = service.deriveTasteProfile(
            List.of("watched"), List.of(), features, List.of());

        assertEquals(1.0, profile.genres().get("sci-fi"));
        assertEquals(0.3, profile.genres().get("drama"));
        assertEquals(0.4, profile.tags().get("space"));
    }

    @Test
    void profiledUserRanksStrongerMatchingGenreAboveTiedPopularity() {
        UserBehaviorProfile profile = new UserBehaviorProfile(
            "u1", 1, "run", "now", null, 1L,
            new UserBehaviorProfile.Preferences(
                List.of(
                    new UserBehaviorProfile.Preference("sci-fi", 0.9, 1L),
                    new UserBehaviorProfile.Preference("drama", 0.3, 1L)
                ),
                List.of()),
            null, List.of());

        assertEquals(List.of("sci-fi", "drama"),
            recommendForTiedGenres(userId -> Optional.of(profile)).recommendations());
    }

    @Test
    void missingBehaviorProfilePreservesTiedPopularityBaselineOrdering() {
        assertEquals(List.of("drama", "sci-fi"),
            recommendForTiedGenres(userId -> Optional.empty()).recommendations());
    }

    private static RecommendationResult recommendForTiedGenres(UserProfileClient profileClient) {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        ListOperations<String, String> lists = mock(ListOperations.class);
        SetOperations<String, String> sets = mock(SetOperations.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        ZSetOperations<String, String> sortedSets = mock(ZSetOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        when(redis.opsForList()).thenReturn(lists);
        when(redis.opsForSet()).thenReturn(sets);
        when(redis.opsForValue()).thenReturn(values);
        when(redis.opsForZSet()).thenReturn(sortedSets);
        when(sets.size(any())).thenReturn(1L);
        when(sortedSets.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                ZSetOperations.TypedTuple.of("drama", 100.0),
                ZSetOperations.TypedTuple.of("sci-fi", 100.0)
            )));
        when(values.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> key.startsWith("i2vEmb:") ? "1.0 0.0" : "0").toList();
        });

        RecommendationProperties properties = new RecommendationProperties();
        properties.getCandidateGeneration().setColdStartPoolSize(2);
        properties.getBandit().setRelevanceWeight(0.0);
        properties.getBandit().setContentWeight(1.0);
        properties.getBandit().setPopularityWeight(0.0);
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("drama", movie("drama"));
        catalog.put("sci-fi", movie("sci-fi"));
        properties.setCatalog(catalog);
        FeatureCache featureCache = new FeatureCache(properties);
        HybridRecommendationService service = new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache), featureCache,
            List.of(new UserBehaviorProfileQueryHydrator(profileClient)));

        return service.recommend("u1", 2);
    }

    private static MovieProfile movie(String genre) {
        MovieProfile profile = new MovieProfile();
        profile.setGenres(List.of(genre));
        profile.setTags(List.of(genre));
        return profile;
    }
}
