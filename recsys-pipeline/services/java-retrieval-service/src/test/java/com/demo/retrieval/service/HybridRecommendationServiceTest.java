package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.clients.UserMovieHistoryClient.UserMovieHistory;
import com.demo.retrieval.service.query_hydrators.MovieLensUserHistoryQueryHydrator;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
                ZSetOperations.TypedTuple.of("fresh", 80.0)
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
        properties.getCandidateGeneration().setTopNRandomizationPool(1);
        properties.getCandidateGeneration().setColdStartPoolSize(1);
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("watched", movie("drama"));
        catalog.put("rated", movie("comedy"));
        catalog.put("fresh", movie("sci-fi"));
        properties.setCatalog(catalog);

        FeatureCache featureCache = new FeatureCache(properties);
        DeepLearningPredictionService predictionService = mock(DeepLearningPredictionService.class);
        when(predictionService.predict(any(), any())).thenReturn(Optional.empty());
        HybridRecommendationService service = new HybridRecommendationService(
            redis,
            properties,
            predictionService,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache,
            List.of(new MovieLensUserHistoryQueryHydrator(
                userId -> new UserMovieHistory(List.of("watched"), List.of("rated"))
            ))
        );

        RecommendationResult result = service.recommend("u1", 1);

        assertEquals(List.of("fresh"), result.recommendations());
        assertFalse(result.recommendations().contains("watched"));
        assertFalse(result.recommendations().contains("rated"));
    }

    private static MovieProfile movie(String genre) {
        MovieProfile profile = new MovieProfile();
        profile.setGenres(List.of(genre));
        profile.setTags(List.of(genre));
        return profile;
    }
}
