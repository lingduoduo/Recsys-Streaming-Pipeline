package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import org.junit.jupiter.api.BeforeEach;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
        when(hashOps.get(anyString(), anyString())).thenReturn("10");
        when(hashOps.entries(anyString())).thenReturn(Map.of(
            "requests", "2",
            "recommendations_served", "4",
            "clicks", "1",
            "pseudo_regret_total", "0.2",
            "reward_total", "1.0",
            "estimated_reward_total", "2.5",
            "cold_start_impressions", "1",
            "exploratory_impressions", "2"
        ));

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

        service = new HybridRecommendationService(redis, properties);
    }

    @Test
    void recommendInjectsColdStartCandidatesAndReturnsDiagnostics() {
        when(listOps.range("user:u1:recent", 0L, 1L)).thenReturn(List.of("item1"));
        when(zSetOps.reverseRange("global:item_popularity", 0L, 5L)).thenReturn(new LinkedHashSet<>(List.of("item2", "item4")));
        when(zSetOps.score("global:item_popularity", "item2")).thenReturn(10.0);
        when(zSetOps.score("global:item_popularity", "item4")).thenReturn(2.0);
        when(zSetOps.score("global:item_popularity", "item7")).thenReturn(1.0);

        when(valueOps.get(anyString())).thenAnswer(invocation -> switch (invocation.getArgument(0).toString()) {
            case "uEmb:u1" -> null;
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
        });

        RecommendationResult result = service.recommend("u1", 2);

        assertEquals("u1", result.user());
        assertEquals(List.of("item1"), result.recent());
        assertEquals(2, result.recommendations().size());
        assertTrue(result.recommendations().contains("item4"));
        assertFalse(result.candidateDiagnostics().isEmpty());
        assertEquals("ucb", result.metrics().get("algorithm"));
        verify(valueOps).increment("bandit:item:item4:impressions", 1);
    }

    @Test
    void feedbackAggregatesBusinessMetrics() {
        Map<String, Object> result = service.recordFeedback(new FeedbackRequest("u1", "item4", true, 1.0));

        assertEquals("ok", result.get("status"));
        assertEquals(Boolean.TRUE, result.get("clicked"));
        assertTrue(result.containsKey("metrics"));
        verify(hashOps).increment("bandit:metrics", "clicks", 1L);
        verify(valueOps).increment("bandit:item:item4:clicks", 1);
        verify(hashOps).increment("bandit:metrics", "reward_total", 1.0);
    }

    private MovieProfile movie(List<String> genres, List<String> tags, boolean newRelease) {
        MovieProfile profile = new MovieProfile();
        profile.setGenres(genres);
        profile.setTags(tags);
        profile.setNewRelease(newRelease);
        return profile;
    }
}
