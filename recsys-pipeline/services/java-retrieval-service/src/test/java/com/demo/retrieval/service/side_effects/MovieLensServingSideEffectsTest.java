package com.demo.retrieval.service.side_effects;

import com.demo.retrieval.service.grpo.GrpoImpressionPublisher;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class MovieLensServingSideEffectsTest {
    private StringRedisTemplate redis;
    private ValueOperations<String, String> valueOps;
    private SetOperations<String, String> setOps;
    private HashOperations<String, Object, Object> hashOps;
    private MovieLensServingSideEffects sideEffects;

    @BeforeEach
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        setOps = mock(SetOperations.class);
        hashOps = mock(HashOperations.class);
        when(redis.opsForValue()).thenReturn(valueOps);
        when(redis.opsForSet()).thenReturn(setOps);
        when(redis.opsForHash()).thenReturn(hashOps);
        when(redis.executePipelined(any(SessionCallback.class))).thenAnswer(invocation -> {
            SessionCallback cb = invocation.getArgument(0);
            cb.execute(redis);
            return List.of();
        });
        sideEffects = new MovieLensServingSideEffects(redis, new ObjectMapper(), Duration.ofHours(1),
            new GrpoImpressionPublisher(new ObjectMapper(), null, false));
    }

    @Test
    void recordServedWritesMovieLensServingSideEffectsInOnePipeline() {
        ServedMovie top = servedMovie("m1", true);
        ServingSideEffectRequest request = new ServingSideEffectRequest(
            "req-1",
            "u1",
            "ucb",
            Map.of("recent", List.of("m0"), "genres", List.of("sci-fi"), "tags", List.of()),
            List.of(top, servedMovie("m2", false)),
            List.of(top),
            List.of("m3", "m1"),
            List.of("m4"),
            1,
            1,
            1,
            0.8,
            0.1,
            0.5
        );

        sideEffects.recordServed(request);

        verify(valueOps).increment("bandit:item:m1:impressions", 1);
        verify(setOps).add(MovieLensServingSideEffects.EXPOSED_ITEMS_KEY, "m1");
        verify(hashOps).put("user:u1:served_history", "movieIds", "m1,m3");
        verify(hashOps).put("user:u1:impressions", "movieIds", "m1,m4");
        verify(hashOps).putAll(eq("recommendation:request:req-1"), any(Map.class));
        verify(hashOps).increment(MovieLensServingSideEffects.METRICS_HASH_KEY, "requests", 1L);
        verify(hashOps).increment(MovieLensServingSideEffects.metricsHashKey("ucb"), "recommendations_served", 1L);
        ArgumentCaptor<String> pendingPayload = ArgumentCaptor.forClass(String.class);
        verify(valueOps).set(eq(MovieLensServingSideEffects.pendingReplayKey("u1", "m1")), pendingPayload.capture(), eq(Duration.ofHours(1)));
        String payload = pendingPayload.getValue();
        assertTrue(payload.contains("\"action\":\"m1\""));
        assertTrue(payload.contains("\"state\""));
        assertFalse(payload.contains("\"context\""), "context is a removed duplicate of state");
    }

    @Test
    void recordServedSkipsEmptySelections() {
        sideEffects.recordServed(new ServingSideEffectRequest(
            "req-empty",
            "u1",
            "ucb",
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            0,
            0,
            0,
            0.0,
            0.0,
            0.0
        ));

        verify(redis, org.mockito.Mockito.never()).executePipelined(any(SessionCallback.class));
    }

    private ServedMovie servedMovie(String movieId, boolean coldStart) {
        Map<String, Object> predictions = Map.of(
            "outcomeProbability", 0.7,
            "predictionScore", 0.8,
            "diversityScore", 1.0
        );
        return new ServedMovie(movieId, 0.8, 0.6, 0.1, 0.9, coldStart, 2, 1, predictions);
    }
}
