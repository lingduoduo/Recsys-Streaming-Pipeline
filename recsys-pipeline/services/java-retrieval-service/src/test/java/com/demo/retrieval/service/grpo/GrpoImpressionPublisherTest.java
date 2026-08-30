package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoImpressionPublisherTest {

    private final List<String> sent = new ArrayList<>();
    private final GrpoImpressionPublisher.GrpoSender recorder =
        (key, payload) -> sent.add(new String(payload));

    private ServingSideEffectRequest request() {
        ServedMovie movie = new ServedMovie("m1", 0.4, 0.3, 0.05, 0.7, false, 10, 2,
            Map.of("predictionScore", 0.7));
        return new ServingSideEffectRequest(
            "req-1", "u1", "hybrid", Map.of(), List.of(movie), List.of(movie),
            List.of(), List.of(), 0L, 0L, 1L, 0.0, 0.0, 0.0);
    }

    @Test
    void sendsNothingWhenDisabled() {
        new GrpoImpressionPublisher(new ObjectMapper(), recorder, false).publish(request(), 1000L);
        assertTrue(sent.isEmpty());
    }

    @Test
    void sendsOneMessagePerItemWhenEnabled() {
        new GrpoImpressionPublisher(new ObjectMapper(), recorder, true).publish(request(), 1000L);
        assertEquals(1, sent.size());
        assertTrue(sent.get(0).contains("\"item_id\":\"m1\""), sent.get(0));
        assertTrue(sent.get(0).contains("\"grpo_x\":\"v1:"), sent.get(0));
    }

    @Test
    void aSenderFailureNeverPropagates() {
        GrpoImpressionPublisher.GrpoSender broken = (key, payload) -> {
            throw new IllegalStateException("broker down");
        };
        // A Kafka outage must not fail a recommendation request. This is a logging side effect.
        new GrpoImpressionPublisher(new ObjectMapper(), broken, true).publish(request(), 1000L);
    }
}
