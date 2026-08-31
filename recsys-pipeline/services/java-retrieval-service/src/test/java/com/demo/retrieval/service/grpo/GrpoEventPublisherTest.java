package com.demo.retrieval.service.grpo;

import com.demo.retrieval.model.FeedbackRequest;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoEventPublisherTest {

    private final List<String> sent = new ArrayList<>();
    private final GrpoEventPublisher.GrpoSender recorder =
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
        new GrpoEventPublisher(new ObjectMapper(), recorder, false).publish(request(), 1000L);
        assertTrue(sent.isEmpty());
    }

    @Test
    void sendsOneMessagePerItemWhenEnabled() {
        new GrpoEventPublisher(new ObjectMapper(), recorder, true).publish(request(), 1000L);
        assertEquals(1, sent.size());
        assertTrue(sent.get(0).contains("\"item_id\":\"m1\""), sent.get(0));
        assertTrue(sent.get(0).contains("\"grpo_x\":\"v1:"), sent.get(0));
    }

    @Test
    void aSenderFailureNeverPropagates() {
        GrpoEventPublisher.GrpoSender broken = (key, payload) -> {
            throw new IllegalStateException("broker down");
        };
        // A Kafka outage must not fail a recommendation request. This is a logging side effect.
        new GrpoEventPublisher(new ObjectMapper(), broken, true).publish(request(), 1000L);
    }

    @Test
    void publishFeedbackSendsNothingWhenDisabled() {
        FeedbackRequest feedback = new FeedbackRequest("u1", "m1", true, 1.0);
        new GrpoEventPublisher(new ObjectMapper(), recorder, false).publishFeedback(feedback, "req-1", 1000L);
        assertTrue(sent.isEmpty());
    }

    @Test
    void publishFeedbackSendsOneMessageOnClickWhenEnabled() {
        FeedbackRequest feedback = new FeedbackRequest("u1", "m1", true, 1.0);
        new GrpoEventPublisher(new ObjectMapper(), recorder, true).publishFeedback(feedback, "req-1", 1000L);
        assertEquals(1, sent.size());
        assertTrue(sent.get(0).contains("\"event_type\":\"click\""), sent.get(0));
        assertTrue(sent.get(0).contains("\"request_id\":\"req-1\""), sent.get(0));
    }

    @Test
    void publishFeedbackSendsNothingWithoutAClick() {
        FeedbackRequest feedback = new FeedbackRequest("u1", "m1", false, 0.0);
        new GrpoEventPublisher(new ObjectMapper(), recorder, true).publishFeedback(feedback, "req-1", 1000L);
        assertTrue(sent.isEmpty());
    }

    @Test
    void publishFeedbackSenderFailureNeverPropagates() {
        GrpoEventPublisher.GrpoSender broken = (key, payload) -> {
            throw new IllegalStateException("broker down");
        };
        FeedbackRequest feedback = new FeedbackRequest("u1", "m1", true, 1.0);
        // A Kafka outage must not fail a feedback request either. Same logging side effect.
        new GrpoEventPublisher(new ObjectMapper(), broken, true).publishFeedback(feedback, "req-1", 1000L);
    }
}
