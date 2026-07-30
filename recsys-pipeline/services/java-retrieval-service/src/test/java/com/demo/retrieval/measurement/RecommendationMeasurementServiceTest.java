package com.demo.retrieval.measurement;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.model.FeedbackRequest;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecommendationMeasurementServiceTest {

    @Test
    void recordsOnlyBoundedRequestAndStageMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationMeasurementService measurements = measurements(registry);

        measurements.recordRequest("recommend", Duration.ofMillis(12), false);
        assertEquals(1L, registry.find("recommendation.request.latency")
            .tag("endpoint", "recommend").timer().count());

        assertEquals("result", measurements.timeStage("scoring", () -> "result"));
        measurements.timeStage("unbounded-caller-input", () -> null);

        assertEquals(1L, registry.find("recommendation.stage.latency")
            .tag("stage", "scoring").timer().count());
        assertEquals(0L, registry.find("recommendation.stage.latency")
            .tag("stage", "unbounded-caller-input").timers().size());
    }

    @Test
    void collapsesFreeFormFilterReasonsAndRecordsOnlyFeedbackPresence() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationMeasurementService measurements = measurements(registry);

        measurements.recordFilterDecisions(List.of(new FilterDecision("customer supplied explanation")));
        measurements.recordFeedbackCoverage(new FeedbackRequest(
            "user-42", "item-9", true, 1.0, "request-123", 4.5,
            "not relevant", 900L, 0.7));

        assertEquals(1.0, registry.find("recommendation.filter.decisions")
            .tag("reason", "unknown").counter().count());
        assertEquals(1.0, registry.find("recommendation.feedback.presence")
            .tag("signal", "rating").counter().count());
        assertEquals(1.0, registry.find("recommendation.feedback.presence")
            .tag("signal", "negative_feedback_reason").counter().count());
        assertTrue(registry.getMeters().stream().flatMap(meter -> meter.getId().getTags().stream())
            .noneMatch(tag -> tag.getValue().equals("user-42")
                || tag.getValue().equals("item-9")
                || tag.getValue().equals("request-123")
                || tag.getValue().equals("not relevant")));
    }

    @Test
    void snapshotUsesMeasurementSchemaAndLatencyPercentiles() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationMeasurementService measurements = measurements(registry);
        measurements.recordRequest("feedback", Duration.ofMillis(20), false);

        MeasurementSnapshot snapshot = measurements.snapshot();
        Map<String, Object> values = snapshot.asMap();

        assertEquals("2.0", snapshot.schemaVersion());
        Map<?, ?> latency = (Map<?, ?>) values.get("latency");
        assertTrue(latency.containsKey("p50"));
        assertTrue(latency.containsKey("p95"));
        assertTrue(latency.containsKey("p99"));
    }

    private RecommendationMeasurementService measurements(SimpleMeterRegistry registry) {
        return new RecommendationMeasurementService(registry, new RecommendationProperties());
    }
}
