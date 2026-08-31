package com.demo.retrieval.service.grpo;

import com.demo.retrieval.model.FeedbackRequest;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Publishes serving's own GRPO events to Kafka, off by default.
 *
 * Every failure is swallowed and logged. This is a logging side effect on the serving path, not a
 * serving dependency: a broker outage must degrade training data, never a recommendation response.
 */
public class GrpoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(GrpoEventPublisher.class);

    /** Seam over the Kafka producer, so tests need no broker. */
    public interface GrpoSender {
        void send(String key, byte[] payload);
    }

    private final ObjectMapper objectMapper;
    private final GrpoSender sender;
    private final boolean enabled;

    public GrpoEventPublisher(ObjectMapper objectMapper, GrpoSender sender, boolean enabled) {
        this.objectMapper = objectMapper;
        this.sender = sender;
        this.enabled = enabled;
    }

    public void publish(ServingSideEffectRequest request, long nowMs) {
        if (!enabled || sender == null) {
            return;
        }
        try {
            for (Map<String, Object> event : GrpoImpressionEvents.build(request, nowMs)) {
                sender.send(request.requestId(),
                    objectMapper.writeValueAsBytes(event));
            }
        } catch (Exception e) {
            log.warn("Failed to publish GRPO impression events for request {}", request.requestId(), e);
        }
    }

    public void publishFeedback(FeedbackRequest request, String requestId, long nowMs) {
        if (!enabled || sender == null) {
            return;
        }
        try {
            for (Map<String, Object> event : GrpoFeedbackEvents.build(request, requestId, nowMs)) {
                sender.send(requestId, objectMapper.writeValueAsBytes(event));
            }
        } catch (Exception e) {
            log.warn("Failed to publish GRPO feedback events for request {}", requestId, e);
        }
    }
}
