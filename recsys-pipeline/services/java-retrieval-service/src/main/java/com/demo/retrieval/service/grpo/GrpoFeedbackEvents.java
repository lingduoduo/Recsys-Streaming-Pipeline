package com.demo.retrieval.service.grpo;

import com.demo.retrieval.model.FeedbackRequest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Serving's own feedback events, shaped exactly like the click events the Python producers emit.
 *
 * OnlineJoinerStreamingJob.buildTrainingSamples derives label purely from event_type:
 * clicked = max(etype == "click"), ordered = max(etype in ("order","purchase")), and
 * label = ordered ? 2.0 : clicked ? 1.0 : 0.0. A click is therefore the only feedback signal that
 * moves the label, so this emits one event on a click and nothing otherwise. The joiner does read
 * rating, negative_feedback_reason, dwell_millis and completion_rate off feedback events
 * (OnlineJoinerStreamingJob.scala's latestFeedback), but they are deliberately not emitted here:
 * the label is unaffected either way, and a serving-sourced sample will simply carry nulls in
 * those columns rather than values FeedbackRequest doesn't collect.
 *
 * session_id is also deliberately omitted. The joiner aggregates it with
 * first(col("session_id"), ignoreNulls = true), not gated on isImpression, so a fabricated value
 * here would make the sample's session_id nondeterministic depending on event ordering; a null
 * loses to the impression event's real session_id under ignoreNulls.
 *
 * requestId is a parameter rather than request.requestId() because the caller
 * (HybridRecommendationService.recordFeedback) has already resolved which id joins to the
 * impression: serving's own requestId when the client sent one, otherwise the one recovered from
 * the pending replay context. This class only shapes the event; it does not choose the id.
 *
 * position is not meaningful here: FeedbackRequest carries no slate index, and
 * OnlineJoinerStreamingJob.buildTrainingSamples only reads position off impression events
 * (max(when(isImpression, col("position")))), never off feedback ones. 0 is a placeholder, not a
 * claim about where in the slate this item sat.
 */
public final class GrpoFeedbackEvents {

    private GrpoFeedbackEvents() {}

    public static List<Map<String, Object>> build(FeedbackRequest request, String requestId, long nowMs) {
        if (!request.clicked()) {
            return List.of();
        }
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("event_id", UUID.randomUUID().toString());
        event.put("request_id", requestId);
        event.put("user_id", request.user());
        event.put("item_id", request.item());
        event.put("event_type", "click");
        event.put("timestamp_ms", nowMs);
        event.put("position", 0);
        event.put("user_features", Map.of());
        event.put("item_features", Map.of());
        event.put("context_features", Map.of());
        return List.of(event);
    }
}
