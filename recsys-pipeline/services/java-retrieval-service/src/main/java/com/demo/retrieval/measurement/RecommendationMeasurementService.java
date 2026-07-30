package com.demo.retrieval.measurement;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.model.FeedbackRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

@Service
public class RecommendationMeasurementService {
    private static final Logger log = LoggerFactory.getLogger(RecommendationMeasurementService.class);
    private static final Set<String> ENDPOINTS = Set.of("recommend", "feedback");
    private static final Set<String> STAGES =
        Set.of("hydration", "redis_fetch", "scoring", "selection", "side_effects");
    private static final Set<String> FILTER_REASONS =
        Set.of("expired", "muted_product_type", "muted_genre", "muted_keyword", "muted_title", "unknown");
    private static final List<String> FEEDBACK_SIGNALS = List.of(
        "request_id", "rating", "negative_feedback_reason", "dwell_millis", "completion_rate");

    private final MeterRegistry registry;
    private final Duration[] latencyBuckets;
    private final boolean noOp;
    private final LongAdder freshnessTotal = new LongAdder();
    private final LongAdder freshnessObserved = new LongAdder();
    private final LongAdder freshnessFresh = new LongAdder();
    private final Map<String, LongAdder> filterDecisionCounts = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> feedbackPresenceCounts = new ConcurrentHashMap<>();

    @Autowired
    public RecommendationMeasurementService(MeterRegistry registry, RecommendationProperties properties) {
        this(registry, latencyBuckets(properties), false);
    }

    private RecommendationMeasurementService(MeterRegistry registry, Duration[] latencyBuckets, boolean noOp) {
        this.registry = registry;
        this.latencyBuckets = latencyBuckets;
        this.noOp = noOp;
    }

    /** For compatibility constructors and tests that must prove instrumentation cannot affect selection. */
    public static RecommendationMeasurementService noOp() {
        return new RecommendationMeasurementService(new SimpleMeterRegistry(), new Duration[0], true);
    }

    public void recordRequest(String endpoint, Duration duration, boolean error) {
        try {
            if (noOp || !ENDPOINTS.contains(endpoint)) {
                return;
            }
            String outcome = error ? "error" : "success";
            requestTimer(endpoint, outcome).record(safeDuration(duration));
            if (error) {
                Counter.builder("recommendation.request.errors")
                    .tag("endpoint", endpoint)
                    .register(registry)
                    .increment();
            }
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation request measurement", e);
        }
    }

    public <T> T timeStage(String stage, Supplier<T> operation) {
        if (noOp || !STAGES.contains(stage)) {
            return operation.get();
        }
        long started = System.nanoTime();
        try {
            return operation.get();
        } finally {
            recordStage(stage, Duration.ofNanos(System.nanoTime() - started));
        }
    }

    public void recordFreshness(Collection<MovieProfile> selectedProfiles) {
        try {
            if (noOp || selectedProfiles == null) {
                return;
            }
            for (MovieProfile profile : selectedProfiles) {
                freshnessTotal.increment();
                if (profile == null) {
                    continue;
                }
                // The current live catalog exposes the documented boolean fallback. A later optional
                // timestamp can be added without changing this aggregate-only schema.
                freshnessObserved.increment();
                if (profile.isNewRelease()) {
                    freshnessFresh.increment();
                }
                Counter.builder("recommendation.freshness.exposures")
                    .tags("source", "boolean_new_release", "fresh", Boolean.toString(profile.isNewRelease()))
                    .register(registry)
                    .increment();
            }
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation freshness measurement", e);
        }
    }

    public void recordFilterDecisions(Collection<FilterDecision> decisions) {
        try {
            if (noOp || decisions == null) {
                return;
            }
            for (FilterDecision decision : decisions) {
                String reason = decision == null ? "unknown" : boundedReason(decision.reason());
                Counter.builder("recommendation.filter.decisions")
                    .tag("reason", reason)
                    .register(registry)
                    .increment();
                filterDecisionCounts.computeIfAbsent(reason, ignored -> new LongAdder()).increment();
            }
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation filter measurement", e);
        }
    }

    public void recordFeedbackCoverage(FeedbackRequest request) {
        try {
            if (noOp || request == null) {
                return;
            }
            recordPresent("request_id", request.requestId() != null);
            recordPresent("rating", request.rating() != null);
            recordPresent("negative_feedback_reason", request.negativeFeedbackReason() != null);
            recordPresent("dwell_millis", request.dwellMillis() != null);
            recordPresent("completion_rate", request.completionRate() != null);
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation feedback coverage", e);
        }
    }

    public MeasurementSnapshot snapshot() {
        try {
            return new MeasurementSnapshot(
                "2.0", latencySnapshot(), freshnessSnapshot(), safetySnapshot(), feedbackSnapshot());
        } catch (RuntimeException e) {
            log.warn("Unable to assemble recommendation measurement snapshot", e);
            return new MeasurementSnapshot("2.0", emptyLatency(), Map.of(), Map.of(), Map.of());
        }
    }

    private void recordStage(String stage, Duration duration) {
        try {
            Timer.builder("recommendation.stage.latency")
                .tag("stage", stage)
                .serviceLevelObjectives(latencyBuckets)
                .publishPercentiles(0.50, 0.95, 0.99)
                .register(registry)
                .record(safeDuration(duration));
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation stage measurement", e);
        }
    }

    private Timer requestTimer(String endpoint, String outcome) {
        return Timer.builder("recommendation.request.latency")
            .tags("endpoint", endpoint, "outcome", outcome)
            .serviceLevelObjectives(latencyBuckets)
            .publishPercentiles(0.50, 0.95, 0.99)
            .register(registry);
    }

    private void recordPresent(String signal, boolean present) {
        if (!present || !FEEDBACK_SIGNALS.contains(signal)) {
            return;
        }
        Counter.builder("recommendation.feedback.presence")
            .tag("signal", signal)
            .register(registry)
            .increment();
        feedbackPresenceCounts.computeIfAbsent(signal, ignored -> new LongAdder()).increment();
    }

    private Map<String, Object> latencySnapshot() {
        Collection<Timer> timers = registry.find("recommendation.request.latency").timers();
        long count = timers.stream().mapToLong(Timer::count).sum();
        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("count", count);
        latency.put("p50", percentile(timers, 0.50));
        latency.put("p95", percentile(timers, 0.95));
        latency.put("p99", percentile(timers, 0.99));
        return latency;
    }

    private Map<String, Object> emptyLatency() {
        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("count", 0L);
        latency.put("p50", null);
        latency.put("p95", null);
        latency.put("p99", null);
        return latency;
    }

    private Map<String, Object> freshnessSnapshot() {
        long total = freshnessTotal.sum();
        long observed = freshnessObserved.sum();
        Map<String, Object> freshness = new LinkedHashMap<>();
        freshness.put("exposures", total);
        freshness.put("coverage", total == 0 ? null : (double) observed / total);
        freshness.put("freshShare", observed == 0 ? null : (double) freshnessFresh.sum() / observed);
        freshness.put("source", "boolean_new_release");
        return freshness;
    }

    private Map<String, Object> safetySnapshot() {
        Map<String, Object> decisions = new LinkedHashMap<>();
        FILTER_REASONS.stream().sorted().forEach(reason -> {
            LongAdder count = filterDecisionCounts.get(reason);
            if (count != null) {
                decisions.put(reason, count.sum());
            }
        });
        return Map.of("filterDecisions", Map.copyOf(decisions));
    }

    private Map<String, Object> feedbackSnapshot() {
        Map<String, Object> presence = new LinkedHashMap<>();
        FEEDBACK_SIGNALS.forEach(signal -> {
            LongAdder count = feedbackPresenceCounts.get(signal);
            if (count != null) {
                presence.put(signal, count.sum());
            }
        });
        return Map.of("present", Map.copyOf(presence));
    }

    private static Duration[] latencyBuckets(RecommendationProperties properties) {
        if (properties == null || properties.getMeasurements() == null) {
            return new Duration[0];
        }
        return properties.getMeasurements().getLatencyBucketsMs().stream()
            .filter(value -> value != null && value > 0)
            .distinct()
            .sorted(Comparator.naturalOrder())
            .map(Duration::ofMillis)
            .toArray(Duration[]::new);
    }

    private static Duration safeDuration(Duration duration) {
        return duration == null || duration.isNegative() ? Duration.ZERO : duration;
    }

    private static String boundedReason(String reason) {
        return FILTER_REASONS.contains(reason) ? reason : "unknown";
    }

    private static Double percentile(Collection<Timer> timers, double target) {
        java.util.OptionalDouble percentileValue = timers.stream()
            .flatMap(timer -> java.util.Arrays.stream(timer.takeSnapshot().percentileValues()))
            .filter(percentile -> Double.compare(percentile.percentile(), target) == 0)
            .mapToDouble(ValueAtPercentile::value)
            .max();
        return percentileValue.isPresent() ? percentileValue.getAsDouble() : null;
    }
}
