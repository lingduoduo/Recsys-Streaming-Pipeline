package com.demo.retrieval.controller;

import com.demo.retrieval.service.FeedbackRequest;
import com.demo.retrieval.service.HybridRecommendationService;
import com.demo.retrieval.service.RecommendationResult;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@Validated
public class RecommendationController {
    private static final Logger log = LoggerFactory.getLogger(RecommendationController.class);
    private static final String DEFAULT_LIMIT = "6";
    private static final int MAX_LIMIT = 50;

    private final StringRedisTemplate redis;
    private final HybridRecommendationService recommendationService;

    public RecommendationController(StringRedisTemplate redis, HybridRecommendationService recommendationService) {
        this.redis = redis;
        this.recommendationService = recommendationService;
    }

    @GetMapping("/embedding/{item}")
    public Map<String, Object> embedding(
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String item
    ) {
        String key = "i2vEmb:" + item;
        String raw;
        try {
            raw = redis.opsForValue().get(key);
        } catch (Exception e) {
            log.error("Redis fetch failed for embedding {}", key, e);
            return Map.of("item", item, "embedding", List.of());
        }
        if (raw == null) {
            return Map.of("item", item, "embedding", List.of());
        }
        try {
            List<Double> vector = Arrays.stream(raw.split(" "))
                .map(Double::parseDouble)
                .collect(Collectors.toList());
            return Map.of("item", item, "embedding", vector);
        } catch (NumberFormatException e) {
            log.error("Corrupt embedding data for key {}: {}", key, raw, e);
            return Map.of("item", item, "embedding", List.of(), "error", "corrupt_data");
        }
    }

    @GetMapping("/recommend/{user}")
    public Map<String, Object> recommend(
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String user,
        @RequestParam(defaultValue = DEFAULT_LIMIT) int limit
    ) {
        int boundedLimit = Math.max(1, Math.min(limit, MAX_LIMIT));
        RecommendationResult result = recommendationService.recommend(user, boundedLimit);
        return Map.of(
            "user", result.user(),
            "recent", result.recent(),
            "recommendations", result.recommendations(),
            "diagnostics", result.candidateDiagnostics(),
            "metrics", result.metrics()
        );
    }

    @PostMapping("/feedback")
    public Map<String, Object> feedback(@Valid @RequestBody FeedbackRequest request) {
        return recommendationService.recordFeedback(request);
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        return recommendationService.getAggregateMetrics();
    }

    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleValidation(ConstraintViolationException e) {
        return Map.of("error", "Invalid input: id must be 1-64 alphanumeric characters");
    }
}
