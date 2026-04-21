package com.demo.retrieval.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.util.*;
import java.util.stream.Collectors;

@RestController
public class RecommendationController {
    private static final Logger log = LoggerFactory.getLogger(RecommendationController.class);
    private static final String DEFAULT_LIMIT = "6";
    private static final int MAX_LIMIT = 50;

    private final StringRedisTemplate redis;

    public RecommendationController(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @GetMapping("/embedding/{item}")
    public Map<String, Object> embedding(@PathVariable String item) {
        String key = "i2vEmb:" + item;
        try {
            String raw = redis.opsForValue().get(key);
            if (raw == null) {
                return Map.of("item", item, "embedding", List.of());
            }
            List<Double> vector = Arrays.stream(raw.split(" "))
                .map(Double::parseDouble)
                .collect(Collectors.toList());
            return Map.of("item", item, "embedding", vector);
        } catch (Exception e) {
            log.error("Redis fetch failed for embedding {}", key, e);
            return Map.of("item", item, "embedding", List.of());
        }
    }

    @GetMapping("/recommend/{user}")
    public Map<String, Object> recommend(
        @PathVariable String user,
        @RequestParam(defaultValue = DEFAULT_LIMIT) int limit
    ) {
        int boundedLimit = Math.max(1, Math.min(limit, MAX_LIMIT));

        List<String> recent;
        Set<String> popular;
        try {
            recent = Optional
                .ofNullable(redis.opsForList().range("user:" + user + ":recent", 0, boundedLimit - 1))
                .orElseGet(List::of);
            popular = Optional
                .ofNullable(redis.opsForZSet().reverseRange("global:item_popularity", 0, (boundedLimit * 2L) - 1))
                .orElseGet(Set::of);
        } catch (Exception e) {
            log.error("Redis fetch failed for user {}", user, e);
            return Map.of("user", user, "recent", List.of(), "recommendations", List.of());
        }

        Set<String> recentSet = new HashSet<>(recent);
        List<String> recommendations = popular.stream()
            .filter(item -> !recentSet.contains(item))
            .limit(boundedLimit)
            .collect(Collectors.toList());

        return Map.of(
            "user", user,
            "recent", recent,
            "recommendations", recommendations
        );
    }
}
