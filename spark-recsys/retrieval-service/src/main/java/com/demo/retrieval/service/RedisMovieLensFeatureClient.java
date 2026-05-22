package com.demo.retrieval.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class RedisMovieLensFeatureClient implements MovieLensFeatureClient {
    private final StringRedisTemplate redis;

    public RedisMovieLensFeatureClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public Optional<MovieLensUserFeatures> getUserFeatures(String userId) {
        Map<Object, Object> raw = redis.opsForHash().entries("user:" + userId + ":features");
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new MovieLensUserFeatures(
            userId,
            readList(raw.get("favoriteGenres")),
            readDouble(raw.get("avgRating")),
            (int) readLong(raw.get("ratingCount")),
            readList(raw.get("recentlyRatedMovieIds"))
        ));
    }

    private List<String> readList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(String.valueOf(raw).split(","))
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .toList();
    }

    private double readDouble(Object raw) {
        if (raw == null) {
            return 0.0;
        }
        try {
            return Double.parseDouble(String.valueOf(raw));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private long readLong(Object raw) {
        if (raw == null) {
            return 0L;
        }
        try {
            return Long.parseLong(String.valueOf(raw));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
