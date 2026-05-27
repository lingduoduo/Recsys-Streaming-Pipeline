package com.demo.retrieval.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Redis-backed FollowedGenreTopicsClient.
 *
 * Reads favoriteGenres from user:{userId}:features (written by
 * MovieLensContextCollectorStreamingJob). This is the MovieLens analogue of
 * the Rust FollowedGrokTopicsStoreClient, which fetches topic IDs from a
 * dedicated Manhattan store separate from the general feature store.
 */
@Component
public class RedisFollowedGenreTopicsClient implements FollowedGenreTopicsClient {

    private final StringRedisTemplate redis;

    public RedisFollowedGenreTopicsClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<String> getFollowedGenres(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":features", "favoriteGenres"));
    }

    private static List<String> parseList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(raw.toString().split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .toList();
    }
}
