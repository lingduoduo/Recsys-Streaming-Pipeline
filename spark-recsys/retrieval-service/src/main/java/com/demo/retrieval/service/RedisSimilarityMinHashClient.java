package com.demo.retrieval.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Redis-backed SimilarityMinHashClient.
 *
 * Reads from the hash at user:{userId}:minhash, written by the offline
 * MinHash computation pipeline. Field: signature (comma-separated longs).
 * Returns an empty list for unknown users.
 *
 * Mirrors the Rust MutualFollowQueryHydrator which calls StratoClient
 * .get_minhash_with_count() to fetch a MinHash signature for Jaccard
 * similarity computation. In MovieLens, the MinHash is over co-rated movie
 * sets rather than follower sets.
 */
@Component
public class RedisSimilarityMinHashClient implements SimilarityMinHashClient {

    private final StringRedisTemplate redis;

    public RedisSimilarityMinHashClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<Long> getMinHash(String userId) {
        Object raw = redis.opsForHash().get("user:" + userId + ":minhash", "signature");
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(raw.toString().split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .map(Long::parseLong)
            .toList();
    }
}
