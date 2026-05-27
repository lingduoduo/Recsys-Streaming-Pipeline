package com.demo.retrieval.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Redis-backed FollowedStarterPacksClient.
 *
 * Reads from user:{userId}:starter_packs, maintained by the starter pack
 * follow pipeline. Returns an empty list for MovieLens users since starter
 * packs (curated follow sets for new users) have no direct MovieLens equivalent.
 *
 * Mirrors the Rust FollowedStarterPacksStoreClient, which reads from a
 * dedicated Manhattan store separate from the general feature store.
 */
@Component
public class RedisFollowedStarterPacksClient implements FollowedStarterPacksClient {

    private final StringRedisTemplate redis;

    public RedisFollowedStarterPacksClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<Integer> getFollowedPackIds(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":starter_packs", "packIds"));
    }

    private static List<Integer> parseList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(raw.toString().split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .map(Integer::parseInt)
            .toList();
    }
}
