package com.demo.retrieval.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Redis-backed SocialGraphClient.
 *
 * Reads from the hash at user:{userId}:social, written by the social graph
 * ingestion pipeline. Fields: blockedUserIds, mutedUserIds, followedUserIds
 * (comma-separated). Returns empty lists for unknown users.
 */
@Component
public class RedisSocialGraphClient implements SocialGraphClient {

    private final StringRedisTemplate redis;

    public RedisSocialGraphClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<String> getBlockedUserIds(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":social", "blockedUserIds"));
    }

    @Override
    public List<String> getMutedUserIds(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":social", "mutedUserIds"));
    }

    @Override
    public List<String> getFollowedUserIds(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":social", "followedUserIds"));
    }

    @Override
    public List<String> getSubscribedUserIds(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":social", "subscribedUserIds"));
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
