package com.demo.retrieval.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
public class RedisUserMovieHistoryClient implements UserMovieHistoryClient {
    private static final int HISTORY_SIZE = 50;

    private final StringRedisTemplate redis;

    public RedisUserMovieHistoryClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<String> getWatchedMovies(String userId) {
        return readHistory("user:" + userId + ":recent");
    }

    @Override
    public List<String> getRatedMovies(String userId) {
        return readHistory("user:" + userId + ":rated");
    }

    private List<String> readHistory(String key) {
        return Optional.ofNullable(redis.opsForList().range(key, 0, HISTORY_SIZE - 1L))
            .orElseGet(List::of);
    }
}
