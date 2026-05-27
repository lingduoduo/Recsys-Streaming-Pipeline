package com.demo.retrieval.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Redis-backed GeoLocationClient.
 *
 * Reads the user's ZIP code from user:{userId}:features (written by
 * MovieLensContextCollectorStreamingJob) and returns it as the location string.
 *
 * Mirrors the Rust GeoIpLocationClient which resolves a request-time IP address
 * to a geographic location. For MovieLens there is no per-request IP; the user's
 * ZIP code from the dataset is the pre-computed location equivalent.
 */
@Component
public class RedisGeoLocationClient implements GeoLocationClient {

    private final StringRedisTemplate redis;

    public RedisGeoLocationClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public String getLocation(String userId) {
        Object raw = redis.opsForHash().get("user:" + userId + ":features", "zipCode");
        return raw == null ? "" : raw.toString().trim();
    }
}
