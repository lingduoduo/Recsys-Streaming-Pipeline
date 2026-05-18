package com.demo.retrieval.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.demo.retrieval.config.RecommendationProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Three-layer feature storage:
 *   Offline  — full model artifacts on the filesystem (loaded at startup by DeepLearningPredictionService).
 *   Redis    — real-time online features written by streaming jobs (embeddings, bandit counters, user history).
 *   In-memory — this class: Caffeine caches for the hottest Redis reads so the per-request round-trip count
 *               drops from O(N×features) to O(1) after the first population within each TTL window.
 *
 * Item vectors (i2vEmb:*): updated only when training jobs run; safe to cache for minutes.
 * Reward model stats (reward-model:*): updated on every feedback event; short TTL keeps staleness bounded.
 */
@Component
public class FeatureCache {

    private final Cache<String, double[]> itemVectors;
    private final Cache<String, RewardModelStats> rewardStats;

    public FeatureCache(RecommendationProperties properties) {
        RecommendationProperties.Cache cfg = properties.getCache();
        this.itemVectors = Caffeine.newBuilder()
            .maximumSize(cfg.getItemVectorMaxSize())
            .expireAfterWrite(cfg.getItemVectorTtlSeconds(), TimeUnit.SECONDS)
            .build();
        this.rewardStats = Caffeine.newBuilder()
            .maximumSize(cfg.getRewardMaxSize())
            .expireAfterWrite(cfg.getRewardTtlSeconds(), TimeUnit.SECONDS)
            .build();
    }

    // --- item vectors ---

    public double[] getItemVector(String item) {
        return itemVectors.getIfPresent(item);
    }

    public void putItemVector(String item, double[] vector) {
        itemVectors.put(item, vector != null ? vector : new double[0]);
    }

    public boolean hasItemVector(String item) {
        return itemVectors.getIfPresent(item) != null;
    }

    // --- reward model stats ---

    public RewardModelStats getRewardStats(String key) {
        return rewardStats.getIfPresent(key);
    }

    public void putRewardStats(String key, RewardModelStats stats) {
        rewardStats.put(key, stats);
    }

    public void invalidateRewardStats(String key) {
        rewardStats.invalidate(key);
    }

    public record RewardModelStats(long count, double rewardTotal) {}
}
