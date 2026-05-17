package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import org.springframework.lang.NonNull;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Online learning model: maintains per-item/genre/tag reward statistics in Redis,
 * updated in real-time from the feedback stream. Provides score estimates that
 * improve as more interaction data accumulates.
 */
@Service
public class OnlineLearningService {
    private static final String GLOBAL_KEY = "reward-model:global";
    private static final String ITEM_PREFIX = "reward-model:item:";
    private static final String GENRE_PREFIX = "reward-model:genre:";
    private static final String TAG_PREFIX = "reward-model:tag:";

    private final StringRedisTemplate redis;
    private final RecommendationProperties properties;
    private final FeatureCache featureCache;

    public OnlineLearningService(StringRedisTemplate redis, RecommendationProperties properties, FeatureCache featureCache) {
        this.redis = redis;
        this.properties = properties;
        this.featureCache = featureCache;
    }

    /** Score a (item, profile) pair using accumulated reward statistics. Falls back to {@code fallback} when no data exists. */
    public double score(String itemId, MovieProfile profile, double fallback) {
        double weightedReward = 0.0;
        double totalWeight = 0.0;

        RewardEstimate global = readRewardEstimate(GLOBAL_KEY);
        if (global.count() > 0) {
            double weight = properties.getRewardModel().getGlobalWeight() * confidence(global.count());
            weightedReward += global.mean() * weight;
            totalWeight += weight;
        }

        RewardEstimate item = readRewardEstimate(ITEM_PREFIX + itemId);
        if (item.count() > 0) {
            double weight = properties.getRewardModel().getItemWeight() * confidence(item.count());
            weightedReward += item.mean() * weight;
            totalWeight += weight;
        }

        if (profile != null) {
            RewardEstimate genre = aggregateFeatureEstimates(GENRE_PREFIX, normalize(profile.getGenres()));
            if (genre.count() > 0) {
                double weight = properties.getRewardModel().getGenreWeight() * confidence(genre.count());
                weightedReward += genre.mean() * weight;
                totalWeight += weight;
            }

            RewardEstimate tag = aggregateFeatureEstimates(TAG_PREFIX, normalize(profile.getTags()));
            if (tag.count() > 0) {
                double weight = properties.getRewardModel().getTagWeight() * confidence(tag.count());
                weightedReward += tag.mean() * weight;
                totalWeight += weight;
            }
        }

        return totalWeight == 0.0 ? clamp(fallback) : clamp(weightedReward / totalWeight);
    }

    /** Update model parameters from a real-time feedback signal (online learning step). */
    public void update(String itemId, double reward, MovieProfile profile) {
        incrementRewardStats(GLOBAL_KEY, reward);
        incrementRewardStats(ITEM_PREFIX + itemId, reward);

        if (profile == null) {
            return;
        }
        normalize(profile.getGenres()).forEach(genre -> incrementRewardStats(GENRE_PREFIX + genre, reward));
        normalize(profile.getTags()).forEach(tag -> incrementRewardStats(TAG_PREFIX + tag, reward));
    }

    /**
     * Write reward stat increments into a caller-managed Redis pipeline.
     * Avoids opening a new connection per feedback event; all writes are queued
     * in the pipeline and flushed by the caller in one round-trip.
     * The keys that were written are added to {@code keysToInvalidate} so the
     * caller can purge them from the in-memory cache after the pipeline flushes.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void pipelineUpdate(
            org.springframework.data.redis.core.RedisOperations ops,
            String itemId, double reward, MovieProfile profile,
            java.util.List<String> keysToInvalidate) {
        pipelineIncrement(ops, GLOBAL_KEY, reward, keysToInvalidate);
        pipelineIncrement(ops, ITEM_PREFIX + itemId, reward, keysToInvalidate);
        if (profile == null) return;
        normalize(profile.getGenres()).forEach(g -> pipelineIncrement(ops, GENRE_PREFIX + g, reward, keysToInvalidate));
        normalize(profile.getTags()).forEach(t -> pipelineIncrement(ops, TAG_PREFIX + t, reward, keysToInvalidate));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void pipelineIncrement(
            org.springframework.data.redis.core.RedisOperations ops,
            String key, double reward, java.util.List<String> keysToInvalidate) {
        ops.opsForHash().increment(key, "count", 1L);
        ops.opsForHash().increment(key, "reward_total", reward);
        keysToInvalidate.add(key);
    }

    private RewardEstimate aggregateFeatureEstimates(String prefix, Set<String> features) {
        long count = 0L;
        double rewardTotal = 0.0;
        for (String feature : features) {
            RewardEstimate estimate = readRewardEstimate(prefix + feature);
            count += estimate.count();
            rewardTotal += estimate.mean() * estimate.count();
        }
        return count == 0L ? new RewardEstimate(0L, 0.0) : new RewardEstimate(count, rewardTotal / count);
    }

    private RewardEstimate readRewardEstimate(@NonNull String key) {
        FeatureCache.RewardModelStats cached = featureCache.getRewardStats(key);
        if (cached != null) {
            return cached.count() == 0L
                ? new RewardEstimate(0L, 0.0)
                : new RewardEstimate(cached.count(), clamp(cached.rewardTotal() / cached.count()));
        }
        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(key)).orElseGet(Map::of);
        long count = readLong(raw.get("count"));
        double rewardTotal = readDouble(raw.get("reward_total"));
        featureCache.putRewardStats(key, new FeatureCache.RewardModelStats(count, rewardTotal));
        return count == 0L ? new RewardEstimate(0L, 0.0) : new RewardEstimate(count, clamp(rewardTotal / count));
    }

    private void incrementRewardStats(@NonNull String key, double reward) {
        redis.opsForHash().increment(key, "count", 1L);
        redis.opsForHash().increment(key, "reward_total", reward);
        featureCache.invalidateRewardStats(key);
    }

    private double confidence(long count) {
        int minCount = Math.max(1, properties.getRewardModel().getMinFeatureCount());
        return Math.min(1.0, (double) count / minCount);
    }

    private Set<String> normalize(List<String> values) {
        return values == null ? Set.of() : values.stream()
            .filter(v -> v != null && !v.isBlank())
            .map(v -> v.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    }

    private double clamp(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }

    private long readLong(Object raw) {
        if (raw == null) return 0L;
        try { return Long.parseLong(raw.toString()); } catch (NumberFormatException e) { return 0L; }
    }

    private double readDouble(Object raw) {
        if (raw == null) return 0.0;
        try { return Double.parseDouble(raw.toString()); } catch (NumberFormatException e) { return 0.0; }
    }

    private record RewardEstimate(long count, double mean) {}
}
