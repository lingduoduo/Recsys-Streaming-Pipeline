package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Service
public class HybridRecommendationService {
    private static final Logger log = LoggerFactory.getLogger(HybridRecommendationService.class);
    private static final String GLOBAL_POPULARITY_KEY = "global:item_popularity";
    private static final String METRICS_HASH_KEY = "bandit:metrics";
    private static final String EXPOSED_ITEMS_KEY = "bandit:exposed_items";
    private static final int RECENT_HISTORY_SIZE = 50;

    private final StringRedisTemplate redis;
    private final RecommendationProperties properties;

    public HybridRecommendationService(StringRedisTemplate redis, RecommendationProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    public RecommendationResult recommend(String user, int limit) {
        List<String> recent;
        Set<ZSetOperations.TypedTuple<String>> popularWithScores;
        try {
            recent = Optional.ofNullable(redis.opsForList().range("user:" + user + ":recent", 0, RECENT_HISTORY_SIZE - 1L))
                .orElseGet(List::of);
            int fetchSize = Math.max(limit * properties.getCandidateGeneration().getPopularityFetchMultiplier(), limit);
            popularWithScores = Optional.ofNullable(
                redis.opsForZSet().reverseRangeWithScores(GLOBAL_POPULARITY_KEY, 0, fetchSize - 1L)
            ).orElseGet(Set::of);
        } catch (Exception e) {
            log.error("Redis fetch failed for user {}", user, e);
            return new RecommendationResult(user, List.of(), List.of(), List.of(), Map.of());
        }

        // Build popularity map from a single ZREVRANGE WITHSCORES call (eliminates per-item ZSCORE N+1)
        Map<String, Double> popularityMap = popularWithScores.stream()
            .filter(t -> t.getValue() != null && t.getScore() != null)
            .collect(Collectors.toMap(ZSetOperations.TypedTuple::getValue, t -> Math.log1p(t.getScore())));
        Set<String> popular = popularityMap.keySet();
        double maxPopularity = popularityMap.values().stream().mapToDouble(Double::doubleValue).max().orElse(0.0);

        Set<String> recentSet = new HashSet<>(recent);
        List<String> seedItems = recent.isEmpty() ? List.copyOf(popular) : recent;

        Set<String> userGenres = seedItems.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> normalize(p.getGenres()).stream())
            .collect(Collectors.toSet());
        Set<String> userTags = seedItems.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> normalize(p.getTags()).stream())
            .collect(Collectors.toSet());

        Set<String> contentCandidates = generateColdStartCandidates(recentSet, userGenres, userTags);

        LinkedHashSet<String> eligible = new LinkedHashSet<>();
        popular.stream().filter(item -> !recentSet.contains(item)).forEach(eligible::add);
        contentCandidates.stream().filter(item -> !recentSet.contains(item)).forEach(eligible::add);

        double[] userVector = resolveUserVector(user, recent);
        long totalImpressions = resolveLongMetric("recommendations_served");

        // Batch-fetch all impression + click counters (eliminates per-candidate GET N+1)
        List<String> eligibleList = new ArrayList<>(eligible);
        Map<String, long[]> counters = batchFetchCounters(eligibleList);

        List<ScoredCandidate> scored = eligibleList.stream()
            .map(item -> {
                long[] c = counters.getOrDefault(item, new long[]{0L, 0L});
                return scoreCandidate(item, userVector, userGenres, userTags,
                    popularityMap.getOrDefault(item, 0.0), maxPopularity, totalImpressions, c[0], c[1]);
            })
            .sorted(Comparator.comparingDouble(ScoredCandidate::banditScore).reversed())
            .collect(Collectors.toCollection(ArrayList::new));

        if (scored.isEmpty()) {
            return new RecommendationResult(
                user,
                recent,
                List.of(),
                List.of(),
                Map.of("eligibleCandidateCount", 0, "algorithm", properties.getBandit().getAlgorithm())
            );
        }

        randomizeTopN(scored, properties.getCandidateGeneration().getTopNRandomizationPool());

        List<ScoredCandidate> selected = scored.stream().limit(limit).toList();
        trackServedRecommendations(selected);

        List<String> recommendations = selected.stream().map(ScoredCandidate::itemId).toList();
        double pseudoRegret = computePseudoRegret(scored, selected, limit);
        double avgEstimatedReward = selected.stream().mapToDouble(ScoredCandidate::estimatedReward).average().orElse(0.0);
        double avgExplorationBonus = selected.stream().mapToDouble(ScoredCandidate::explorationBonus).average().orElse(0.0);
        long coldStartCount = selected.stream().filter(ScoredCandidate::coldStart).count();
        double coldStartShare = selected.isEmpty() ? 0.0 : (double) coldStartCount / selected.size();

        incrementMetric("requests", 1);
        incrementMetric("recommendations_served", selected.size());
        incrementMetric("exploratory_impressions", selected.stream().filter(c -> c.explorationBonus() > 0.0).count());
        incrementMetric("cold_start_impressions", coldStartCount);
        incrementMetric("estimated_reward_total", avgEstimatedReward * selected.size());
        incrementMetric("pseudo_regret_total", pseudoRegret);
        incrementMetric("novelty_total", selected.stream().mapToDouble(ScoredCandidate::noveltyScore).sum());

        List<Map<String, Object>> diagnostics = selected.stream()
            .map(candidate -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("item", candidate.itemId());
                row.put("estimatedReward", round(candidate.estimatedReward()));
                row.put("relevanceScore", round(candidate.relevanceScore()));
                row.put("contentScore", round(candidate.contentScore()));
                row.put("explorationBonus", round(candidate.explorationBonus()));
                row.put("banditScore", round(candidate.banditScore()));
                row.put("coldStart", candidate.coldStart());
                row.put("impressions", candidate.impressions());
                row.put("clicks", candidate.clicks());
                return row;
            })
            .toList();

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("algorithm", properties.getBandit().getAlgorithm());
        metrics.put("eligibleCandidateCount", eligible.size());
        metrics.put("randomizationPool", Math.min(properties.getCandidateGeneration().getTopNRandomizationPool(), scored.size()));
        metrics.put("pseudoRegret", round(pseudoRegret));
        metrics.put("avgEstimatedReward", round(avgEstimatedReward));
        metrics.put("avgExplorationBonus", round(avgExplorationBonus));
        metrics.put("coldStartShare", round(coldStartShare));
        metrics.put("catalogCoverage", round(resolveCatalogCoverage()));

        return new RecommendationResult(user, recent, recommendations, diagnostics, metrics);
    }

    public Map<String, Object> recordFeedback(FeedbackRequest request) {
        String itemId = request.item();
        if (itemId == null || itemId.isBlank()) {
            return Map.of("status", "ignored", "reason", "missing_item");
        }

        double reward = request.reward();
        if (request.clicked()) {
            incrementMetric("clicks", 1);
            incrementItemCounter(itemId, "clicks", 1);
        }
        incrementMetric("reward_total", reward);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "ok");
        response.put("item", itemId);
        response.put("clicked", request.clicked());
        response.put("reward", reward);
        response.put("metrics", getAggregateMetrics());
        return response;
    }

    public Map<String, Object> getAggregateMetrics() {
        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(METRICS_HASH_KEY)).orElseGet(Map::of);
        Map<String, Object> metrics = new LinkedHashMap<>();
        long requests = readLong(raw.get("requests"));
        long served = readLong(raw.get("recommendations_served"));
        long clicks = readLong(raw.get("clicks"));
        double pseudoRegret = readDouble(raw.get("pseudo_regret_total"));
        double rewardTotal = readDouble(raw.get("reward_total"));
        double estimatedRewardTotal = readDouble(raw.get("estimated_reward_total"));
        double noveltyTotal = readDouble(raw.get("novelty_total"));

        metrics.put("requests", requests);
        metrics.put("recommendationsServed", served);
        metrics.put("clicks", clicks);
        metrics.put("ctr", served == 0 ? 0.0 : round((double) clicks / served));
        metrics.put("avgObservedReward", served == 0 ? 0.0 : round(rewardTotal / served));
        metrics.put("avgEstimatedReward", served == 0 ? 0.0 : round(estimatedRewardTotal / served));
        metrics.put("avgPseudoRegret", requests == 0 ? 0.0 : round(pseudoRegret / requests));
        metrics.put("avgNoveltyScore", served == 0 ? 0.0 : round(noveltyTotal / served));
        metrics.put("coldStartImpressions", readLong(raw.get("cold_start_impressions")));
        metrics.put("exploratoryImpressions", readLong(raw.get("exploratory_impressions")));
        metrics.put("catalogCoverage", round(resolveCatalogCoverage()));
        return metrics;
    }

    private Set<String> generateColdStartCandidates(Set<String> excludedItems, Set<String> userGenres, Set<String> userTags) {
        Map<String, MovieProfile> catalog = properties.getCatalog();
        if (catalog.isEmpty()) {
            return Set.of();
        }

        // Collect non-excluded candidate IDs, then batch-fetch their impression counts
        List<String> candidateIds = catalog.keySet().stream()
            .filter(id -> !excludedItems.contains(id))
            .toList();

        List<String> impressionKeys = candidateIds.stream()
            .map(id -> "bandit:item:" + id + ":impressions")
            .toList();
        List<String> impressionValues = Optional.ofNullable(redis.opsForValue().multiGet(impressionKeys))
            .orElseGet(() -> Collections.nCopies(candidateIds.size(), null));

        Map<String, Long> impressionMap = new LinkedHashMap<>();
        for (int i = 0; i < candidateIds.size(); i++) {
            impressionMap.put(candidateIds.get(i), readLong(impressionValues.get(i)));
        }

        int threshold = properties.getBandit().getColdStartExposureThreshold();
        return candidateIds.stream()
            .filter(id -> {
                MovieProfile p = catalog.get(id);
                return p.isNewRelease() || impressionMap.getOrDefault(id, 0L) < threshold;
            })
            .sorted(Comparator.comparingDouble((String id) ->
                contentScore(catalog.get(id), userGenres, userTags)).reversed())
            .limit(properties.getCandidateGeneration().getColdStartPoolSize())
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private ScoredCandidate scoreCandidate(
        String itemId,
        double[] userVector,
        Set<String> userGenres,
        Set<String> userTags,
        double itemPopularity,
        double maxPopularity,
        long totalImpressions,
        long impressions,
        long clicks
    ) {
        MovieProfile profile = properties.getCatalog().get(itemId);

        double[] itemVector = resolveItemVector(itemId);
        double relevance = itemVector.length == 0 || userVector.length == 0 ? 0.0 : cosine(userVector, itemVector);
        double content = profile == null ? 0.0 : contentScore(profile, userGenres, userTags);
        double popularity = maxPopularity == 0.0 ? 0.0 : itemPopularity / maxPopularity;

        double baseScore = (properties.getBandit().getRelevanceWeight() * normalizeScore(relevance))
            + (properties.getBandit().getContentWeight() * content)
            + (properties.getBandit().getPopularityWeight() * popularity);
        boolean coldStart = impressions < properties.getBandit().getColdStartExposureThreshold() || (profile != null && profile.isNewRelease());
        double exploration = computeExplorationBonus(impressions, clicks, totalImpressions, coldStart);

        // Warm items: blend empirical CTR (30%) with model base score (70%); cold-start: trust model only
        double empiricalCtr = impressions > 0 ? clamp((double) clicks / impressions) : 0.0;
        double estimatedReward = impressions > 0
            ? clamp(0.7 * baseScore + 0.3 * empiricalCtr)
            : clamp(baseScore);
        double novelty = 1.0 / (impressions + 1.0);

        return new ScoredCandidate(
            itemId,
            estimatedReward,
            relevance,
            content,
            popularity,
            exploration,
            baseScore + exploration,
            coldStart,
            novelty,
            impressions,
            clicks
        );
    }

    private void randomizeTopN(List<ScoredCandidate> scored, int topN) {
        if (scored.size() < 2 || topN <= 1) {
            return;
        }

        int boundary = Math.min(topN, scored.size());
        List<ScoredCandidate> randomized = new ArrayList<>(scored.subList(0, boundary));
        Collections.shuffle(randomized, ThreadLocalRandom.current());
        for (int i = 0; i < boundary; i++) {
            scored.set(i, randomized.get(i));
        }
    }

    @SuppressWarnings("unchecked")
    private void trackServedRecommendations(List<ScoredCandidate> selected) {
        // Pipeline all writes into a single round-trip instead of 3 × |selected| sequential calls
        long now = System.currentTimeMillis();
        redis.executePipelined(new SessionCallback<>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                for (ScoredCandidate c : selected) {
                    operations.opsForValue().increment("bandit:item:" + c.itemId() + ":impressions", 1);
                    operations.opsForSet().add(EXPOSED_ITEMS_KEY, c.itemId());
                    operations.opsForValue().set("bandit:last_served:" + c.itemId(), String.valueOf(now));
                }
                return null;
            }
        });
    }

    // Batch-fetch impression + click counters for a list of item IDs in two mget calls
    private Map<String, long[]> batchFetchCounters(List<String> itemIds) {
        if (itemIds.isEmpty()) {
            return Map.of();
        }
        List<String> impressionKeys = itemIds.stream().map(id -> "bandit:item:" + id + ":impressions").toList();
        List<String> clickKeys = itemIds.stream().map(id -> "bandit:item:" + id + ":clicks").toList();

        List<String> allKeys = new ArrayList<>(impressionKeys.size() + clickKeys.size());
        allKeys.addAll(impressionKeys);
        allKeys.addAll(clickKeys);

        List<String> values = Optional.ofNullable(redis.opsForValue().multiGet(allKeys))
            .orElseGet(() -> Collections.nCopies(allKeys.size(), null));

        Map<String, long[]> result = new LinkedHashMap<>();
        for (int i = 0; i < itemIds.size(); i++) {
            long impressions = readLong(values.get(i));
            long clicks = readLong(values.get(itemIds.size() + i));
            result.put(itemIds.get(i), new long[]{impressions, clicks});
        }
        return result;
    }

    private double[] resolveUserVector(String user, List<String> recent) {
        String key = properties.getEmbeddings().getUserPrefix() + ":" + user;
        double[] userVector = parseVector(redis.opsForValue().get(key));
        if (userVector.length > 0) {
            return userVector;
        }

        List<double[]> vectors = recent.stream()
            .map(this::resolveItemVector)
            .filter(vector -> vector.length > 0)
            .toList();
        if (vectors.isEmpty()) {
            return new double[0];
        }

        int dimension = vectors.get(0).length;
        double[] aggregate = new double[dimension];
        for (double[] vector : vectors) {
            for (int i = 0; i < Math.min(dimension, vector.length); i++) {
                aggregate[i] += vector[i];
            }
        }
        for (int i = 0; i < dimension; i++) {
            aggregate[i] /= vectors.size();
        }
        return aggregate;
    }

    private double[] resolveItemVector(String item) {
        return parseVector(redis.opsForValue().get(properties.getEmbeddings().getItemPrefix() + ":" + item));
    }

    private double[] parseVector(String raw) {
        if (raw == null || raw.isBlank()) {
            return new double[0];
        }

        String[] parts = raw.trim().split("\\s+");
        double[] vector = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                vector[i] = Double.parseDouble(parts[i]);
            } catch (NumberFormatException e) {
                return new double[0];
            }
        }
        return vector;
    }

    private void incrementItemCounter(String itemId, String counter, long amount) {
        redis.opsForValue().increment("bandit:item:" + itemId + ":" + counter, amount);
    }

    private void incrementMetric(String field, long amount) {
        redis.opsForHash().increment(METRICS_HASH_KEY, field, amount);
    }

    private void incrementMetric(String field, double amount) {
        redis.opsForHash().increment(METRICS_HASH_KEY, field, amount);
    }

    private long resolveLongMetric(String field) {
        Object value = redis.opsForHash().get(METRICS_HASH_KEY, field);
        return readLong(value);
    }

    private double resolveCatalogCoverage() {
        int catalogSize = properties.getCatalog().size();
        if (catalogSize == 0) {
            return 0.0;
        }
        Long exposed = redis.opsForSet().size(EXPOSED_ITEMS_KEY);
        return exposed == null ? 0.0 : (double) exposed / catalogSize;
    }

    private double computeExplorationBonus(long itemImpressions, long clicks, long totalImpressions, boolean coldStart) {
        String algorithm = properties.getBandit().getAlgorithm().toLowerCase(Locale.ROOT);
        double alpha = properties.getBandit().getExplorationAlpha();
        double maxBonus = properties.getBandit().getMaxExplorationBonus();

        double bonus;
        if ("thompson".equals(algorithm)) {
            // Optimistic Beta prior for cold start: Beta(clicks+2, failures+1) shifts mean toward success
            double alphaPrior = coldStart ? clicks + 2.0 : clicks + 1.0;
            double betaPrior = Math.max(itemImpressions - clicks, 0) + 1.0;
            bonus = alpha * sampleBeta(alphaPrior, betaPrior);
        } else {
            // UCB1: coefficient sqrt(2) is the standard; cold start items earn a larger confidence interval
            double confidence = Math.sqrt(2.0 * Math.log(totalImpressions + 2.0) / (itemImpressions + 1.0));
            bonus = alpha * confidence * (coldStart ? properties.getBandit().getColdStartBoost() : 1.0);
        }
        return Math.min(bonus, maxBonus);
    }

    private double contentScore(MovieProfile profile, Set<String> userGenres, Set<String> userTags) {
        Set<String> genres = normalize(profile.getGenres());
        Set<String> tags = normalize(profile.getTags());
        double genreOverlap = overlapRatio(userGenres, genres);
        double tagOverlap = overlapRatio(userTags, tags);
        return clamp((genreOverlap * 0.7) + (tagOverlap * 0.3));
    }

    private Set<String> normalize(List<String> values) {
        return values == null ? Set.of() : values.stream()
            .filter(value -> value != null && !value.isBlank())
            .map(value -> value.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    }

    private double overlapRatio(Set<String> left, Set<String> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return 0.0;
        }
        Set<String> intersection = new HashSet<>(left);
        intersection.retainAll(right);
        Set<String> union = new HashSet<>(left);
        union.addAll(right);
        return union.isEmpty() ? 0.0 : (double) intersection.size() / union.size();
    }

    private double cosine(double[] left, double[] right) {
        if (left.length == 0 || right.length == 0 || left.length != right.length) {
            return 0.0;
        }
        double dot = 0.0;
        double leftNorm = 0.0;
        double rightNorm = 0.0;
        for (int i = 0; i < left.length; i++) {
            dot += left[i] * right[i];
            leftNorm += left[i] * left[i];
            rightNorm += right[i] * right[i];
        }
        if (leftNorm == 0.0 || rightNorm == 0.0) {
            return 0.0;
        }
        return dot / (Math.sqrt(leftNorm) * Math.sqrt(rightNorm));
    }

    private double normalizeScore(double raw) {
        return clamp((raw + 1.0) / 2.0);
    }

    private double clamp(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }

    private long readLong(Object raw) {
        if (raw == null) {
            return 0L;
        }
        try {
            return Long.parseLong(raw.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private double readDouble(Object raw) {
        if (raw == null) {
            return 0.0;
        }
        try {
            return Double.parseDouble(raw.toString());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private double computePseudoRegret(List<ScoredCandidate> ranked, List<ScoredCandidate> selected, int limit) {
        List<ScoredCandidate> oracle = ranked.stream()
            .sorted(Comparator.comparingDouble(ScoredCandidate::estimatedReward).reversed())
            .limit(limit)
            .toList();

        double oracleReward = oracle.stream().mapToDouble(ScoredCandidate::estimatedReward).sum();
        double selectedReward = selected.stream().mapToDouble(ScoredCandidate::estimatedReward).sum();
        return Math.max(0.0, oracleReward - selectedReward);
    }

    private double round(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    private double sampleBeta(double alpha, double beta) {
        double x = sampleGamma(alpha);
        double y = sampleGamma(beta);
        return (x <= 0.0 && y <= 0.0) ? 0.5 : x / (x + y);
    }

    private double sampleGamma(double shape) {
        if (shape <= 0.0) {
            return 0.0;
        }
        if (shape < 1.0) {
            double u = ThreadLocalRandom.current().nextDouble();
            return sampleGamma(shape + 1.0) * Math.pow(u, 1.0 / shape);
        }

        double d = shape - (1.0 / 3.0);
        double c = 1.0 / Math.sqrt(9.0 * d);
        while (true) {
            double x = ThreadLocalRandom.current().nextGaussian();
            double v = 1.0 + (c * x);
            if (v <= 0.0) {
                continue;
            }
            v = v * v * v;
            double u = ThreadLocalRandom.current().nextDouble();
            if (u < 1.0 - (0.0331 * x * x * x * x)) {
                return d * v;
            }
            if (Math.log(u) < (0.5 * x * x) + d * (1.0 - v + Math.log(v))) {
                return d * v;
            }
        }
    }

    private record ScoredCandidate(
        String itemId,
        double estimatedReward,
        double relevanceScore,
        double contentScore,
        double popularityScore,
        double explorationBonus,
        double banditScore,
        boolean coldStart,
        double noveltyScore,
        long impressions,
        long clicks
    ) {
    }
}
