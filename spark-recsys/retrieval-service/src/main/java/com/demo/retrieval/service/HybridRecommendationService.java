package com.demo.retrieval.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.stream.Stream;

@Service
public class HybridRecommendationService {
    private static final Logger log = LoggerFactory.getLogger(HybridRecommendationService.class);
    private static final String GLOBAL_POPULARITY_KEY = "global:item_popularity";
    private static final String METRICS_HASH_KEY = "bandit:metrics";
    private static final String REPLAY_BUFFER_KEY = "replay:recommendations";
    private static final String REPLAY_PENDING_PREFIX = "replay:pending:";
    private static final List<String> SUPPORTED_ALGORITHMS = List.of("ucb", "thompson");
    private static final String EXPOSED_ITEMS_KEY = "bandit:exposed_items";
    private static final int RECENT_HISTORY_SIZE = 50;
    private static final double WARM_PRIOR_STRENGTH = 2.0;

    private final StringRedisTemplate redis;
    private final RecommendationProperties properties;
    private final DeepLearningPredictionService predictionService;
    private final OnlineLearningService onlineLearningService;
    private final FeatureCache featureCache;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public HybridRecommendationService(
        StringRedisTemplate redis,
        RecommendationProperties properties,
        DeepLearningPredictionService predictionService,
        OnlineLearningService onlineLearningService,
        FeatureCache featureCache
    ) {
        this.redis = redis;
        this.properties = properties;
        this.predictionService = predictionService;
        this.onlineLearningService = onlineLearningService;
        this.featureCache = featureCache;
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

        // Warm item-vector cache for all candidates + recent history in one MGET, then the scoring
        // loop and user-vector fallback aggregation are pure cache reads with no Redis round-trips.
        batchWarmItemVectors(new ArrayList<>(eligible), recent);

        double[] userVector = resolveUserVector(user, recent);
        long totalImpressions = resolveLongMetric("recommendations_served");

        // Batch-fetch all impression + click counters (eliminates per-candidate GET N+1)
        List<String> eligibleList = new ArrayList<>(eligible);
        Map<String, long[]> counters = batchFetchCounters(eligibleList);
        Map<String, Double> dlScores = eligibleList.stream()
            .collect(Collectors.toMap(
                item -> item,
                item -> predictionService.predict(user, item).map(p -> p.score()).orElse(0.0)
            ));

        List<ScoredCandidate> scored = eligibleList.stream()
            .map(item -> {
                long[] c = counters.getOrDefault(item, new long[]{0L, 0L});
                return scoreCandidate(item, userVector, userGenres, userTags,
                    popularityMap.getOrDefault(item, 0.0), maxPopularity, totalImpressions, c[0], c[1],
                    dlScores.getOrDefault(item, 0.0));
            })
            .sorted(Comparator.comparingDouble(ScoredCandidate::banditScore).reversed())
            .collect(Collectors.toCollection(ArrayList::new));

        if (scored.isEmpty()) {
            return new RecommendationResult(
                user,
                recent,
                List.of(),
                List.of(),
                Map.of("eligibleCandidateCount", 0, "algorithm", currentAlgorithm())
            );
        }

        randomizeTopN(scored, properties.getCandidateGeneration().getTopNRandomizationPool());

        List<ScoredCandidate> selected = scored.stream().limit(limit).toList();
        List<String> candidateSnapshot = scored.stream()
            .limit(Math.max(limit, properties.getReplayBuffer().getCandidateSnapshotSize()))
            .map(ScoredCandidate::itemId)
            .toList();
        trackServedRecommendations(user, recent, userGenres, userTags, candidateSnapshot, selected);

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
                row.put("dlScore", round(candidate.dlScore()));
                row.put("rewardModelScore", round(candidate.onlineScore()));
                row.put("explorationBonus", round(candidate.explorationBonus()));
                row.put("banditScore", round(candidate.banditScore()));
                row.put("coldStart", candidate.coldStart());
                row.put("impressions", candidate.impressions());
                row.put("clicks", candidate.clicks());
                return row;
            })
            .toList();

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("algorithm", currentAlgorithm());
        metrics.put("eligibleCandidateCount", eligible.size());
        metrics.put("randomizationPool", Math.min(properties.getCandidateGeneration().getTopNRandomizationPool(), scored.size()));
        metrics.put("pseudoRegret", round(pseudoRegret));
        metrics.put("avgEstimatedReward", round(avgEstimatedReward));
        metrics.put("avgExplorationBonus", round(avgExplorationBonus));
        metrics.put("coldStartShare", round(coldStartShare));
        metrics.put("catalogCoverage", round(resolveCatalogCoverage()));

        return new RecommendationResult(user, recent, recommendations, diagnostics, metrics);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Map<String, Object> recordFeedback(FeedbackRequest request) {
        String itemId = request.item();
        if (itemId == null || itemId.isBlank()) {
            return Map.of("status", "ignored", "reason", "missing_item");
        }

        double reward = request.reward();
        MovieProfile profile = properties.getCatalog().get(itemId);
        String algorithm = currentAlgorithm();

        // Read-side: GET pending replay context before the pipeline (reads can't be pipelined with writes).
        String replayPayload = buildReplayPayload(request);

        // Collect keys invalidated by the pipeline so the in-memory cache is purged after the flush.
        List<String> cacheKeysToInvalidate = new ArrayList<>();

        // Batch ALL feedback writes into one pipeline: ~22+ round-trips → 1.
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) {
                if (request.clicked()) {
                    operations.opsForHash().increment(METRICS_HASH_KEY, "clicks", 1L);
                    operations.opsForHash().increment(metricsHashKey(algorithm), "clicks", 1L);
                    operations.opsForValue().increment("bandit:item:" + itemId + ":clicks", 1);
                }
                operations.opsForHash().increment(METRICS_HASH_KEY, "reward_total", reward);
                operations.opsForHash().increment(metricsHashKey(algorithm), "reward_total", reward);
                onlineLearningService.pipelineUpdate(operations, itemId, reward, profile, cacheKeysToInvalidate);
                if (replayPayload != null) {
                    operations.opsForList().rightPush(REPLAY_BUFFER_KEY, replayPayload);
                    operations.opsForList().trim(REPLAY_BUFFER_KEY, -(long) Math.max(1, properties.getReplayBuffer().getMaxSize()), -1L);
                }
                return null;
            }
        });

        cacheKeysToInvalidate.forEach(featureCache::invalidateRewardStats);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "ok");
        response.put("item", itemId);
        response.put("clicked", request.clicked());
        response.put("reward", reward);
        response.put("metrics", getAggregateMetrics());
        return response;
    }

    public Map<String, Object> getAggregateMetrics() {
        String algorithm = currentAlgorithm();
        Map<String, Object> metrics = buildAggregateMetricsForAlgorithm(algorithm);

        Map<String, Object> allAlgorithms = new LinkedHashMap<>();
        for (String candidate : supportedAlgorithms()) {
            Map<String, Object> candidateMetrics = buildAggregateMetricsForAlgorithm(candidate);
            if (hasRecordedActivity(candidateMetrics) || candidate.equals(algorithm)) {
                allAlgorithms.put(candidate, candidateMetrics);
            }
        }

        metrics.put("allAlgorithms", allAlgorithms);
        metrics.put("global", buildAggregateMetricsFromHash(METRICS_HASH_KEY, "all"));
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
        long clicks,
        double dlScore
    ) {
        MovieProfile profile = properties.getCatalog().get(itemId);

        double[] itemVector = resolveItemVector(itemId);
        double relevance = itemVector.length == 0 || userVector.length == 0 ? 0.0 : cosine(userVector, itemVector);
        double content = profile == null ? 0.0 : contentScore(profile, userGenres, userTags);
        double popularity = maxPopularity == 0.0 ? 0.0 : itemPopularity / maxPopularity;

        double offlineScore = (properties.getBandit().getRelevanceWeight() * normalizeScore(relevance))
            + (properties.getBandit().getContentWeight() * content)
            + (properties.getBandit().getPopularityWeight() * popularity)
            + (properties.getBandit().getDeepLearningWeight() * dlScore);
        double onlineScore = onlineLearningService.score(itemId, profile, offlineScore);
        double onlineWeight = clamp(properties.getRewardModel().getWeight());
        double learnedPrior = (offlineScore * (1.0 - onlineWeight)) + (onlineScore * onlineWeight);
        boolean coldStart = impressions < properties.getBandit().getColdStartExposureThreshold() || (profile != null && profile.isNewRelease());
        BanditArmScore armScore = computeBanditArmScore(learnedPrior, impressions, clicks, totalImpressions, coldStart);
        double estimatedReward = armScore.posteriorMean();
        double novelty = 1.0 / (impressions + 1.0);

        return new ScoredCandidate(
            itemId,
            estimatedReward,
            relevance,
            content,
            popularity,
            onlineScore,
            armScore.explorationBonus(),
            armScore.rankingScore(),
            coldStart,
            novelty,
            impressions,
            clicks,
            dlScore
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
    private void trackServedRecommendations(
        String user,
        List<String> recent,
        Set<String> userGenres,
        Set<String> userTags,
        List<String> candidateSnapshot,
        List<ScoredCandidate> selected
    ) {
        long now = System.currentTimeMillis();
        redis.executePipelined(new SessionCallback<>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                for (ScoredCandidate c : selected) {
                    operations.opsForValue().increment("bandit:item:" + c.itemId() + ":impressions", 1);
                    operations.opsForSet().add(EXPOSED_ITEMS_KEY, c.itemId());
                    operations.opsForValue().set("bandit:last_served:" + c.itemId(), String.valueOf(now));
                    serializeReplayContext(user, recent, userGenres, userTags, candidateSnapshot, c, now)
                        .ifPresent(payload -> operations.opsForValue().set(pendingReplayKey(user, c.itemId()), payload));
                }
                return null;
            }
        });
    }

    private Optional<String> serializeReplayContext(
        String user,
        List<String> recent,
        Set<String> userGenres,
        Set<String> userTags,
        List<String> candidateSnapshot,
        ScoredCandidate selected,
        long timestamp
    ) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "recommendation");
        event.put("user", user);
        event.put("context", Map.of(
            "recent", recent,
            "genres", userGenres,
            "tags", userTags
        ));
        event.put("candidates", candidateSnapshot);
        event.put("action", selected.itemId());
        event.put("estimatedReward", round(selected.estimatedReward()));
        event.put("onlineScore", round(selected.onlineScore()));
        event.put("banditScore", round(selected.banditScore()));
        event.put("coldStart", selected.coldStart());
        event.put("timestamp", timestamp);
        try {
            return Optional.of(objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize replay context for user {} item {}", user, selected.itemId(), e);
            return Optional.empty();
        }
    }

    // Reads the pending replay context (GET) and serializes the feedback event to JSON.
    // Must run before the write pipeline because reads can't be issued inside executePipelined.
    private String buildReplayPayload(FeedbackRequest request) {
        Map<String, Object> event = readPendingReplayContext(request.user(), request.item()).orElseGet(LinkedHashMap::new);
        event.putIfAbsent("type", "recommendation");
        event.putIfAbsent("user", request.user());
        event.putIfAbsent("action", request.item());
        event.put("clicked", request.clicked());
        event.put("reward", request.reward());
        event.put("feedbackTimestamp", System.currentTimeMillis());
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize replay event for user {} item {}", request.user(), request.item(), e);
            return null;
        }
    }

    private Optional<Map<String, Object>> readPendingReplayContext(String user, String item) {
        String raw = redis.opsForValue().get(pendingReplayKey(user, item));
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(raw, new TypeReference<>() {
            }));
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize pending replay context for user {} item {}", user, item, e);
            return Optional.empty();
        }
    }

    private String pendingReplayKey(String user, String item) {
        return REPLAY_PENDING_PREFIX + user + ":" + item;
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
        double[] cached = featureCache.getItemVector(item);
        if (cached != null) return cached;
        double[] vector = parseVector(redis.opsForValue().get(properties.getEmbeddings().getItemPrefix() + ":" + item));
        featureCache.putItemVector(item, vector);
        return vector;
    }

    private void batchWarmItemVectors(List<String> candidates, List<String> recentHistory) {
        String prefix = properties.getEmbeddings().getItemPrefix() + ":";
        List<String> coldItems = Stream.concat(candidates.stream(), recentHistory.stream())
            .distinct()
            .filter(item -> !featureCache.hasItemVector(item))
            .collect(Collectors.toCollection(ArrayList::new));
        if (coldItems.isEmpty()) return;
        List<String> keys = coldItems.stream().map(item -> prefix + item).toList();
        List<String> raw = Optional.ofNullable(redis.opsForValue().multiGet(keys))
            .orElseGet(() -> Collections.nCopies(coldItems.size(), null));
        for (int i = 0; i < coldItems.size(); i++) {
            featureCache.putItemVector(coldItems.get(i), parseVector(raw.get(i)));
        }
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
        redis.opsForHash().increment(metricsHashKey(currentAlgorithm()), field, amount);
    }

    private void incrementMetric(String field, double amount) {
        redis.opsForHash().increment(METRICS_HASH_KEY, field, amount);
        redis.opsForHash().increment(metricsHashKey(currentAlgorithm()), field, amount);
    }

    private long resolveLongMetric(String field) {
        Object value = redis.opsForHash().get(metricsHashKey(currentAlgorithm()), field);
        return readLong(value);
    }

    private Map<String, Object> buildAggregateMetricsForAlgorithm(String algorithm) {
        return buildAggregateMetricsFromHash(metricsHashKey(algorithm), algorithm);
    }

    private Map<String, Object> buildAggregateMetricsFromHash(String hashKey, String algorithm) {
        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(hashKey)).orElseGet(Map::of);
        Map<String, Object> metrics = new LinkedHashMap<>();
        long requests = readLong(raw.get("requests"));
        long served = readLong(raw.get("recommendations_served"));
        long clicks = readLong(raw.get("clicks"));
        double pseudoRegret = readDouble(raw.get("pseudo_regret_total"));
        double rewardTotal = readDouble(raw.get("reward_total"));
        double estimatedRewardTotal = readDouble(raw.get("estimated_reward_total"));
        double noveltyTotal = readDouble(raw.get("novelty_total"));

        metrics.put("algorithm", algorithm);
        metrics.put("requests", requests);
        metrics.put("recommendationsServed", served);
        metrics.put("clicks", clicks);
        metrics.put("ctr", served == 0 ? 0.0 : round((double) clicks / served));
        metrics.put("avgObservedReward", served == 0 ? 0.0 : round(rewardTotal / served));
        metrics.put("avgEstimatedReward", served == 0 ? 0.0 : round(estimatedRewardTotal / served));
        metrics.put("avgPseudoRegret", requests == 0 ? 0.0 : round(pseudoRegret / requests));
        metrics.put("cumulativePseudoRegret", round(pseudoRegret));
        metrics.put("avgNoveltyScore", served == 0 ? 0.0 : round(noveltyTotal / served));
        metrics.put("coldStartImpressions", readLong(raw.get("cold_start_impressions")));
        metrics.put("exploratoryImpressions", readLong(raw.get("exploratory_impressions")));
        metrics.put("catalogCoverage", round(resolveCatalogCoverage()));
        return metrics;
    }

    private boolean hasRecordedActivity(Map<String, Object> metrics) {
        Object requests = metrics.get("requests");
        return requests instanceof Number number && number.longValue() > 0L;
    }

    private List<String> supportedAlgorithms() {
        String current = currentAlgorithm();
        if (SUPPORTED_ALGORITHMS.contains(current)) {
            return SUPPORTED_ALGORITHMS;
        }

        List<String> algorithms = new ArrayList<>(SUPPORTED_ALGORITHMS);
        algorithms.add(current);
        return algorithms;
    }

    private String currentAlgorithm() {
        String algorithm = properties.getBandit().getAlgorithm();
        if (algorithm == null || algorithm.isBlank()) {
            return "ucb";
        }
        return algorithm.trim().toLowerCase(Locale.ROOT);
    }

    private String metricsHashKey(String algorithm) {
        return METRICS_HASH_KEY + ":" + algorithm;
    }

    private double resolveCatalogCoverage() {
        int catalogSize = properties.getCatalog().size();
        if (catalogSize == 0) {
            return 0.0;
        }
        Long exposed = redis.opsForSet().size(EXPOSED_ITEMS_KEY);
        return exposed == null ? 0.0 : (double) exposed / catalogSize;
    }

    private BanditArmScore computeBanditArmScore(
        double baseScore,
        long itemImpressions,
        long clicks,
        long totalImpressions,
        boolean coldStart
    ) {
        String algorithm = currentAlgorithm();
        long failures = Math.max(itemImpressions - clicks, 0L);
        double priorStrength = coldStart
            ? Math.max(WARM_PRIOR_STRENGTH, properties.getBandit().getColdStartBoost() * 4.0)
            : WARM_PRIOR_STRENGTH;
        double priorAlpha = 1.0 + (clamp(baseScore) * priorStrength);
        double priorBeta = 1.0 + ((1.0 - clamp(baseScore)) * priorStrength);
        double posteriorAlpha = priorAlpha + clicks;
        double posteriorBeta = priorBeta + failures;
        double posteriorMean = clamp(posteriorAlpha / (posteriorAlpha + posteriorBeta));

        if ("thompson".equals(algorithm)) {
            double sampledPosterior = clamp(sampleBeta(posteriorAlpha, posteriorBeta));
            double explorationMagnitude = Math.min(
                Math.abs(sampledPosterior - posteriorMean),
                properties.getBandit().getMaxExplorationBonus()
            );
            return new BanditArmScore(posteriorMean, explorationMagnitude, sampledPosterior);
        }

        double effectivePulls = itemImpressions + priorStrength;
        double confidence = Math.sqrt(Math.log(totalImpressions + 2.0) / (2.0 * (effectivePulls + 1.0)));
        double bonus = properties.getBandit().getExplorationAlpha() * confidence;
        if (coldStart) {
            bonus *= properties.getBandit().getColdStartBoost();
        }
        bonus = Math.min(bonus, properties.getBandit().getMaxExplorationBonus());
        return new BanditArmScore(posteriorMean, bonus, posteriorMean + bonus);
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
        double onlineScore,
        double explorationBonus,
        double banditScore,
        boolean coldStart,
        double noveltyScore,
        long impressions,
        long clicks,
        double dlScore
    ) {
    }

    private record BanditArmScore(
        double posteriorMean,
        double explorationBonus,
        double rankingScore
    ) {
    }

}
