package com.demo.retrieval.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.Filtering;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
    private static final List<String> SUPPORTED_ALGORITHMS = List.of("ucb", "thompson", "q-learning", "sarsa");
    private static final String EXPOSED_ITEMS_KEY = "bandit:exposed_items";
    private static final int RECENT_HISTORY_SIZE = 50;
    private static final double WARM_PRIOR_STRENGTH = 2.0;
    private static final ThreadLocal<MessageDigest> SHA256_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    });

    private record FilterContext(
        Set<String> blockedUsers,
        Set<String> mutedProductTypes,
        Set<String> mutedGenres,
        Set<String> mutedKeywords) {}

    // Per-item data normalized once per catalog lifetime (productType, title lowercased+trimmed;
    // tags+keywords merged into allKeywords so intersection checks need no per-call allocation).
    private record NormalizedProfile(
        String productType,
        Set<String> genres,
        Set<String> allKeywords,
        String title,
        long expiresAtEpochMillis) {}

    private record CatalogCache(
        Map<String, MovieProfile> source,
        Map<String, NormalizedProfile> normalized) {}

    private record MovieCandidate(
        String movieId,
        double popularityScore,
        double contentScore,
        boolean coldStartSource) {}

    private record CandidatePipelineContext(
        ScoredMoviesQuery query,
        Map<String, Double> popularityMap,
        Set<String> excludedItems,
        Set<String> userGenres,
        Set<String> userTags,
        FilterContext filterCtx,
        int resultSize) {}

    private record CandidateFilterResult(
        List<MovieCandidate> kept,
        List<MovieCandidate> removed) {}

    private record CandidateSelectResult(
        List<MovieCandidate> selected,
        List<MovieCandidate> nonSelected) {}

    private record CandidateGenerationResult(
        List<MovieCandidate> retrievedCandidates,
        List<MovieCandidate> filteredCandidates,
        List<MovieCandidate> scoredCandidates,
        List<MovieCandidate> selectedCandidates) {}

    @FunctionalInterface
    private interface CandidateSource {
        List<MovieCandidate> fetch(CandidatePipelineContext context);

        default boolean enable(CandidatePipelineContext context) {
            return true;
        }

        default String name() {
            return getClass().getSimpleName();
        }
    }

    @FunctionalInterface
    private interface CandidateHydrator {
        List<MovieCandidate> hydrate(CandidatePipelineContext context, List<MovieCandidate> candidates);

        default boolean enable(CandidatePipelineContext context) {
            return true;
        }
    }

    @FunctionalInterface
    private interface CandidateFilter {
        CandidateFilterResult filter(CandidatePipelineContext context, List<MovieCandidate> candidates);

        default boolean enable(CandidatePipelineContext context) {
            return true;
        }
    }

    @FunctionalInterface
    private interface CandidateScorer {
        List<MovieCandidate> score(CandidatePipelineContext context, List<MovieCandidate> candidates);

        default boolean enable(CandidatePipelineContext context) {
            return true;
        }
    }

    @FunctionalInterface
    private interface CandidateSelector {
        CandidateSelectResult select(CandidatePipelineContext context, List<MovieCandidate> candidates);

        default boolean enable(CandidatePipelineContext context) {
            return true;
        }
    }

    @FunctionalInterface
    private interface CandidateSideEffect {
        void run(CandidatePipelineContext context, List<MovieCandidate> selected, List<MovieCandidate> nonSelected);

        default boolean enable(CandidatePipelineContext context) {
            return true;
        }
    }

    private volatile CatalogCache catalogCache;

    private final StringRedisTemplate redis;
    private final RecommendationProperties properties;
    private final DeepLearningPredictionService predictionService;
    private final OnlineLearningService onlineLearningService;
    private final FeatureCache featureCache;
    private final List<QueryHydrator<ScoredMoviesQuery>> queryHydrators;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public HybridRecommendationService(
        StringRedisTemplate redis,
        RecommendationProperties properties,
        DeepLearningPredictionService predictionService,
        OnlineLearningService onlineLearningService,
        FeatureCache featureCache,
        List<QueryHydrator<ScoredMoviesQuery>> queryHydrators
    ) {
        this.redis = redis;
        this.properties = properties;
        this.predictionService = predictionService;
        this.onlineLearningService = onlineLearningService;
        this.featureCache = featureCache;
        this.queryHydrators = List.copyOf(queryHydrators);
    }

    public RecommendationResult recommend(String user, int limit) {
        FilterContext filterCtx = buildFilterContext();
        if (isBlockedUser(user, filterCtx)) {
            return new RecommendationResult(
                user,
                List.of(),
                List.of(),
                List.of(),
                Map.of("eligibleCandidateCount", 0, "algorithm", currentAlgorithm(), "filterReason", "blocked_user")
            );
        }

        ScoredMoviesQuery query = ScoredMoviesQuery.forUser(user);
        ScoredMoviesQuery hydratedQuery;
        Set<ZSetOperations.TypedTuple<String>> popularWithScores;
        try {
            hydratedQuery = hydrateQuery(query);
            int fetchSize = Math.max(limit * properties.getCandidateGeneration().getPopularityFetchMultiplier(), limit);
            popularWithScores = Optional.ofNullable(
                redis.opsForZSet().reverseRangeWithScores(GLOBAL_POPULARITY_KEY, 0, fetchSize - 1L)
            ).orElseGet(Set::of);
        } catch (Exception e) {
            log.error("Recommendation fetch failed for user {}", user, e);
            return new RecommendationResult(user, List.of(), List.of(), List.of(), Map.of());
        }
        List<String> recent = hydratedQuery.watchedMovieIds();
        List<String> rated = hydratedQuery.ratedMovieIds();
        MovieLensUserFeatures userFeatures = hydratedQuery.userFeatures();

        // Build popularity map from a single ZREVRANGE WITHSCORES call (eliminates per-item ZSCORE N+1)
        Map<String, Double> popularityMap = popularWithScores.stream()
            .filter(t -> t.getValue() != null && t.getScore() != null)
            .collect(Collectors.toMap(
                ZSetOperations.TypedTuple::getValue,
                t -> Math.log1p(t.getScore()),
                Math::max,
                LinkedHashMap::new
            ));
        Set<String> popular = popularityMap.keySet();
        double maxPopularity = popularityMap.values().stream().mapToDouble(Double::doubleValue).max().orElse(0.0);

        Set<String> historySet = new HashSet<>(recent);
        historySet.addAll(rated);
        historySet.addAll(userFeatures.recentlyRatedMovieIds());
        List<String> seedItems = recent.isEmpty() ? (rated.isEmpty() ? List.copyOf(popular) : rated) : recent;

        Set<String> userGenres = seedItems.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> normalize(p.getGenres()).stream())
            .collect(Collectors.toCollection(LinkedHashSet::new));
        userGenres.addAll(normalize(userFeatures.favoriteGenres()));
        Set<String> userTags = seedItems.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> normalize(p.getTags()).stream())
            .collect(Collectors.toSet());
        Map<String, Object> state = buildState(recent, userGenres, userTags);
        String stateKey = stateKey(state);

        CandidateGenerationResult candidateGeneration = generateCandidates(
            hydratedQuery,
            popularityMap,
            historySet,
            userGenres,
            userTags,
            filterCtx,
            limit
        );
        List<String> eligibleList = candidateGeneration.selectedCandidates().stream()
            .map(MovieCandidate::movieId)
            .toList();

        // Warm item-vector cache for all candidates + recent history in one MGET, then the scoring
        // loop and user-vector fallback aggregation are pure cache reads with no Redis round-trips.
        batchWarmItemVectors(eligibleList, recent);

        double[] userVector = resolveUserVector(user, recent);
        long totalImpressions = resolveLongMetric("recommendations_served");

        // Batch-fetch all impression + click counters (eliminates per-candidate GET N+1)
        Map<String, long[]> counters = batchFetchCounters(eligibleList);
        Map<String, Double> qValues = isTabularRl()
            ? batchFetchQValues(stateKey, eligibleList)
            : Map.of();
        Map<String, Double> dlScores = predictionService.predictBatch(user, eligibleList);

        // Warm reward-stats cache for all candidates in one pipeline flush so every
        // subsequent onlineLearningService.score() call is a pure in-memory read.
        onlineLearningService.batchWarmRewardStats(eligibleList, properties.getCatalog());

        List<ScoredCandidate> scored = eligibleList.stream()
            .map(item -> {
                long[] c = counters.getOrDefault(item, new long[]{0L, 0L});
                return scoreCandidate(item, userVector, userGenres, userTags,
                    popularityMap.getOrDefault(item, 0.0), maxPopularity, totalImpressions, c[0], c[1],
                    dlScores.getOrDefault(item, 0.0), qValues.get(item));
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

        String requestId = UUID.randomUUID().toString();
        List<ScoredCandidate> selected = scored.stream().limit(limit).toList();
        List<ScoredCandidate> candidateSnapshot = scored.stream()
            .limit(Math.max(limit, properties.getReplayBuffer().getCandidateSnapshotSize()))
            .toList();
        trackServedRecommendations(requestId, user, recent, userGenres, userTags, candidateSnapshot, selected);

        List<String> recommendations = selected.stream().map(ScoredCandidate::itemId).toList();
        double pseudoRegret = computePseudoRegret(scored, selected, limit);
        double avgEstimatedReward = selected.stream().mapToDouble(ScoredCandidate::estimatedReward).average().orElse(0.0);
        double avgExplorationBonus = selected.stream().mapToDouble(ScoredCandidate::explorationBonus).average().orElse(0.0);
        long coldStartCount = selected.stream().filter(ScoredCandidate::coldStart).count();
        double coldStartShare = selected.isEmpty() ? 0.0 : (double) coldStartCount / selected.size();
        long exploratoryCount = selected.stream().filter(c -> c.explorationBonus() > 0.0).count();
        double noveltyTotal = selected.stream().mapToDouble(ScoredCandidate::noveltyScore).sum();
        long servedCount = selected.size();
        double estimatedRewardTotal = avgEstimatedReward * servedCount;
        String algoKey = metricsHashKey(currentAlgorithm());
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForHash().increment(METRICS_HASH_KEY, "requests", 1L);
                operations.opsForHash().increment(algoKey, "requests", 1L);
                operations.opsForHash().increment(METRICS_HASH_KEY, "recommendations_served", servedCount);
                operations.opsForHash().increment(algoKey, "recommendations_served", servedCount);
                operations.opsForHash().increment(METRICS_HASH_KEY, "exploratory_impressions", exploratoryCount);
                operations.opsForHash().increment(algoKey, "exploratory_impressions", exploratoryCount);
                operations.opsForHash().increment(METRICS_HASH_KEY, "cold_start_impressions", coldStartCount);
                operations.opsForHash().increment(algoKey, "cold_start_impressions", coldStartCount);
                operations.opsForHash().increment(METRICS_HASH_KEY, "estimated_reward_total", estimatedRewardTotal);
                operations.opsForHash().increment(algoKey, "estimated_reward_total", estimatedRewardTotal);
                operations.opsForHash().increment(METRICS_HASH_KEY, "pseudo_regret_total", pseudoRegret);
                operations.opsForHash().increment(algoKey, "pseudo_regret_total", pseudoRegret);
                operations.opsForHash().increment(METRICS_HASH_KEY, "novelty_total", noveltyTotal);
                operations.opsForHash().increment(algoKey, "novelty_total", noveltyTotal);
                return null;
            }
        });

        List<Map<String, Object>> diagnostics = selected.stream()
            .map(candidate -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("item", candidate.itemId());
                row.put("estimatedReward", round(candidate.estimatedReward()));
                row.put("relevanceScore", round(candidate.relevanceScore()));
                row.put("contentScore", round(candidate.contentScore()));
                row.put("dlScore", round(candidate.dlScore()));
                row.put("qValue", round(candidate.qValue()));
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
        metrics.put("retrievedCandidateCount", candidateGeneration.retrievedCandidates().size());
        metrics.put("filteredCandidateCount", candidateGeneration.filteredCandidates().size());
        metrics.put("scoredCandidateCount", candidateGeneration.scoredCandidates().size());
        metrics.put("eligibleCandidateCount", eligibleList.size());
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
        Map<String, Object> replayEvent = readPendingReplayContext(request.user(), request.item()).orElseGet(LinkedHashMap::new);
        TabularRlUpdate tabularRlUpdate = isTabularRl()
            ? buildTabularRlUpdate(request, replayEvent)
            : null;
        String replayPayload = buildReplayPayload(request, replayEvent);

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
                if (tabularRlUpdate != null) {
                    operations.opsForHash().put(tabularRlUpdate.qKey(), tabularRlUpdate.action(), String.valueOf(tabularRlUpdate.updatedValue()));
                    operations.opsForHash().increment(metricsHashKey(algorithm), "q_updates", 1L);
                    operations.opsForHash().increment(metricsHashKey(algorithm), "q_td_error_total", tabularRlUpdate.tdError());
                }
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

    private ScoredMoviesQuery hydrateQuery(ScoredMoviesQuery query) {
        ScoredMoviesQuery current = query;
        for (QueryHydrator<ScoredMoviesQuery> hydrator : queryHydrators) {
            if (!hydrator.enable(query)) continue;
            ScoredMoviesQuery hydrated = hydrator.hydrate(current);
            current = hydrator.update(current, hydrated);
        }
        return current;
    }

    private CandidateGenerationResult generateCandidates(
        ScoredMoviesQuery query,
        Map<String, Double> popularityMap,
        Set<String> excludedItems,
        Set<String> userGenres,
        Set<String> userTags,
        FilterContext filterCtx,
        int limit
    ) {
        CandidatePipelineContext context = new CandidatePipelineContext(
            query,
            popularityMap,
            excludedItems,
            userGenres,
            userTags,
            filterCtx,
            limit
        );

        List<MovieCandidate> retrieved = fetchCandidates(context, List.of(
            this::fetchPopularCandidates,
            this::fetchColdStartCandidates
        ));
        List<MovieCandidate> hydrated = runCandidateHydrators(context, retrieved, List.of());
        CandidateFilterResult filterResult = runCandidateFilters(context, hydrated, List.of(this::filterEligibleCandidates));
        List<MovieCandidate> scored = runCandidateScorers(context, filterResult.kept(), List.of(this::preRankCandidates));
        CandidateSelectResult selectResult = selectCandidates(context, scored, this::selectDistinctCandidates);
        runCandidateSideEffects(context, selectResult.selected(), selectResult.nonSelected(), List.of());
        return new CandidateGenerationResult(hydrated, filterResult.removed(), scored, selectResult.selected());
    }

    private List<MovieCandidate> fetchCandidates(CandidatePipelineContext context, List<CandidateSource> sources) {
        return sources.stream()
            .filter(source -> source.enable(context))
            .flatMap(source -> source.fetch(context).stream())
            .toList();
    }

    private List<MovieCandidate> runCandidateHydrators(
        CandidatePipelineContext context,
        List<MovieCandidate> candidates,
        List<CandidateHydrator> hydrators
    ) {
        List<MovieCandidate> current = candidates;
        for (CandidateHydrator hydrator : hydrators) {
            if (hydrator.enable(context)) {
                current = hydrator.hydrate(context, current);
            }
        }
        return current;
    }

    private CandidateFilterResult runCandidateFilters(
        CandidatePipelineContext context,
        List<MovieCandidate> candidates,
        List<CandidateFilter> filters
    ) {
        List<MovieCandidate> current = candidates;
        List<MovieCandidate> removedAll = new ArrayList<>();
        for (CandidateFilter filter : filters) {
            if (filter.enable(context)) {
                CandidateFilterResult result = filter.filter(context, current);
                current = result.kept();
                removedAll.addAll(result.removed());
            }
        }
        return new CandidateFilterResult(current, List.copyOf(removedAll));
    }

    private List<MovieCandidate> runCandidateScorers(
        CandidatePipelineContext context,
        List<MovieCandidate> candidates,
        List<CandidateScorer> scorers
    ) {
        List<MovieCandidate> current = candidates;
        for (CandidateScorer scorer : scorers) {
            if (scorer.enable(context)) {
                current = scorer.score(context, current);
            }
        }
        return current;
    }

    private CandidateSelectResult selectCandidates(
        CandidatePipelineContext context,
        List<MovieCandidate> candidates,
        CandidateSelector selector
    ) {
        return selector.enable(context)
            ? selector.select(context, candidates)
            : new CandidateSelectResult(candidates, List.of());
    }

    private void runCandidateSideEffects(
        CandidatePipelineContext context,
        List<MovieCandidate> selected,
        List<MovieCandidate> nonSelected,
        List<CandidateSideEffect> sideEffects
    ) {
        sideEffects.stream()
            .filter(sideEffect -> sideEffect.enable(context))
            .forEach(sideEffect -> sideEffect.run(context, selected, nonSelected));
    }

    private List<MovieCandidate> fetchPopularCandidates(CandidatePipelineContext context) {
        return context.popularityMap().entrySet().stream()
            .map(entry -> new MovieCandidate(
                entry.getKey(),
                entry.getValue(),
                contentScoreForItem(entry.getKey(), context.userGenres(), context.userTags()),
                false
            ))
            .toList();
    }

    private CandidateFilterResult filterEligibleCandidates(CandidatePipelineContext context, List<MovieCandidate> candidates) {
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate ->
                isEligibleCandidate(candidate.movieId(), context.excludedItems(), context.filterCtx())));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }

    private CandidateSelectResult selectDistinctCandidates(CandidatePipelineContext context, List<MovieCandidate> candidates) {
        LinkedHashMap<String, MovieCandidate> selected = new LinkedHashMap<>();
        List<MovieCandidate> nonSelected = new ArrayList<>();
        for (MovieCandidate candidate : candidates) {
            boolean duplicate = selected.containsKey(candidate.movieId());
            selected.merge(candidate.movieId(), candidate, this::mergeCandidate);
            if (duplicate) {
                nonSelected.add(candidate);
            }
        }
        return new CandidateSelectResult(List.copyOf(selected.values()), List.copyOf(nonSelected));
    }

    private MovieCandidate mergeCandidate(MovieCandidate left, MovieCandidate right) {
        return new MovieCandidate(
            left.movieId(),
            Math.max(left.popularityScore(), right.popularityScore()),
            Math.max(left.contentScore(), right.contentScore()),
            left.coldStartSource() || right.coldStartSource()
        );
    }

    private List<MovieCandidate> fetchColdStartCandidates(CandidatePipelineContext context) {
        Map<String, MovieProfile> catalog = properties.getCatalog();
        if (catalog.isEmpty()) {
            return List.of();
        }

        int poolSize = Math.max(1, properties.getCandidateGeneration().getColdStartPoolSize());
        int probeSize = Math.max(poolSize, Math.max(
            context.resultSize(),
            poolSize * properties.getCandidateGeneration().getPopularityFetchMultiplier()
        ));
        List<MovieCandidate> probeCandidates = catalog.entrySet().stream()
            .filter(entry -> isEligibleCandidate(entry.getKey(), context.excludedItems(), context.filterCtx()))
            .map(entry -> new MovieCandidate(
                entry.getKey(),
                0.0,
                contentScore(entry.getValue(), context.userGenres(), context.userTags()),
                true
            ))
            .sorted(
                Comparator.comparing((MovieCandidate candidate) -> isNewRelease(candidate.movieId())).reversed()
                    .thenComparing(Comparator.comparingDouble(MovieCandidate::contentScore).reversed())
            )
            .limit(probeSize)
            .toList();

        List<String> impressionKeys = probeCandidates.stream()
            .map(candidate -> "bandit:item:" + candidate.movieId() + ":impressions")
            .toList();
        List<String> impressionValues = Optional.ofNullable(redis.opsForValue().multiGet(impressionKeys))
            .orElseGet(() -> Collections.nCopies(probeCandidates.size(), null));

        Map<String, Long> impressionMap = new LinkedHashMap<>();
        for (int i = 0; i < probeCandidates.size(); i++) {
            impressionMap.put(probeCandidates.get(i).movieId(), readLong(impressionValues.get(i)));
        }

        int threshold = properties.getBandit().getColdStartExposureThreshold();
        return probeCandidates.stream()
            .filter(candidate -> isNewRelease(candidate.movieId()) || impressionMap.getOrDefault(candidate.movieId(), 0L) < threshold)
            .limit(poolSize)
            .toList();
    }

    private boolean isNewRelease(String itemId) {
        MovieProfile profile = properties.getCatalog().get(itemId);
        return profile != null && profile.isNewRelease();
    }

    private double contentScoreForItem(String itemId, Set<String> userGenres, Set<String> userTags) {
        MovieProfile profile = properties.getCatalog().get(itemId);
        return profile == null ? 0.0 : contentScore(profile, userGenres, userTags);
    }

    // Mirrors MovieRankingScorer + TopKMovieSelector from the Scala pipeline:
    // combines popularity and content signals into a pre-rank score, then bounds
    // the pool so only the top-N candidates flow into the expensive full-ranking phase.
    private List<MovieCandidate> preRankCandidates(CandidatePipelineContext context, List<MovieCandidate> candidates) {
        int configured = properties.getCandidateGeneration().getCandidatePoolSize();
        int poolSize = configured > 0 ? configured
            : context.resultSize() * properties.getCandidateGeneration().getPopularityFetchMultiplier();
        double popWeight = properties.getBandit().getPopularityWeight();
        double contentWeight = properties.getBandit().getContentWeight();
        double totalWeight = popWeight + contentWeight;
        double normPop = totalWeight == 0.0 ? 0.5 : popWeight / totalWeight;
        double normContent = totalWeight == 0.0 ? 0.5 : contentWeight / totalWeight;
        return candidates.stream()
            .sorted(Comparator.comparingDouble(
                (MovieCandidate c) -> normPop * c.popularityScore() + normContent * c.contentScore()
            ).reversed())
            .limit(poolSize)
            .toList();
    }

    private Map<String, NormalizedProfile> getNormalizedCatalog() {
        Map<String, MovieProfile> catalog = properties.getCatalog();
        CatalogCache cache = catalogCache;
        if (cache != null && cache.source() == catalog) {
            return cache.normalized();
        }
        Map<String, NormalizedProfile> built = new HashMap<>(catalog.size() * 4 / 3 + 1);
        catalog.forEach((id, p) -> {
            Set<String> allKeywords = new HashSet<>(normalize(p.getTags()));
            allKeywords.addAll(normalize(p.getKeywords()));
            built.put(id, new NormalizedProfile(
                normalizeValue(p.getProductType()),
                normalize(p.getGenres()),
                Collections.unmodifiableSet(allKeywords),
                normalizeValue(p.getTitle()),
                p.getExpiresAtEpochMillis()
            ));
        });
        synchronized (this) {
            CatalogCache c2 = catalogCache;
            if (c2 != null && c2.source() == catalog) {
                return c2.normalized();
            }
            CatalogCache newCache = new CatalogCache(catalog, Collections.unmodifiableMap(built));
            catalogCache = newCache;
            return newCache.normalized();
        }
    }

    private FilterContext buildFilterContext() {
        Filtering f = properties.getFiltering();
        if (!f.isEnabled()) {
            return new FilterContext(Set.of(), Set.of(), Set.of(), Set.of());
        }
        return new FilterContext(
            normalize(f.getBlockedUsers()),
            normalize(f.getMutedProductTypes()),
            normalize(f.getMutedGenres()),
            normalize(f.getMutedKeywords())
        );
    }

    private boolean isBlockedUser(String user, FilterContext filterCtx) {
        return !filterCtx.blockedUsers().isEmpty() && filterCtx.blockedUsers().contains(normalizeValue(user));
    }

    private boolean isEligibleCandidate(String itemId, Set<String> recentItems, FilterContext filterCtx) {
        if (recentItems.contains(itemId)) {
            return false;
        }

        NormalizedProfile profile = getNormalizedCatalog().get(itemId);
        if (profile == null) {
            return true;
        }

        if (profile.expiresAtEpochMillis() > 0 && profile.expiresAtEpochMillis() <= System.currentTimeMillis()) {
            return false;
        }

        if (!filterCtx.mutedProductTypes().isEmpty() && filterCtx.mutedProductTypes().contains(profile.productType())) {
            return false;
        }

        if (!filterCtx.mutedGenres().isEmpty() && !Collections.disjoint(filterCtx.mutedGenres(), profile.genres())) {
            return false;
        }

        if (filterCtx.mutedKeywords().isEmpty()) {
            return true;
        }
        if (!Collections.disjoint(filterCtx.mutedKeywords(), profile.allKeywords())) {
            return false;
        }
        return filterCtx.mutedKeywords().stream().noneMatch(k -> !k.isBlank() && profile.title().contains(k));
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
        double dlScore,
        Double qValue
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
        double qLearningScore = computeTabularRlRankingScore(learnedPrior, qValue);
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
            isTabularRl() ? qLearningScore : armScore.rankingScore(),
            coldStart,
            novelty,
            impressions,
            clicks,
            dlScore,
            qValue == null ? 0.0 : qValue
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
        String requestId,
        String user,
        List<String> recent,
        Set<String> userGenres,
        Set<String> userTags,
        List<ScoredCandidate> candidateSnapshot,
        List<ScoredCandidate> selected
    ) {
        long now = System.currentTimeMillis();
        redis.executePipelined(new SessionCallback<>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                for (int i = 0; i < selected.size(); i++) {
                    ScoredCandidate c = selected.get(i);
                    operations.opsForValue().increment("bandit:item:" + c.itemId() + ":impressions", 1);
                    operations.opsForSet().add(EXPOSED_ITEMS_KEY, c.itemId());
                    operations.opsForValue().set("bandit:last_served:" + c.itemId(), String.valueOf(now));
                    serializeReplayContext(requestId, user, recent, userGenres, userTags, candidateSnapshot, c, i, selected.size(), now)
                        .ifPresent(payload -> operations.opsForValue().set(pendingReplayKey(user, c.itemId()), payload));
                }
                return null;
            }
        });
    }

    private Optional<String> serializeReplayContext(
        String requestId,
        String user,
        List<String> recent,
        Set<String> userGenres,
        Set<String> userTags,
        List<ScoredCandidate> candidateSnapshot,
        ScoredCandidate selected,
        int actionPosition,
        int slateSize,
        long timestamp
    ) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "rl_experience");
        event.put("schemaVersion", 1);
        event.put("requestId", requestId);
        event.put("user", user);
        event.put("state", buildState(recent, userGenres, userTags));
        event.put("context", event.get("state"));
        event.put("actionSpace", candidateSnapshot.stream().map(this::candidateFeatures).toList());
        event.put("candidates", candidateSnapshot.stream().map(ScoredCandidate::itemId).toList());
        event.put("action", selected.itemId());
        event.put("actionPosition", actionPosition);
        event.put("slateSize", slateSize);
        event.put("policy", Map.of(
            "name", currentAlgorithm(),
            "rankingScore", round(selected.banditScore()),
            "explorationBonus", round(selected.explorationBonus()),
            "propensity", round(slateSize <= 0 ? 0.0 : 1.0 / slateSize)
        ));
        event.put("modelPredictions", modelPredictions(selected));
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

    // Must run before the write pipeline: reads cannot be issued inside executePipelined.
    private String buildReplayPayload(FeedbackRequest request, Map<String, Object> event) {
        event.putIfAbsent("type", "rl_experience");
        event.putIfAbsent("schemaVersion", 1);
        event.putIfAbsent("user", request.user());
        event.putIfAbsent("action", request.item());
        event.put("clicked", request.clicked());
        event.put("reward", request.reward());
        event.put("feedbackTimestamp", System.currentTimeMillis());
        event.put("nextState", buildCurrentState(request.user()));
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize replay event for user {} item {}", request.user(), request.item(), e);
            return null;
        }
    }

    private Map<String, Object> buildCurrentState(String user) {
        List<String> recent = Optional.ofNullable(redis.opsForList().range("user:" + user + ":recent", 0, RECENT_HISTORY_SIZE - 1L))
            .orElseGet(List::of);
        Set<String> genres = recent.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> normalize(p.getGenres()).stream())
            .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> tags = recent.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> normalize(p.getTags()).stream())
            .collect(Collectors.toCollection(LinkedHashSet::new));
        return buildState(recent, genres, tags);
    }

    private Map<String, Object> buildState(List<String> recent, Set<String> userGenres, Set<String> userTags) {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("recent", recent);
        state.put("genres", userGenres.stream().sorted().toList());
        state.put("tags", userTags.stream().sorted().toList());
        return state;
    }

    private Map<String, Object> candidateFeatures(ScoredCandidate candidate) {
        Map<String, Object> features = new LinkedHashMap<>();
        features.put("item", candidate.itemId());
        features.put("modelPredictions", modelPredictions(candidate));
        features.put("coldStart", candidate.coldStart());
        features.put("impressions", candidate.impressions());
        features.put("clicks", candidate.clicks());
        return features;
    }

    private Map<String, Object> modelPredictions(ScoredCandidate candidate) {
        Map<String, Object> predictions = new LinkedHashMap<>();
        predictions.put("deepLearningScore", round(candidate.dlScore()));
        predictions.put("qValue", round(candidate.qValue()));
        predictions.put("rewardModelScore", round(candidate.onlineScore()));
        predictions.put("estimatedReward", round(candidate.estimatedReward()));
        predictions.put("relevanceScore", round(candidate.relevanceScore()));
        predictions.put("contentScore", round(candidate.contentScore()));
        predictions.put("popularityScore", round(candidate.popularityScore()));
        predictions.put("banditScore", round(candidate.banditScore()));
        return predictions;
    }

    private TabularRlUpdate buildTabularRlUpdate(FeedbackRequest request, Map<String, Object> replayEvent) {
        Object rawState = replayEvent.get("state");
        if (!(rawState instanceof Map<?, ?> state)) {
            return null;
        }

        String action = request.item();
        String qKey = qKeyForStateMap(state);
        double currentQ = readDouble(redis.opsForHash().get(qKey, action));
        Map<String, Object> nextState = buildCurrentState(request.user());
        NextActionValue nextActionValue = nextActionValue(nextState);
        double alpha = clamp(properties.getBandit().getQLearningAlpha());
        double gamma = clamp(properties.getBandit().getQLearningGamma());
        double tdTarget = request.reward() + (gamma * nextActionValue.value());
        double tdError = tdTarget - currentQ;
        double updated = currentQ + (alpha * tdError);
        replayEvent.put("nextAction", nextActionValue.action());
        return new TabularRlUpdate(qKey, action, updated, tdError);
    }

    private Map<String, Double> batchFetchQValues(String stateKey, List<String> itemIds) {
        if (itemIds.isEmpty()) {
            return Map.of();
        }

        List<Object> fields = new ArrayList<>(itemIds);
        List<Object> values = Optional.ofNullable(redis.opsForHash().multiGet(qKeyForState(stateKey), fields))
            .orElseGet(() -> Collections.nCopies(itemIds.size(), null));

        Map<String, Double> result = new LinkedHashMap<>();
        for (int i = 0; i < itemIds.size(); i++) {
            Object raw = values.get(i);
            if (raw != null) {
                result.put(itemIds.get(i), readDouble(raw));
            }
        }
        return result;
    }

    private NextActionValue nextActionValue(Map<String, Object> nextState) {
        String qKey = qKeyForStateMap(nextState);
        if ("sarsa".equals(currentAlgorithm())) {
            return chooseSarsaNextAction(nextState, qKey);
        }
        return new NextActionValue(null, maxQValue(qKey));
    }

    private NextActionValue chooseSarsaNextAction(Map<String, Object> nextState, String qKey) {
        List<String> actionSpace = nextActionSpace(nextState);
        if (actionSpace.isEmpty()) {
            return new NextActionValue(null, 0.0);
        }

        if (ThreadLocalRandom.current().nextDouble() < clamp(properties.getBandit().getQLearningEpsilon())) {
            String action = actionSpace.get(ThreadLocalRandom.current().nextInt(actionSpace.size()));
            return new NextActionValue(action, readDouble(redis.opsForHash().get(qKey, action)));
        }

        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(qKey)).orElseGet(Map::of);
        String bestAction = actionSpace.get(0);
        double bestValue = readDouble(raw.get(bestAction));
        for (String action : actionSpace) {
            double value = readDouble(raw.get(action));
            if (value > bestValue) {
                bestAction = action;
                bestValue = value;
            }
        }
        return new NextActionValue(bestAction, bestValue);
    }

    private List<String> nextActionSpace(Map<String, Object> nextState) {
        Object rawRecent = nextState.get("recent");
        Set<String> recent = rawRecent instanceof List<?> values
            ? values.stream().map(String::valueOf).collect(Collectors.toSet())
            : Set.of();
        List<String> actions = properties.getCatalog().keySet().stream()
            .filter(item -> !recent.contains(item))
            .toList();
        return actions.isEmpty() ? List.copyOf(properties.getCatalog().keySet()) : actions;
    }

    private double maxQValue(String qKey) {
        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(qKey)).orElseGet(Map::of);
        return raw.values().stream()
            .mapToDouble(this::readDouble)
            .max()
            .orElse(0.0);
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

    private boolean isTabularRl() {
        return "q-learning".equals(currentAlgorithm()) || "sarsa".equals(currentAlgorithm());
    }

    private String metricsHashKey(String algorithm) {
        return METRICS_HASH_KEY + ":" + algorithm;
    }

    private String qKeyForStateMap(Map<?, ?> state) {
        return qKeyForState(stateKey(state));
    }

    private String qKeyForState(String stateKey) {
        return currentAlgorithm() + ":q:" + stateKey;
    }

    private String stateKey(Object state) {
        try {
            String canonical = objectMapper.writeValueAsString(state);
            byte[] hash = SHA256_DIGEST.get().digest(canonical.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        } catch (JsonProcessingException e) {
            log.warn("Failed to build Q-learning state key; falling back to hashCode", e);
            return Integer.toHexString(String.valueOf(state).hashCode());
        }
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

    private double computeTabularRlRankingScore(double learnedPrior, Double qValue) {
        if (ThreadLocalRandom.current().nextDouble() < clamp(properties.getBandit().getQLearningEpsilon())) {
            return ThreadLocalRandom.current().nextDouble();
        }
        return qValue == null ? learnedPrior : qValue;
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
            .map(this::normalizeValue)
            .filter(value -> !value.isBlank())
            .collect(Collectors.toSet());
    }

    private String normalizeValue(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT).trim();
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
        double dlScore,
        double qValue
    ) {
    }

    private record TabularRlUpdate(
        String qKey,
        String action,
        double updatedValue,
        double tdError
    ) {
    }

    private record NextActionValue(
        String action,
        double value
    ) {
    }

    private record BanditArmScore(
        double posteriorMean,
        double explorationBonus,
        double rankingScore
    ) {
    }

}
