package com.demo.retrieval.service.retrieval;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.content.CatalogContentScoring;
import com.demo.retrieval.service.content.NormalizedProfile;
import com.demo.retrieval.service.filters.CandidateFilter;
import com.demo.retrieval.service.filters.CandidateFilterResult;
import com.demo.retrieval.service.filters.FilterContext;
import com.demo.retrieval.service.filters.PreviouslySeenMoviesBackupFilter;
import com.demo.retrieval.service.filters.PreviouslySeenMoviesFilter;
import com.demo.retrieval.service.filters.PreviouslyServedMoviesFilter;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Content-based candidate retrieval: fetch popular + cold-start candidates, filter, weight-sort/trim,
 * and dedup. Extracted verbatim from HybridRecommendationService; leans on {@link CatalogContentScoring}
 * for the normalized catalog + content score.
 */
public class ContentCandidateRetriever {

    private final StringRedisTemplate redis;
    private final RecommendationProperties properties;
    private final CatalogContentScoring catalogContentScoring;

    public ContentCandidateRetriever(StringRedisTemplate redis, RecommendationProperties properties,
                                     CatalogContentScoring catalogContentScoring) {
        this.redis = redis;
        this.properties = properties;
        this.catalogContentScoring = catalogContentScoring;
    }

    public RetrievalOutcome retrieve(
        ScoredMoviesQuery query,
        Map<String, Double> popularityMap,
        Set<String> excludedItems,
        Set<String> userGenres,
        Set<String> userTags,
        FilterContext filterCtx,
        int limit
    ) {
        List<MovieCandidate> retrieved = new ArrayList<>();
        retrieved.addAll(fetchPopularCandidates(popularityMap, userGenres, userTags));
        retrieved.addAll(fetchColdStartCandidates(excludedItems, userGenres, userTags, filterCtx, limit));

        List<MovieCandidate> kept = List.copyOf(retrieved);
        List<MovieCandidate> removed = new ArrayList<>();
        for (CandidateFilter filter : List.of(
                new PreviouslySeenMoviesFilter(),
                new PreviouslySeenMoviesBackupFilter(),
                new PreviouslyServedMoviesFilter())) {
            if (filter.enable(query)) {
                CandidateFilterResult result = filter.filter(query, kept);
                kept = result.kept();
                removed.addAll(result.removed());
            }
        }
        CandidateFilterResult eligible = filterEligibleCandidates(kept, excludedItems, filterCtx);
        kept = eligible.kept();
        removed.addAll(eligible.removed());

        List<MovieCandidate> scored = preRankCandidates(kept, limit);
        List<MovieCandidate> selected = selectDistinctCandidates(scored);
        return new RetrievalOutcome(List.copyOf(retrieved), List.copyOf(removed), scored, selected);
    }

    private List<MovieCandidate> fetchPopularCandidates(
        Map<String, Double> popularityMap, Set<String> userGenres, Set<String> userTags) {
        return popularityMap.entrySet().stream()
            .map(entry -> new MovieCandidate(
                entry.getKey(),
                entry.getValue(),
                contentScoreForItem(entry.getKey(), userGenres, userTags),
                false
            ))
            .toList();
    }

    private CandidateFilterResult filterEligibleCandidates(
        List<MovieCandidate> candidates, Set<String> excludedItems, FilterContext filterCtx) {
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate ->
                isEligibleCandidate(candidate, excludedItems, filterCtx)));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }

    private List<MovieCandidate> selectDistinctCandidates(List<MovieCandidate> candidates) {
        LinkedHashMap<String, MovieCandidate> selected = new LinkedHashMap<>();
        for (MovieCandidate candidate : candidates) {
            selected.merge(candidate.movieId(), candidate, MovieCandidate::merge);
        }
        return List.copyOf(selected.values());
    }

    private List<MovieCandidate> fetchColdStartCandidates(
        Set<String> excludedItems, Set<String> userGenres, Set<String> userTags,
        FilterContext filterCtx, int resultSize) {
        Map<String, MovieProfile> catalog = properties.getCatalog();
        if (catalog.isEmpty()) {
            return List.of();
        }

        Map<String, NormalizedProfile> normalizedCatalog = catalogContentScoring.normalizedCatalog();
        int poolSize = Math.max(1, properties.getCandidateGeneration().getColdStartPoolSize());
        int probeSize = Math.max(poolSize, Math.max(
            resultSize,
            poolSize * properties.getCandidateGeneration().getPopularityFetchMultiplier()
        ));
        List<MovieCandidate> probeCandidates = catalog.entrySet().stream()
            .filter(entry -> isEligibleCandidate(entry.getKey(), excludedItems, filterCtx))
            .map(entry -> {
                NormalizedProfile np = normalizedCatalog.get(entry.getKey());
                double cs = np == null ? 0.0 : catalogContentScoring.contentScore(np, userGenres, userTags);
                return new MovieCandidate(entry.getKey(), 0.0, cs, true);
            })
            .sorted(
                Comparator.comparing((MovieCandidate candidate) -> catalogContentScoring.isNewRelease(candidate.movieId())).reversed()
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
            .filter(candidate -> catalogContentScoring.isNewRelease(candidate.movieId()) || impressionMap.getOrDefault(candidate.movieId(), 0L) < threshold)
            .limit(poolSize)
            .toList();
    }

    private double contentScoreForItem(String itemId, Set<String> userGenres, Set<String> userTags) {
        NormalizedProfile profile = catalogContentScoring.normalizedCatalog().get(itemId);
        return profile == null ? 0.0 : catalogContentScoring.contentScore(profile, userGenres, userTags);
    }

    // Mirrors MovieRankingScorer + TopKMovieSelector from the Scala pipeline:
    // combines popularity and content signals into a pre-rank score, then bounds
    // the pool so only the top-N candidates flow into the expensive full-ranking phase.
    private List<MovieCandidate> preRankCandidates(List<MovieCandidate> candidates, int resultSize) {
        int configured = properties.getCandidateGeneration().getCandidatePoolSize();
        int poolSize = configured > 0 ? configured
            : resultSize * properties.getCandidateGeneration().getPopularityFetchMultiplier();
        double popWeight = properties.getBandit().getPopularityWeight();
        double contentWeight = properties.getBandit().getContentWeight();
        double totalWeight = popWeight + contentWeight;
        double normPop = totalWeight == 0.0 ? 0.5 : popWeight / totalWeight;
        double normContent = totalWeight == 0.0 ? 0.5 : contentWeight / totalWeight;
        return candidates.stream()
            .sorted(Comparator.comparingDouble(
                (MovieCandidate c) -> normPop * c.popularityScore()
                    + normContent * c.contentScore()
            ).reversed())
            .limit(poolSize)
            .toList();
    }

    private boolean isEligibleCandidate(String itemId, Set<String> recentItems, FilterContext filterCtx) {
        if (recentItems.contains(itemId)) {
            return false;
        }

        NormalizedProfile profile = catalogContentScoring.normalizedCatalog().get(itemId);
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

    private boolean isEligibleCandidate(MovieCandidate candidate, Set<String> recentItems, FilterContext filterCtx) {
        return isEligibleCandidate(candidate.movieId(), recentItems, filterCtx);
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
}
