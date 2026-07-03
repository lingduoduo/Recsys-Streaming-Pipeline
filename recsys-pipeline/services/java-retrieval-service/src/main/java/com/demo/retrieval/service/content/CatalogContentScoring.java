package com.demo.retrieval.service.content;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.RecommendationConstants;
import com.demo.retrieval.service.text.TextNormalization;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Shared normalized-catalog + content-scoring core, used by both the candidate retriever and the
 * scorer. Owns the normalized-catalog cache (keyed on catalog-map identity) and the genre/tag
 * Jaccard content score. Extracted verbatim from HybridRecommendationService.
 */
public class CatalogContentScoring {

    private final RecommendationProperties properties;
    private volatile CatalogCache catalogCache;

    public CatalogContentScoring(RecommendationProperties properties) {
        this.properties = properties;
    }

    public Map<String, NormalizedProfile> normalizedCatalog() {
        Map<String, MovieProfile> catalog = properties.getCatalog();
        CatalogCache cache = catalogCache;
        if (cache != null && cache.source() == catalog) {
            return cache.normalized();
        }
        Map<String, NormalizedProfile> built = new HashMap<>(catalog.size() * 4 / 3 + 1);
        catalog.forEach((id, p) -> {
            Set<String> normalizedTags = TextNormalization.normalize(p.getTags());
            Set<String> allKeywords = new HashSet<>(normalizedTags);
            allKeywords.addAll(TextNormalization.normalize(p.getKeywords()));
            built.put(id, new NormalizedProfile(
                TextNormalization.normalizeValue(p.getProductType()),
                TextNormalization.normalize(p.getGenres()),
                Collections.unmodifiableSet(normalizedTags),
                Collections.unmodifiableSet(allKeywords),
                TextNormalization.normalizeValue(p.getTitle()),
                p.isNewRelease(),
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

    public NormalizedProfile profileFor(String itemId) {
        return normalizedCatalog().get(itemId);
    }

    public boolean isNewRelease(String itemId) {
        NormalizedProfile profile = profileFor(itemId);
        return profile != null && profile.newRelease();
    }

    public double contentScore(NormalizedProfile profile, Set<String> userGenres, Set<String> userTags) {
        return RecommendationConstants.clamp(
            (overlapRatio(userGenres, profile.genres()) * RecommendationConstants.CONTENT_GENRE_WEIGHT)
            + (overlapRatio(userTags, profile.tags()) * RecommendationConstants.CONTENT_TAG_WEIGHT));
    }

    private double overlapRatio(Set<String> left, Set<String> right) {
        if (left.isEmpty() || right.isEmpty()) return 0.0;
        Set<String> smaller = left.size() <= right.size() ? left : right;
        Set<String> larger  = left.size() <= right.size() ? right : left;
        int intersectionSize = 0;
        for (String s : smaller) {
            if (larger.contains(s)) intersectionSize++;
        }
        int unionSize = left.size() + right.size() - intersectionSize;
        return unionSize == 0 ? 0.0 : (double) intersectionSize / unionSize;
    }

    private record CatalogCache(
        Map<String, MovieProfile> source,
        Map<String, NormalizedProfile> normalized) {
    }
}
