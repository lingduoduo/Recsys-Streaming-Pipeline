package com.demo.retrieval.service.audit;

import com.demo.retrieval.service.content.NormalizedProfile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Inverted index over the normalized catalog: genre → item ids, tag → item ids. Built once per
 * audit call and shared across every user, so resolving a preference is one hash lookup instead
 * of a catalog scan per user per preference. Posting lists are sorted so output is deterministic.
 */
public final class CatalogPreferenceIndex {
    public static final String KIND_GENRE = "genre";
    public static final String KIND_TAG = "tag";

    private final Map<String, List<String>> byGenre;
    private final Map<String, List<String>> byTag;
    private final int catalogSize;

    private CatalogPreferenceIndex(Map<String, List<String>> byGenre, Map<String, List<String>> byTag, int catalogSize) {
        this.byGenre = byGenre;
        this.byTag = byTag;
        this.catalogSize = catalogSize;
    }

    public static CatalogPreferenceIndex build(Map<String, NormalizedProfile> normalizedCatalog) {
        Map<String, List<String>> byGenre = new HashMap<>();
        Map<String, List<String>> byTag = new HashMap<>();
        normalizedCatalog.forEach((itemId, profile) -> {
            profile.genres().forEach(genre -> byGenre.computeIfAbsent(genre, g -> new ArrayList<>()).add(itemId));
            profile.tags().forEach(tag -> byTag.computeIfAbsent(tag, t -> new ArrayList<>()).add(itemId));
        });
        byGenre.values().forEach(Collections::sort);
        byTag.values().forEach(Collections::sort);
        byGenre.replaceAll((k, v) -> List.copyOf(v));
        byTag.replaceAll((k, v) -> List.copyOf(v));
        return new CatalogPreferenceIndex(byGenre, byTag, normalizedCatalog.size());
    }

    /** Sorted item ids carrying {@code value} as a {@code kind}; empty for a miss or an unknown kind. */
    public List<String> lookup(String kind, String value) {
        if (value == null) {
            return List.of();
        }
        Map<String, List<String>> postings = switch (kind == null ? "" : kind) {
            case KIND_GENRE -> byGenre;
            case KIND_TAG -> byTag;
            default -> Map.of();
        };
        return postings.getOrDefault(value, List.of());
    }

    public int catalogSize() {
        return catalogSize;
    }
}
