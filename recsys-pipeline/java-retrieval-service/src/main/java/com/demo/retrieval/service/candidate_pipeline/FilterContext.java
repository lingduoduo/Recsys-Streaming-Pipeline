package com.demo.retrieval.service.candidate_pipeline;

import java.util.Set;

public record FilterContext(
    Set<String> blockedUsers,
    Set<String> mutedProductTypes,
    Set<String> mutedGenres,
    Set<String> mutedKeywords,
    Set<String> mutedLanguageCodes,
    Set<String> blockedVisibilityReasons,
    boolean dropAncillaryCandidates,
    boolean dropBlockedQuotes,
    boolean requireMediaCandidates,
    boolean dropAuthorsBlockingViewer
) {
    public FilterContext {
        blockedUsers = blockedUsers == null ? Set.of() : Set.copyOf(blockedUsers);
        mutedProductTypes = mutedProductTypes == null ? Set.of() : Set.copyOf(mutedProductTypes);
        mutedGenres = mutedGenres == null ? Set.of() : Set.copyOf(mutedGenres);
        mutedKeywords = mutedKeywords == null ? Set.of() : Set.copyOf(mutedKeywords);
        mutedLanguageCodes = mutedLanguageCodes == null ? Set.of() : Set.copyOf(mutedLanguageCodes);
        blockedVisibilityReasons = blockedVisibilityReasons == null ? Set.of() : Set.copyOf(blockedVisibilityReasons);
    }

    public static FilterContext empty() {
        return new FilterContext(Set.of(), Set.of(), Set.of(), Set.of(), Set.of(), Set.of(), false, false, false, false);
    }
}
