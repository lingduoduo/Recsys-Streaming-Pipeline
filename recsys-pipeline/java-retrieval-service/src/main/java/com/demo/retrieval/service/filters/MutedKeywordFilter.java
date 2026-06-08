package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MutedKeywordFilter implements CandidateFilter {
    private final Set<String> mutedKeywords;

    public MutedKeywordFilter(Collection<String> mutedKeywords) {
        this.mutedKeywords = mutedKeywords == null ? Set.of() : mutedKeywords.stream()
            .map(this::normalize)
            .filter(keyword -> !keyword.isBlank())
            .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public boolean enable(ScoredMoviesQuery query) {
        return !mutedKeywords.isEmpty();
    }

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        if (mutedKeywords.isEmpty()) {
            return new CandidateFilterResult(candidates, List.of());
        }
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate -> !matchesMutedKeyword(candidate)));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }

    private boolean matchesMutedKeyword(MovieCandidate candidate) {
        String text = normalize(candidate.coreDataText());
        return !text.isBlank() && mutedKeywords.stream().anyMatch(text::contains);
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }
}
