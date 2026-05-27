package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GenreIdsFilter implements CandidateFilter {
    @Override
    public boolean enable(ScoredMoviesQuery query) {
        return query.isGenreRequest() || query.hasExcludedGenres();
    }

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        List<MovieCandidate> kept = candidates;
        List<MovieCandidate> removed = List.of();

        if (query.isGenreRequest()) {
            Set<Integer> expandedTopicIds = GenreExpansion.expand(Set.copyOf(query.genreIds()));
            Map<Boolean, List<MovieCandidate>> partitioned = kept.stream()
                .collect(Collectors.partitioningBy(candidate -> matchesRequestedTopics(candidate, expandedTopicIds)));
            kept = partitioned.getOrDefault(Boolean.TRUE, List.of());
            removed = partitioned.getOrDefault(Boolean.FALSE, List.of());
        }

        if (query.hasExcludedGenres()) {
            Set<Integer> excludedGenreIds = GenreExpansion.expand(Set.copyOf(query.excludedGenreIds()));
            Map<Boolean, List<MovieCandidate>> partitioned = kept.stream()
                .collect(Collectors.partitioningBy(candidate -> !matchesExcludedTopics(candidate, excludedGenreIds)));
            kept = partitioned.getOrDefault(Boolean.TRUE, List.of());
            removed = java.util.stream.Stream.concat(
                removed.stream(),
                partitioned.getOrDefault(Boolean.FALSE, List.of()).stream()
            ).toList();
        }

        return new CandidateFilterResult(kept, removed);
    }

    private boolean matchesRequestedTopics(MovieCandidate candidate, Set<Integer> expandedTopicIds) {
        if (candidate.matchedGenreIds().isEmpty()) {
            return queryAllowsUntopicedCandidate();
        }
        return candidate.matchedGenreIds().stream().anyMatch(expandedTopicIds::contains)
            || candidate.unmatchedGenreIds().stream().anyMatch(expandedTopicIds::contains);
    }

    private boolean matchesExcludedTopics(MovieCandidate candidate, Set<Integer> excludedGenreIds) {
        return !candidate.matchedGenreIds().isEmpty()
            && candidate.matchedGenreIds().stream().anyMatch(excludedGenreIds::contains);
    }

    private boolean queryAllowsUntopicedCandidate() {
        return false;
    }
}
