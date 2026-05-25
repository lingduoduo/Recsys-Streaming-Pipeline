package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TopicIdsFilter implements CandidateFilter {
    @Override
    public boolean enable(ScoredMoviesQuery query) {
        return query.isTopicRequest() || query.hasExcludedTopics();
    }

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        List<MovieCandidate> kept = candidates;
        List<MovieCandidate> removed = List.of();

        if (query.isTopicRequest()) {
            Set<Integer> expandedTopicIds = TopicIdExpansion.expand(Set.copyOf(query.topicIds()));
            Map<Boolean, List<MovieCandidate>> partitioned = kept.stream()
                .collect(Collectors.partitioningBy(candidate -> matchesRequestedTopics(candidate, expandedTopicIds)));
            kept = partitioned.getOrDefault(Boolean.TRUE, List.of());
            removed = partitioned.getOrDefault(Boolean.FALSE, List.of());
        }

        if (query.hasExcludedTopics()) {
            Set<Integer> excludedTopicIds = TopicIdExpansion.expand(Set.copyOf(query.excludedTopicIds()));
            Map<Boolean, List<MovieCandidate>> partitioned = kept.stream()
                .collect(Collectors.partitioningBy(candidate -> !matchesExcludedTopics(candidate, excludedTopicIds)));
            kept = partitioned.getOrDefault(Boolean.TRUE, List.of());
            removed = java.util.stream.Stream.concat(
                removed.stream(),
                partitioned.getOrDefault(Boolean.FALSE, List.of()).stream()
            ).toList();
        }

        return new CandidateFilterResult(kept, removed);
    }

    private boolean matchesRequestedTopics(MovieCandidate candidate, Set<Integer> expandedTopicIds) {
        if (candidate.filteredTopicIds().isEmpty()) {
            return queryAllowsUntopicedCandidate();
        }
        return candidate.filteredTopicIds().stream().anyMatch(expandedTopicIds::contains)
            || candidate.unfilteredTopicIds().stream().anyMatch(expandedTopicIds::contains);
    }

    private boolean matchesExcludedTopics(MovieCandidate candidate, Set<Integer> excludedTopicIds) {
        return !candidate.filteredTopicIds().isEmpty()
            && candidate.filteredTopicIds().stream().anyMatch(excludedTopicIds::contains);
    }

    private boolean queryAllowsUntopicedCandidate() {
        return false;
    }
}
