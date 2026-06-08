package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CreatorBlocklistFilter implements CandidateFilter {
    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Set<String> mutedIds = new HashSet<>(query.userFeatures().mutedUserIds());
        Set<String> blockedIds = new HashSet<>(query.userFeatures().blockedUserIds());

        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(c ->
                !contains(mutedIds, c.ownerId())
                && !contains(blockedIds, c.ownerId())
                && !contains(blockedIds, c.quotedOwnerId())
            ));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }

    private boolean contains(Set<String> ids, String id) {
        return id != null && ids.contains(id);
    }

    public static class IneligibleSubscriptionFilter implements CandidateFilter {
        @Override
        public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
            Set<String> subscribedIds = new HashSet<>(query.userFeatures().subscribedUserIds());
            Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
                .collect(Collectors.partitioningBy(c -> {
                    String creatorId = c.subscriptionAuthorId();
                    return creatorId == null || creatorId.isBlank() || subscribedIds.contains(creatorId);
                }));
            return new CandidateFilterResult(
                partitioned.getOrDefault(Boolean.TRUE, List.of()),
                partitioned.getOrDefault(Boolean.FALSE, List.of())
            );
        }
    }
}
