package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AuthorSocialgraphFilter implements CandidateFilter {
    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Set<String> mutedUserIds = new HashSet<>(query.userFeatures().mutedUserIds());
        Set<String> blockedUserIds = new HashSet<>(query.userFeatures().blockedUserIds());

        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate -> isAllowed(candidate, mutedUserIds, blockedUserIds)));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }

    private boolean isAllowed(MovieCandidate candidate, Set<String> mutedUserIds, Set<String> blockedUserIds) {
        return !contains(mutedUserIds, candidate.ownerId())
            && !contains(blockedUserIds, candidate.ownerId())
            && !contains(blockedUserIds, candidate.quotedOwnerId())
            && !Boolean.TRUE.equals(candidate.authorBlocksViewer())
            && !Boolean.TRUE.equals(candidate.quotedAuthorBlocksViewer());
    }

    private boolean contains(Set<String> userIds, String userId) {
        return userId != null && userIds.contains(userId);
    }

    public static class IneligibleSubscriptionFilter implements CandidateFilter {
        @Override
        public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
            Set<String> subscribedUserIds = new HashSet<>(query.userFeatures().subscribedUserIds());
            Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
                .collect(Collectors.partitioningBy(candidate -> {
                    String authorId = candidate.subscriptionAuthorId();
                    return authorId == null || authorId.isBlank() || subscribedUserIds.contains(authorId);
                }));
            return new CandidateFilterResult(
                partitioned.getOrDefault(Boolean.TRUE, List.of()),
                partitioned.getOrDefault(Boolean.FALSE, List.of())
            );
        }
    }
}
