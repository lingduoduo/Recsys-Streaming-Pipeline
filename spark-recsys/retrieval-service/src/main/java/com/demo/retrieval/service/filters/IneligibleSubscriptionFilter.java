package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Drops subscription-gated movies from authors the requesting user is not subscribed to.
 * A candidate with a non-blank subscriptionAuthorId is subscription-only content;
 * it is eligible only when the viewing user's subscribedUserIds contains that author.
 * Candidates with no subscriptionAuthorId are always kept (publicly available content).
 */
public class IneligibleSubscriptionFilter implements CandidateFilter {

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
