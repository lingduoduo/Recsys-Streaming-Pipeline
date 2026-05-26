package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Drops candidates that were not successfully hydrated with core data.
 * A missing ownerId indicates the movie record could not be resolved from the catalog,
 * making the candidate ineligible for scoring and display.
 */
public class CoreDataHydrationFilter implements CandidateFilter {

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(
                c -> c.ownerId() != null && !c.ownerId().isBlank()
            ));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }
}
