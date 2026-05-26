package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Drops candidates flagged as ancillary by the visibility filter.
 * Ancillary movies (e.g. reply-chain context, quoted content surfaced for completion)
 * are intentionally excluded from recommendation slates to avoid flooding the user
 * with peripheral content around a single interaction thread.
 */
public class AncillaryVFFilter implements CandidateFilter {

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(c -> !c.dropAncillaryMovies()));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }
}
