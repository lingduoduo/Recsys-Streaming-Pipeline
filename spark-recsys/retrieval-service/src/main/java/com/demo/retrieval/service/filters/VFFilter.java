package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VFFilter implements CandidateFilter {
    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate -> !shouldDrop(candidate.visibilityReason())));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }

    private boolean shouldDrop(String visibilityReason) {
        return visibilityReason != null && !visibilityReason.isBlank();
    }

    public static class AncillaryVFFilter implements CandidateFilter {
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
}
