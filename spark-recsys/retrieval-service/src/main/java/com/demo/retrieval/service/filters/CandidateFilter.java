package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface CandidateFilter {
    CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates);

    default boolean enable(ScoredMoviesQuery query) {
        return true;
    }

    class VFFilter implements CandidateFilter {
        @Override
        public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
            Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
                .collect(Collectors.partitioningBy(c -> c.visibilityReason() == null || c.visibilityReason().isBlank()));
            return new CandidateFilterResult(
                partitioned.getOrDefault(Boolean.TRUE, List.of()),
                partitioned.getOrDefault(Boolean.FALSE, List.of())
            );
        }
    }

    class AncillaryVFFilter implements CandidateFilter {
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
