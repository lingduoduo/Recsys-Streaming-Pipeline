package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VideoFilter implements CandidateFilter {
    @Override
    public boolean enable(ScoredMoviesQuery query) {
        return query.excludeVideos();
    }

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate -> candidate.quotedVideoDurationMillis() == null));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }
}
