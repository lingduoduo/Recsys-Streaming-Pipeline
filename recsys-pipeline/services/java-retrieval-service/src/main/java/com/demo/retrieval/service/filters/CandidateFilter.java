package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;

@FunctionalInterface
public interface CandidateFilter {
    CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates);

    default boolean enable(ScoredMoviesQuery query) {
        return true;
    }
}
