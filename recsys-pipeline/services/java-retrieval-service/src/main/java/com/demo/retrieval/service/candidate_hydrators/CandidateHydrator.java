package com.demo.retrieval.service.candidate_hydrators;

import com.demo.retrieval.service.ScoredMoviesQuery;

import java.util.List;

public interface CandidateHydrator {
    List<MovieCandidate> hydrate(ScoredMoviesQuery query, List<MovieCandidate> candidates);

    default boolean enable(ScoredMoviesQuery query) {
        return true;
    }
}
