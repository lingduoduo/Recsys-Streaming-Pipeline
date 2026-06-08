package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;

@FunctionalInterface
public interface CandidateSelector {
    CandidateSelection select(CandidatePipelineContext context, List<MovieCandidate> candidates);

    default boolean enable(CandidatePipelineContext context) {
        return true;
    }

    default String name() {
        return getClass().getSimpleName();
    }
}
