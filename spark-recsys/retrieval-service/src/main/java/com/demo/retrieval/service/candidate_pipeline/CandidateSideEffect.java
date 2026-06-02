package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;

@FunctionalInterface
public interface CandidateSideEffect {
    void run(CandidatePipelineContext context, List<MovieCandidate> selected, List<MovieCandidate> nonSelected);

    default boolean enable(CandidatePipelineContext context) {
        return true;
    }
}
