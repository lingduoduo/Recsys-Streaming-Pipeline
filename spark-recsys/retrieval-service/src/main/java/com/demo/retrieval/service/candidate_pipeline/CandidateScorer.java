package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;

@FunctionalInterface
public interface CandidateScorer {
    List<MovieCandidate> score(CandidatePipelineContext context, List<MovieCandidate> candidates);

    default boolean enable(CandidatePipelineContext context) {
        return true;
    }
}
