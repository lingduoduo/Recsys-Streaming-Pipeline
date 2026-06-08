package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;

@FunctionalInterface
public interface CandidateSource {
    List<MovieCandidate> fetch(CandidatePipelineContext context);

    default boolean enable(CandidatePipelineContext context) {
        return true;
    }
}
