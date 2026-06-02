package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import com.demo.retrieval.service.filters.CandidateFilterResult;

import java.util.List;

@FunctionalInterface
public interface PipelineCandidateFilter {
    CandidateFilterResult filter(CandidatePipelineContext context, List<MovieCandidate> candidates);

    default boolean enable(CandidatePipelineContext context) {
        return true;
    }
}
