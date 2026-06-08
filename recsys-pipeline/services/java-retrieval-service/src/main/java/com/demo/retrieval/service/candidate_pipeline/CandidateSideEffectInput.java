package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;

public record CandidateSideEffectInput(
    List<MovieCandidate> selected,
    List<MovieCandidate> nonSelected,
    CandidatePipelineResult result
) {
    public CandidateSideEffectInput {
        selected = selected == null ? List.of() : List.copyOf(selected);
        nonSelected = nonSelected == null ? List.of() : List.copyOf(nonSelected);
    }
}
