package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.List;

public record CandidatePipelineResult(
    List<MovieCandidate> retrievedCandidates,
    List<MovieCandidate> filteredCandidates,
    List<MovieCandidate> scoredCandidates,
    List<MovieCandidate> selectedCandidates,
    List<MovieCandidate> nonSelectedCandidates
) {
    public CandidatePipelineResult {
        retrievedCandidates = retrievedCandidates == null ? List.of() : List.copyOf(retrievedCandidates);
        filteredCandidates = filteredCandidates == null ? List.of() : List.copyOf(filteredCandidates);
        scoredCandidates = scoredCandidates == null ? List.of() : List.copyOf(scoredCandidates);
        selectedCandidates = selectedCandidates == null ? List.of() : List.copyOf(selectedCandidates);
        nonSelectedCandidates = nonSelectedCandidates == null ? List.of() : List.copyOf(nonSelectedCandidates);
    }
}
