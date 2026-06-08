package com.demo.retrieval.service.candidate_hydrators;

public record MovieCandidate(
    String movieId,
    double popularityScore,
    double contentScore,
    boolean coldStartSource
) {
}
