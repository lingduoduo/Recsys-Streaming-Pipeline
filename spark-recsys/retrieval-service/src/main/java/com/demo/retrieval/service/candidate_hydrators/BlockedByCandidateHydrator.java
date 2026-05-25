package com.demo.retrieval.service.candidate_hydrators;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.ScoredMoviesQuery;

import java.util.List;
import java.util.Map;

public class BlockedByCandidateHydrator implements CandidateHydrator {
    private final Map<String, MovieProfile> catalog;

    public BlockedByCandidateHydrator(Map<String, MovieProfile> catalog) {
        this.catalog = catalog;
    }

    @Override
    public List<MovieCandidate> hydrate(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        return candidates.stream()
            .map(candidate -> {
                MovieProfile profile = catalog.get(candidate.movieId());
                return profile == null ? candidate : candidate.withAuthorBlocksViewer(profile.getAuthorBlocksViewer());
            })
            .toList();
    }
}
