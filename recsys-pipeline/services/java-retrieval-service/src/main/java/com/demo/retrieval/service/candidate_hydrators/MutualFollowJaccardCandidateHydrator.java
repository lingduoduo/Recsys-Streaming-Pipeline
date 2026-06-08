package com.demo.retrieval.service.candidate_hydrators;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.ScoredMoviesQuery;

import java.util.List;
import java.util.Map;

public class MutualFollowJaccardCandidateHydrator implements CandidateHydrator {
    private final Map<String, MovieProfile> catalog;

    public MutualFollowJaccardCandidateHydrator(Map<String, MovieProfile> catalog) {
        this.catalog = catalog;
    }

    @Override
    public List<MovieCandidate> hydrate(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        return candidates.stream()
            .map(candidate -> {
                MovieProfile profile = catalog.get(candidate.movieId());
                return profile == null ? candidate : candidate.withMutualFollowJaccard(profile.getMutualFollowJaccard());
            })
            .toList();
    }
}
