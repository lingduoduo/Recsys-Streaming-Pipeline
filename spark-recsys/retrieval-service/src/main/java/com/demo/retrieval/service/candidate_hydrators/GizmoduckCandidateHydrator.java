package com.demo.retrieval.service.candidate_hydrators;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.ScoredMoviesQuery;

import java.util.List;
import java.util.Map;

public class GizmoduckCandidateHydrator implements CandidateHydrator {
    private final Map<String, MovieProfile> catalog;

    public GizmoduckCandidateHydrator(Map<String, MovieProfile> catalog) {
        this.catalog = catalog;
    }

    @Override
    public List<MovieCandidate> hydrate(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        return candidates.stream()
            .map(candidate -> {
                MovieProfile profile = catalog.get(candidate.movieId());
                if (profile == null) {
                    return candidate;
                }
                return candidate.withGizmoduck(
                    profile.getAuthorFollowersCount(),
                    profile.getAuthorScreenName(),
                    profile.getRetweetedScreenName()
                );
            })
            .toList();
    }
}
