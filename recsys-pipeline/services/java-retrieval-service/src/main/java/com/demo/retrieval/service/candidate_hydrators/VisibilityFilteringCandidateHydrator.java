package com.demo.retrieval.service.candidate_hydrators;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.ScoredMoviesQuery;

import java.util.List;
import java.util.Map;

public class VisibilityFilteringCandidateHydrator implements CandidateHydrator {
    private final Map<String, MovieProfile> catalog;

    public VisibilityFilteringCandidateHydrator(Map<String, MovieProfile> catalog) {
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
                return candidate.withVisibility(
                    profile.getVisibilityReason(),
                    profile.isDropAncillaryMovies() || shouldDropAncillary(profile),
                    profile.getAncestorMovieIds(),
                    profile.getQuotedMovieId()
                );
            })
            .toList();
    }

    private boolean shouldDropAncillary(MovieProfile profile) {
        for (String ancestorId : profile.getAncestorMovieIds()) {
            if (hasVisibilityDrop(ancestorId)) {
                return true;
            }
        }
        return hasVisibilityDrop(profile.getQuotedMovieId()) || hasVisibilityDrop(profile.getSourceMovieId());
    }

    private boolean hasVisibilityDrop(String movieId) {
        if (movieId == null || movieId.isBlank()) {
            return false;
        }
        MovieProfile profile = catalog.get(movieId);
        return profile != null && profile.getVisibilityReason() != null && !profile.getVisibilityReason().isBlank();
    }
}
