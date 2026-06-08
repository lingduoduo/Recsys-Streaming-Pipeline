package com.demo.retrieval.service.candidate_hydrators;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.ScoredMoviesQuery;

import java.util.List;
import java.util.Map;

public class GenreMatchCandidateHydrator implements CandidateHydrator {
    private final Map<String, MovieProfile> catalog;

    public GenreMatchCandidateHydrator(Map<String, MovieProfile> catalog) {
        this.catalog = catalog;
    }

    @Override
    public List<MovieCandidate> hydrate(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        return candidates.stream()
            .map(candidate -> {
                MovieProfile profile = catalog.get(candidate.movieId());
                return profile == null
                    ? candidate
                    : candidate.withMatchedGenres(profile.getMatchedGenreIds(), profile.getUnmatchedGenreIds());
            })
            .toList();
    }
}
