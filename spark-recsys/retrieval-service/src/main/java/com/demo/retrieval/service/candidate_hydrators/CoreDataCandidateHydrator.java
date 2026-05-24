package com.demo.retrieval.service.candidate_hydrators;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.ScoredMoviesQuery;

import java.util.List;
import java.util.Map;

public class CoreDataCandidateHydrator implements CandidateHydrator {
    private final Map<String, MovieProfile> catalog;

    public CoreDataCandidateHydrator(Map<String, MovieProfile> catalog) {
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
                return candidate.withCoreData(
                    profile.getOwnerId(),
                    profile.getSourceUserId(),
                    profile.getSourceMovieId(),
                    profile.getInReplyToMovieId(),
                    profile.getCoreDataText()
                );
            })
            .toList();
    }
}
