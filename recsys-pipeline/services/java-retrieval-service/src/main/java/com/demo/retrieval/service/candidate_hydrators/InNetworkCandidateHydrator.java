package com.demo.retrieval.service.candidate_hydrators;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.ScoredMoviesQuery;

import java.util.List;
import java.util.Map;

public class InNetworkCandidateHydrator implements CandidateHydrator {
    private final Map<String, MovieProfile> catalog;

    public InNetworkCandidateHydrator(Map<String, MovieProfile> catalog) {
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
                Boolean configured = profile.getInNetwork();
                if (configured != null) {
                    return candidate.withInNetwork(configured);
                }
                String ownerId = profile.getOwnerId();
                boolean followed = ownerId != null && query.userFeatures().followedUserIds().contains(ownerId);
                boolean self = ownerId != null && ownerId.equals(query.userId());
                return candidate.withInNetwork(followed || self);
            })
            .toList();
    }
}
