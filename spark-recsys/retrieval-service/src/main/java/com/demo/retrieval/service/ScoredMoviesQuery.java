package com.demo.retrieval.service;

import java.util.List;

public record ScoredMoviesQuery(
    String userId,
    MovieLensUserFeatures userFeatures,
    List<String> watchedMovieIds,
    List<String> ratedMovieIds,
    List<String> candidateMovieIds
) {
    public ScoredMoviesQuery {
        userFeatures = userFeatures == null ? MovieLensUserFeatures.forUser(userId) : userFeatures;
        watchedMovieIds = watchedMovieIds == null ? List.of() : List.copyOf(watchedMovieIds);
        ratedMovieIds = ratedMovieIds == null ? List.of() : List.copyOf(ratedMovieIds);
        candidateMovieIds = candidateMovieIds == null ? List.of() : List.copyOf(candidateMovieIds);
    }

    public static ScoredMoviesQuery forUser(String userId) {
        return new ScoredMoviesQuery(userId, MovieLensUserFeatures.forUser(userId), List.of(), List.of(), List.of());
    }
}
