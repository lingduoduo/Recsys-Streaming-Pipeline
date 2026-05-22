package com.demo.retrieval.service;

import java.util.List;

public record ScoredMoviesQuery(
    String userId,
    List<String> watchedMovieIds,
    List<String> ratedMovieIds
) {
    public ScoredMoviesQuery {
        watchedMovieIds = watchedMovieIds == null ? List.of() : List.copyOf(watchedMovieIds);
        ratedMovieIds = ratedMovieIds == null ? List.of() : List.copyOf(ratedMovieIds);
    }

    public static ScoredMoviesQuery forUser(String userId) {
        return new ScoredMoviesQuery(userId, List.of(), List.of());
    }
}
