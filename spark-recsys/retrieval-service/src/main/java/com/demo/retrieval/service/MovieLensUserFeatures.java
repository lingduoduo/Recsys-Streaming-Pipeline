package com.demo.retrieval.service;

import java.util.List;

public record MovieLensUserFeatures(
    String userId,
    List<String> favoriteGenres,
    double avgRating,
    int ratingCount,
    List<String> recentlyRatedMovieIds
) {
    public MovieLensUserFeatures {
        favoriteGenres = favoriteGenres == null ? List.of() : List.copyOf(favoriteGenres);
        recentlyRatedMovieIds = recentlyRatedMovieIds == null ? List.of() : List.copyOf(recentlyRatedMovieIds);
    }

    public static MovieLensUserFeatures forUser(String userId) {
        return new MovieLensUserFeatures(userId, List.of(), 0.0, 0, List.of());
    }
}
