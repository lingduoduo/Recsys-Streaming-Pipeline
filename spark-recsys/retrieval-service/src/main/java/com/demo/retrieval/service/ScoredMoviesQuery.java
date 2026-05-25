package com.demo.retrieval.service;

import java.util.List;

public record ScoredMoviesQuery(
    String userId,
    MovieLensUserFeatures userFeatures,
    List<String> watchedMovieIds,
    List<String> ratedMovieIds,
    List<String> candidateMovieIds,
    List<Integer> topicIds,
    List<Integer> excludedTopicIds,
    boolean bulkTopicRequest,
    boolean excludeVideos
) {
    public ScoredMoviesQuery(
        String userId,
        MovieLensUserFeatures userFeatures,
        List<String> watchedMovieIds,
        List<String> ratedMovieIds,
        List<String> candidateMovieIds
    ) {
        this(userId, userFeatures, watchedMovieIds, ratedMovieIds, candidateMovieIds, List.of(), List.of(), false, false);
    }

    public ScoredMoviesQuery {
        userFeatures = userFeatures == null ? MovieLensUserFeatures.forUser(userId) : userFeatures;
        watchedMovieIds = watchedMovieIds == null ? List.of() : List.copyOf(watchedMovieIds);
        ratedMovieIds = ratedMovieIds == null ? List.of() : List.copyOf(ratedMovieIds);
        candidateMovieIds = candidateMovieIds == null ? List.of() : List.copyOf(candidateMovieIds);
        topicIds = topicIds == null ? List.of() : List.copyOf(topicIds);
        excludedTopicIds = excludedTopicIds == null ? List.of() : List.copyOf(excludedTopicIds);
    }

    public static ScoredMoviesQuery forUser(String userId) {
        return new ScoredMoviesQuery(userId, MovieLensUserFeatures.forUser(userId), List.of(), List.of(), List.of());
    }

    public boolean isTopicRequest() {
        return !topicIds.isEmpty();
    }

    public boolean hasExcludedTopics() {
        return !excludedTopicIds.isEmpty();
    }
}
