package com.demo.retrieval.model;

import java.util.List;

public record MovieLensUserFeatures(
    String userId,
    List<String> favoriteGenres,
    double avgRating,
    int ratingCount,
    List<String> recentlyRatedMovieIds,
    List<String> actionSequenceMovieIds,
    List<String> retrievalSequenceMovieIds,
    List<String> scoringSequenceMovieIds,
    List<String> servedMovieIds,
    List<Long> pastRequestTimestamps,
    List<Integer> inferredGenres,
    List<Long> impressionBloomFilter,
    List<String> impressedMovieIds,
    List<String> cachedMovieIds,
    boolean hasCachedMovies,
    UserDemographics demographics
) {
    public MovieLensUserFeatures(
        String userId,
        List<String> favoriteGenres,
        double avgRating,
        int ratingCount,
        List<String> recentlyRatedMovieIds
    ) {
        this(
            userId,
            favoriteGenres,
            avgRating,
            ratingCount,
            recentlyRatedMovieIds,
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            false,
            UserDemographics.empty()
        );
    }

    public MovieLensUserFeatures {
        favoriteGenres = strings(favoriteGenres);
        recentlyRatedMovieIds = strings(recentlyRatedMovieIds);
        actionSequenceMovieIds = strings(actionSequenceMovieIds);
        retrievalSequenceMovieIds = strings(retrievalSequenceMovieIds);
        scoringSequenceMovieIds = strings(scoringSequenceMovieIds);
        servedMovieIds = strings(servedMovieIds);
        pastRequestTimestamps = pastRequestTimestamps == null ? List.of() : List.copyOf(pastRequestTimestamps);
        inferredGenres = inferredGenres == null ? List.of() : List.copyOf(inferredGenres);
        impressionBloomFilter = impressionBloomFilter == null ? List.of() : List.copyOf(impressionBloomFilter);
        impressedMovieIds = strings(impressedMovieIds);
        cachedMovieIds = strings(cachedMovieIds);
        demographics = demographics == null ? UserDemographics.empty() : demographics;
    }

    public static MovieLensUserFeatures forUser(String userId) {
        return new MovieLensUserFeatures(userId, List.of(), 0.0, 0, List.of());
    }

    public MovieLensUserFeatures withActionSequenceMovieIds(List<String> value) {
        return copy(value, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds, pastRequestTimestamps,
            inferredGenres, impressionBloomFilter, impressedMovieIds, cachedMovieIds, hasCachedMovies, demographics);
    }

    public MovieLensUserFeatures withRetrievalSequenceMovieIds(List<String> value) {
        return copy(actionSequenceMovieIds, value, scoringSequenceMovieIds, servedMovieIds, pastRequestTimestamps,
            inferredGenres, impressionBloomFilter, impressedMovieIds, cachedMovieIds, hasCachedMovies, demographics);
    }

    public MovieLensUserFeatures withScoringSequenceMovieIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, value, servedMovieIds, pastRequestTimestamps,
            inferredGenres, impressionBloomFilter, impressedMovieIds, cachedMovieIds, hasCachedMovies, demographics);
    }

    public MovieLensUserFeatures withServedMovieIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, value,
            pastRequestTimestamps, inferredGenres, impressionBloomFilter, impressedMovieIds, cachedMovieIds,
            hasCachedMovies, demographics);
    }

    public MovieLensUserFeatures withPastRequestTimestamps(List<Long> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds, value,
            inferredGenres, impressionBloomFilter, impressedMovieIds, cachedMovieIds, hasCachedMovies, demographics);
    }

    public MovieLensUserFeatures withInferredGenres(List<Integer> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            pastRequestTimestamps, value, impressionBloomFilter, impressedMovieIds, cachedMovieIds, hasCachedMovies,
            demographics);
    }

    public MovieLensUserFeatures withInferredGrokTopics(List<Integer> value) {
        return withInferredGenres(value);
    }

    public MovieLensUserFeatures withImpressionBloomFilter(List<Long> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            pastRequestTimestamps, inferredGenres, value, impressedMovieIds, cachedMovieIds, hasCachedMovies,
            demographics);
    }

    public MovieLensUserFeatures withImpressedMovieIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            pastRequestTimestamps, inferredGenres, impressionBloomFilter, value, cachedMovieIds, hasCachedMovies,
            demographics);
    }

    public MovieLensUserFeatures withCachedMovieIds(List<String> value, boolean hasCached) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            pastRequestTimestamps, inferredGenres, impressionBloomFilter, impressedMovieIds, value, hasCached,
            demographics);
    }

    public MovieLensUserFeatures withDemographics(UserDemographics value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            pastRequestTimestamps, inferredGenres, impressionBloomFilter, impressedMovieIds, cachedMovieIds,
            hasCachedMovies, value);
    }

    private MovieLensUserFeatures copy(
        List<String> actions,
        List<String> retrieval,
        List<String> scoring,
        List<String> served,
        List<Long> requests,
        List<Integer> genres,
        List<Long> bloom,
        List<String> impressed,
        List<String> cached,
        boolean hasCached,
        UserDemographics userDemographics
    ) {
        return new MovieLensUserFeatures(
            userId, favoriteGenres, avgRating, ratingCount, recentlyRatedMovieIds, actions, retrieval, scoring, served,
            requests, genres, bloom, impressed, cached, hasCached, userDemographics
        );
    }

    private static List<String> strings(List<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }
}
