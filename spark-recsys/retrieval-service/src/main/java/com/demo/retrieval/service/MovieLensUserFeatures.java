package com.demo.retrieval.service;

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
    List<String> subscribedUserIds,
    List<String> mutedUserIds,
    List<String> blockedUserIds,
    List<String> followedUserIds,
    List<Long> pastRequestTimestamps,
    List<Long> mutualFollowMinhash,
    List<Integer> inferredGrokTopics,
    List<Integer> followedGrokTopics,
    List<Integer> followedStarterPacks,
    List<Long> impressionBloomFilter,
    List<String> impressedMovieIds,
    List<String> cachedMovieIds,
    boolean hasCachedMovies,
    String ipLocation,
    UserDemographics demographics,
    String inferredGender,
    Double inferredGenderScore
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
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            false,
            "",
            UserDemographics.empty(),
            null,
            null
        );
    }

    public MovieLensUserFeatures {
        favoriteGenres = copyStrings(favoriteGenres);
        recentlyRatedMovieIds = copyStrings(recentlyRatedMovieIds);
        actionSequenceMovieIds = copyStrings(actionSequenceMovieIds);
        retrievalSequenceMovieIds = copyStrings(retrievalSequenceMovieIds);
        scoringSequenceMovieIds = copyStrings(scoringSequenceMovieIds);
        servedMovieIds = copyStrings(servedMovieIds);
        subscribedUserIds = copyStrings(subscribedUserIds);
        mutedUserIds = copyStrings(mutedUserIds);
        blockedUserIds = copyStrings(blockedUserIds);
        followedUserIds = copyStrings(followedUserIds);
        pastRequestTimestamps = copyLongs(pastRequestTimestamps);
        mutualFollowMinhash = copyLongs(mutualFollowMinhash);
        inferredGrokTopics = copyIntegers(inferredGrokTopics);
        followedGrokTopics = copyIntegers(followedGrokTopics);
        followedStarterPacks = copyIntegers(followedStarterPacks);
        impressionBloomFilter = copyLongs(impressionBloomFilter);
        impressedMovieIds = copyStrings(impressedMovieIds);
        cachedMovieIds = copyStrings(cachedMovieIds);
        ipLocation = ipLocation == null ? "" : ipLocation.trim();
        demographics = demographics == null ? UserDemographics.empty() : demographics;
    }

    public static MovieLensUserFeatures forUser(String userId) {
        return new MovieLensUserFeatures(userId, List.of(), 0.0, 0, List.of());
    }

    public MovieLensUserFeatures withActionSequenceMovieIds(List<String> value) {
        return copy(value, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds, subscribedUserIds,
            mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps, mutualFollowMinhash,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withRetrievalSequenceMovieIds(List<String> value) {
        return copy(actionSequenceMovieIds, value, scoringSequenceMovieIds, servedMovieIds, subscribedUserIds,
            mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps, mutualFollowMinhash,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withScoringSequenceMovieIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, value, servedMovieIds, subscribedUserIds,
            mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps, mutualFollowMinhash,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withServedMovieIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, value, subscribedUserIds,
            mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps, mutualFollowMinhash,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withSubscribedUserIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds, value,
            mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps, mutualFollowMinhash,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withMutedUserIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, value, blockedUserIds, followedUserIds, pastRequestTimestamps, mutualFollowMinhash,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withBlockedUserIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, value, followedUserIds, pastRequestTimestamps, mutualFollowMinhash,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withFollowedUserIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, value, pastRequestTimestamps, mutualFollowMinhash,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withPastRequestTimestamps(List<Long> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, value, mutualFollowMinhash,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withMutualFollowMinhash(List<Long> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps, value,
            inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter, impressedMovieIds,
            cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withInferredGrokTopics(List<Integer> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps,
            mutualFollowMinhash, value, followedGrokTopics, followedStarterPacks, impressionBloomFilter,
            impressedMovieIds, cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender,
            inferredGenderScore);
    }

    public MovieLensUserFeatures withFollowedGrokTopics(List<Integer> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps,
            mutualFollowMinhash, inferredGrokTopics, value, followedStarterPacks, impressionBloomFilter,
            impressedMovieIds, cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender,
            inferredGenderScore);
    }

    public MovieLensUserFeatures withFollowedStarterPacks(List<Integer> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps,
            mutualFollowMinhash, inferredGrokTopics, followedGrokTopics, value, impressionBloomFilter,
            impressedMovieIds, cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender,
            inferredGenderScore);
    }

    public MovieLensUserFeatures withImpressionBloomFilter(List<Long> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps,
            mutualFollowMinhash, inferredGrokTopics, followedGrokTopics, followedStarterPacks, value,
            impressedMovieIds, cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender,
            inferredGenderScore);
    }

    public MovieLensUserFeatures withImpressedMovieIds(List<String> value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps,
            mutualFollowMinhash, inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter,
            value, cachedMovieIds, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withCachedMovieIds(List<String> value, boolean hasCachedMovies) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps,
            mutualFollowMinhash, inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter,
            impressedMovieIds, value, hasCachedMovies, ipLocation, demographics, inferredGender, inferredGenderScore);
    }

    public MovieLensUserFeatures withIpLocation(String value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps,
            mutualFollowMinhash, inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter,
            impressedMovieIds, cachedMovieIds, hasCachedMovies, value, demographics, inferredGender,
            inferredGenderScore);
    }

    public MovieLensUserFeatures withDemographics(UserDemographics value) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps,
            mutualFollowMinhash, inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter,
            impressedMovieIds, cachedMovieIds, hasCachedMovies, ipLocation, value, inferredGender,
            inferredGenderScore);
    }

    public MovieLensUserFeatures withInferredGender(String value, Double score) {
        return copy(actionSequenceMovieIds, retrievalSequenceMovieIds, scoringSequenceMovieIds, servedMovieIds,
            subscribedUserIds, mutedUserIds, blockedUserIds, followedUserIds, pastRequestTimestamps,
            mutualFollowMinhash, inferredGrokTopics, followedGrokTopics, followedStarterPacks, impressionBloomFilter,
            impressedMovieIds, cachedMovieIds, hasCachedMovies, ipLocation, demographics, value, score);
    }

    private MovieLensUserFeatures copy(
        List<String> actionSequenceMovieIds,
        List<String> retrievalSequenceMovieIds,
        List<String> scoringSequenceMovieIds,
        List<String> servedMovieIds,
        List<String> subscribedUserIds,
        List<String> mutedUserIds,
        List<String> blockedUserIds,
        List<String> followedUserIds,
        List<Long> pastRequestTimestamps,
        List<Long> mutualFollowMinhash,
        List<Integer> inferredGrokTopics,
        List<Integer> followedGrokTopics,
        List<Integer> followedStarterPacks,
        List<Long> impressionBloomFilter,
        List<String> impressedMovieIds,
        List<String> cachedMovieIds,
        boolean hasCachedMovies,
        String ipLocation,
        UserDemographics demographics,
        String inferredGender,
        Double inferredGenderScore
    ) {
        return new MovieLensUserFeatures(
            userId,
            favoriteGenres,
            avgRating,
            ratingCount,
            recentlyRatedMovieIds,
            actionSequenceMovieIds,
            retrievalSequenceMovieIds,
            scoringSequenceMovieIds,
            servedMovieIds,
            subscribedUserIds,
            mutedUserIds,
            blockedUserIds,
            followedUserIds,
            pastRequestTimestamps,
            mutualFollowMinhash,
            inferredGrokTopics,
            followedGrokTopics,
            followedStarterPacks,
            impressionBloomFilter,
            impressedMovieIds,
            cachedMovieIds,
            hasCachedMovies,
            ipLocation,
            demographics,
            inferredGender,
            inferredGenderScore
        );
    }

    private static List<String> copyStrings(List<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }

    private static List<Long> copyLongs(List<Long> values) {
        return values == null ? List.of() : List.copyOf(values);
    }

    private static List<Integer> copyIntegers(List<Integer> values) {
        return values == null ? List.of() : List.copyOf(values);
    }
}
