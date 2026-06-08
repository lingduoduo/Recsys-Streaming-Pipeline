package com.demo.retrieval.service;

import com.demo.retrieval.service.clients.*;
import com.demo.retrieval.service.query_hydrators.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserContextQueryHydratorsTest {

    // helper: build a feature object whose recentlyRatedMovieIds is set
    private static MovieLensFeatureClient clientWithRecent(List<String> recent) {
        return userId -> Optional.of(new MovieLensUserFeatures(userId, List.of(), 0.0, recent.size(), recent));
    }

    @Test
    void ratingSequencesHydratorProducesAllThreeSequencesInOneRead() {
        List<String> recent = List.of("m1", "m2", "m3");
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(clientWithRecent(recent));
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1", MovieLensUserFeatures.forUser("u1"),
            List.of("watched"), List.of("rated"), List.of("candidate")
        );

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(recent, updated.userFeatures().actionSequenceMovieIds());
        assertEquals(recent, updated.userFeatures().retrievalSequenceMovieIds());
        assertEquals(recent, updated.userFeatures().scoringSequenceMovieIds());
        // query-level fields untouched
        assertEquals(query.watchedMovieIds(), updated.watchedMovieIds());
        assertEquals(query.ratedMovieIds(), updated.ratedMovieIds());
    }

    @Test
    void ratingSequencesHydratorDeduplicatesPreservingOrder() {
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            clientWithRecent(List.of("a", "b", "a", "c", "b")));
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        List<String> deduped = List.of("a", "b", "c");
        assertEquals(deduped, updated.userFeatures().actionSequenceMovieIds());
        assertEquals(deduped, updated.userFeatures().retrievalSequenceMovieIds());
        assertEquals(deduped, updated.userFeatures().scoringSequenceMovieIds());
    }

    @Test
    void ratingSequencesHydratorTruncatesToWindowLengths() {
        // 150 items — exceeds all three windows
        List<String> longList = IntStream.range(0, 150).mapToObj(i -> "m" + i).toList();
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(clientWithRecent(longList));
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(RatingSequencesQueryHydrator.MAX_ACTION_SEQ_LENGTH,
            updated.userFeatures().actionSequenceMovieIds().size());
        assertEquals(RatingSequencesQueryHydrator.MAX_RETRIEVAL_SEQ_LENGTH,
            updated.userFeatures().retrievalSequenceMovieIds().size());
        assertEquals(RatingSequencesQueryHydrator.MAX_SCORING_SEQ_LENGTH,
            updated.userFeatures().scoringSequenceMovieIds().size());
        assertEquals("m0", updated.userFeatures().retrievalSequenceMovieIds().get(0));
    }

    @Test
    void ratingSequencesHydratorReturnsEmptyForUnknownUser() {
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(userId -> Optional.empty());
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertTrue(updated.userFeatures().actionSequenceMovieIds().isEmpty());
        assertTrue(updated.userFeatures().retrievalSequenceMovieIds().isEmpty());
        assertTrue(updated.userFeatures().scoringSequenceMovieIds().isEmpty());
    }

    @Test
    void demographicsHydratorCopiesOnlyDemographics() {
        UserDemographics demographics = new UserDemographics(42, "M", "artist", "90210");
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withDemographics(demographics);
        UserDemographicsQueryHydrator hydrator = new UserDemographicsQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(demographics, updated.userFeatures().demographics());
        assertEquals(List.of(), updated.userFeatures().actionSequenceMovieIds());
    }

    @Test
    void inferredGenderHydratorCopiesLabelAndScore() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withInferredGender("female", 0.91);
        UserInferredGenderQueryHydrator hydrator = new UserInferredGenderQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals("female", updated.userFeatures().inferredGender());
        assertEquals(0.91, updated.userFeatures().inferredGenderScore());
    }

    @Test
    void inferredGenderHydratorFallsBackToDemographicsForNewUser() {
        // ratingCount == 0 → new user; inferredGender absent → derive from demographics
        UserDemographics demographics = new UserDemographics(25, "F", "student", "10001");
        MovieLensUserFeatures fetched = new MovieLensUserFeatures(
            "u1", List.of(), 0.0, 0, List.of()
        ).withDemographics(demographics);
        UserInferredGenderQueryHydrator hydrator = new UserInferredGenderQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals("F", updated.userFeatures().inferredGender());
        assertEquals(1.0, updated.userFeatures().inferredGenderScore());
    }

    @Test
    void inferredGenderHydratorSkipsFallbackForEstablishedUser() {
        // ratingCount > 0 → established user; inferredGender absent → null, no fallback
        UserDemographics demographics = new UserDemographics(35, "M", "engineer", "94102");
        MovieLensUserFeatures fetched = new MovieLensUserFeatures(
            "u1", List.of(), 4.0, 50, List.of()
        ).withDemographics(demographics);
        UserInferredGenderQueryHydrator hydrator = new UserInferredGenderQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(null, updated.userFeatures().inferredGender());
        assertEquals(null, updated.userFeatures().inferredGenderScore());
    }

    @Test
    void servedHistoryHydratorFetchesFromDedicatedClient() {
        ServedHistoryQueryHydrator hydrator = new ServedHistoryQueryHydrator(
            userId -> List.of("served1", "served2"));
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("served1", "served2"), updated.userFeatures().servedMovieIds());
        assertEquals(List.of(), updated.userFeatures().subscribedUserIds());
    }

    @Test
    void subscribedUserIdsHydratorFetchesFromSocialGraph() {
        SocialGraphClient client = mock(SocialGraphClient.class);
        when(client.getSubscribedUserIds("u1")).thenReturn(List.of("friend1", "friend2"));
        SubscribedUserIdsQueryHydrator hydrator = new SubscribedUserIdsQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("friend1", "friend2"), updated.userFeatures().subscribedUserIds());
        assertEquals(List.of(), updated.userFeatures().servedMovieIds());
    }

    @Test
    void mutedUserIdsHydratorFetchesFromSocialGraph() {
        SocialGraphClient client = mock(SocialGraphClient.class);
        when(client.getMutedUserIds("u1")).thenReturn(List.of("muted1", "muted2"));
        MutedUserIdsQueryHydrator hydrator = new MutedUserIdsQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("muted1", "muted2"), updated.userFeatures().mutedUserIds());
        assertEquals(List.of(), updated.userFeatures().subscribedUserIds());
    }

    @Test
    void pastRequestTimestampsHydratorFetchesFromDedicatedClient() {
        PastRequestTimestampsQueryHydrator hydrator = new PastRequestTimestampsQueryHydrator(
            userId -> List.of(1000L, 2000L));
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(1000L, 2000L), updated.userFeatures().pastRequestTimestamps());
        assertEquals(List.of(), updated.userFeatures().mutualFollowMinhash());
    }

    @Test
    void mutualFollowHydratorFetchesFromMinHashClient() {
        SimilarityMinHashClient client = mock(SimilarityMinHashClient.class);
        when(client.getMinHash("u1")).thenReturn(List.of(7L, 9L));
        MutualFollowQueryHydrator hydrator = new MutualFollowQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(7L, 9L), updated.userFeatures().mutualFollowMinhash());
        assertEquals(List.of(), updated.userFeatures().pastRequestTimestamps());
    }

    @Test
    void inferredGenresHydratorCopiesOnlyTopics() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withInferredGrokTopics(List.of(1, 0, 1));
        InferredGenresQueryHydrator hydrator = new InferredGenresQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(1, 0, 1), updated.userFeatures().inferredGenres());
        assertEquals(List.of(), updated.userFeatures().impressionBloomFilter());
    }

    @Test
    void impressionBloomFilterHydratorFetchesFromDedicatedClient() {
        ImpressionBloomFilterClient client = mock(ImpressionBloomFilterClient.class);
        when(client.getBloomFilterBits("u1")).thenReturn(List.of(44L, 55L));
        ImpressionBloomFilterQueryHydrator hydrator = new ImpressionBloomFilterQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(44L, 55L), updated.userFeatures().impressionBloomFilter());
        assertEquals(List.of(), updated.userFeatures().inferredGenres());
    }

    @Test
    void impressedMoviesHydratorFetchesFromDedicatedClient() {
        ImpressedMoviesClient client = mock(ImpressedMoviesClient.class);
        when(client.getImpressedMovieIds("u1")).thenReturn(List.of("m11", "m12"));
        ImpressedMoviesQueryHydrator hydrator = new ImpressedMoviesQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("m11", "m12"), updated.userFeatures().impressedMovieIds());
        assertEquals("", updated.userFeatures().ipLocation());
    }

    @Test
    void ipHydratorFetchesLocationFromDedicatedClient() {
        IpQueryHydrator hydrator = new IpQueryHydrator(userId -> "10001");
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals("10001", updated.userFeatures().ipLocation());
        assertEquals(List.of(), updated.userFeatures().impressedMovieIds());
    }

    @Test
    void blockedUserIdsHydratorFetchesFromSocialGraph() {
        SocialGraphClient client = mock(SocialGraphClient.class);
        when(client.getBlockedUserIds("u1")).thenReturn(List.of("blocked1", "blocked2"));
        BlockedUserIdsQueryHydrator hydrator = new BlockedUserIdsQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("blocked1", "blocked2"), updated.userFeatures().blockedUserIds());
        assertEquals(List.of(), updated.userFeatures().followedUserIds());
    }

    @Test
    void followedUserIdsHydratorFetchesFromSocialGraph() {
        SocialGraphClient client = mock(SocialGraphClient.class);
        when(client.getFollowedUserIds("u1")).thenReturn(List.of("followed1", "followed2"));
        FollowedUserIdsQueryHydrator hydrator = new FollowedUserIdsQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("followed1", "followed2"), updated.userFeatures().followedUserIds());
        assertEquals(List.of(), updated.userFeatures().blockedUserIds());
    }

    @Test
    void cachedMoviesHydratorSetsHasCachedMoviesWhenAboveThreshold() {
        List<String> largeCache = IntStream.range(0, CachedMoviesQueryHydrator.MIN_CACHED_MOVIES_THRESHOLD)
            .mapToObj(i -> "m" + i).toList();
        CachedMoviesClient client = mock(CachedMoviesClient.class);
        when(client.getCachedMovieIds("u1")).thenReturn(largeCache);
        CachedMoviesQueryHydrator hydrator = new CachedMoviesQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(largeCache, updated.userFeatures().cachedMovieIds());
        assertEquals(true, updated.userFeatures().hasCachedMovies());
    }

    @Test
    void cachedMoviesHydratorClearsFlagWhenBelowThreshold() {
        CachedMoviesClient client = mock(CachedMoviesClient.class);
        when(client.getCachedMovieIds("u1")).thenReturn(List.of("cached1", "cached2"));
        CachedMoviesQueryHydrator hydrator = new CachedMoviesQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("cached1", "cached2"), updated.userFeatures().cachedMovieIds());
        assertEquals(false, updated.userFeatures().hasCachedMovies());
    }

    @Test
    void relatedContentsHydratorCopiesContentCategoryIds() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withFollowedGrokTopics(List.of(3, 7, 12));
        FollowedGenresQueryHydrator hydrator = new FollowedGenresQueryHydrator(
            userId -> Optional.of(fetched));
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(3, 7, 12), updated.userFeatures().followedGenres());
        assertEquals(List.of(), updated.userFeatures().followedCollections());
    }

    @Test
    void followedCollectionsHydratorFetchesFromDedicatedClient() {
        FollowedCollectionsClient client = mock(FollowedCollectionsClient.class);
        when(client.getFollowedPackIds("u1")).thenReturn(List.of(1, 0, 1));
        FollowedCollectionsQueryHydrator hydrator = new FollowedCollectionsQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(1, 0, 1), updated.userFeatures().followedCollections());
        assertEquals(List.of(), updated.userFeatures().followedGenres());
    }
}
