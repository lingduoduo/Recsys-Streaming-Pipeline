package com.demo.retrieval.service;

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
    void actionSequenceHydratorDerivesFromRecentRatings() {
        List<String> recent = List.of("m1", "m2", "m3");
        UserActionSequenceQueryHydrator hydrator = new UserActionSequenceQueryHydrator(clientWithRecent(recent));
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1", MovieLensUserFeatures.forUser("u1"),
            List.of("watched"), List.of("rated"), List.of("candidate")
        );

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(recent, updated.userFeatures().actionSequenceMovieIds());
        // other fields untouched
        assertEquals(query.watchedMovieIds(), updated.watchedMovieIds());
        assertEquals(query.ratedMovieIds(), updated.ratedMovieIds());
        assertEquals(List.of(), updated.userFeatures().retrievalSequenceMovieIds());
        assertEquals(List.of(), updated.userFeatures().scoringSequenceMovieIds());
    }

    @Test
    void actionSequenceHydratorDeduplicatesPreservingOrder() {
        UserActionSequenceQueryHydrator hydrator = new UserActionSequenceQueryHydrator(
            clientWithRecent(List.of("a", "b", "a", "c", "b")));
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(List.of("a", "b", "c"), updated.userFeatures().actionSequenceMovieIds());
    }

    @Test
    void actionSequenceHydratorTruncatesToMax() {
        List<String> longList = IntStream.range(0, 80).mapToObj(i -> "m" + i).toList();
        UserActionSequenceQueryHydrator hydrator = new UserActionSequenceQueryHydrator(clientWithRecent(longList));
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(UserActionSequenceQueryHydrator.MAX_ACTION_SEQ_LENGTH,
            updated.userFeatures().actionSequenceMovieIds().size());
    }

    @Test
    void actionSequenceHydratorReturnsEmptyForUnknownUser() {
        UserActionSequenceQueryHydrator hydrator = new UserActionSequenceQueryHydrator(
            userId -> Optional.empty());
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertTrue(updated.userFeatures().actionSequenceMovieIds().isEmpty());
    }

    @Test
    void retrievalSequenceHydratorDerivesFromRecentRatings() {
        List<String> recent = List.of("r1", "r2", "r3");
        RetrievalSequenceQueryHydrator hydrator = new RetrievalSequenceQueryHydrator(clientWithRecent(recent));
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(recent, updated.userFeatures().retrievalSequenceMovieIds());
        assertEquals(List.of(), updated.userFeatures().scoringSequenceMovieIds());
        assertEquals(List.of(), updated.userFeatures().actionSequenceMovieIds());
    }

    @Test
    void retrievalSequenceHydratorDeduplicatesAndTruncatesToMax() {
        List<String> longList = IntStream.range(0, 150).mapToObj(i -> "m" + i).toList();
        RetrievalSequenceQueryHydrator hydrator = new RetrievalSequenceQueryHydrator(clientWithRecent(longList));
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(RetrievalSequenceQueryHydrator.MAX_RETRIEVAL_SEQ_LENGTH,
            updated.userFeatures().retrievalSequenceMovieIds().size());
        assertEquals("m0", updated.userFeatures().retrievalSequenceMovieIds().get(0));
    }

    @Test
    void scoringSequenceHydratorDerivesShortDenseWindow() {
        List<String> recent = List.of("s1", "s2", "s3");
        ScoringSequenceQueryHydrator hydrator = new ScoringSequenceQueryHydrator(clientWithRecent(recent));
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(recent, updated.userFeatures().scoringSequenceMovieIds());
        assertEquals(List.of(), updated.userFeatures().retrievalSequenceMovieIds());
        assertEquals(List.of(), updated.userFeatures().actionSequenceMovieIds());
    }

    @Test
    void scoringSequenceHydratorTruncatesToMax() {
        List<String> longList = IntStream.range(0, 60).mapToObj(i -> "m" + i).toList();
        ScoringSequenceQueryHydrator hydrator = new ScoringSequenceQueryHydrator(clientWithRecent(longList));
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(ScoringSequenceQueryHydrator.MAX_SCORING_SEQ_LENGTH,
            updated.userFeatures().scoringSequenceMovieIds().size());
        assertEquals("m0", updated.userFeatures().scoringSequenceMovieIds().get(0));
    }

    @Test
    void scoringSequenceHydratorDeduplicates() {
        ScoringSequenceQueryHydrator hydrator = new ScoringSequenceQueryHydrator(
            clientWithRecent(List.of("x", "y", "x", "z", "y")));
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(List.of("x", "y", "z"), updated.userFeatures().scoringSequenceMovieIds());
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
    void servedHistoryHydratorCopiesOnlyServedMovieIds() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withServedMovieIds(List.of("served1", "served2"));
        ServedHistoryQueryHydrator hydrator = new ServedHistoryQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("served1", "served2"), updated.userFeatures().servedMovieIds());
        assertEquals(List.of(), updated.userFeatures().subscribedUserIds());
    }

    @Test
    void subscribedUserIdsHydratorCopiesOnlySubscribedUserIds() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withSubscribedUserIds(List.of("friend1", "friend2"));
        SubscribedUserIdsQueryHydrator hydrator = new SubscribedUserIdsQueryHydrator(
            userId -> Optional.of(fetched)
        );
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
    void pastRequestTimestampsHydratorCopiesOnlyPastRequestTimestamps() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withPastRequestTimestamps(List.of(1000L, 2000L));
        PastRequestTimestampsQueryHydrator hydrator = new PastRequestTimestampsQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(1000L, 2000L), updated.userFeatures().pastRequestTimestamps());
        assertEquals(List.of(), updated.userFeatures().mutualFollowMinhash());
    }

    @Test
    void mutualFollowHydratorCopiesOnlyMinhash() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withMutualFollowMinhash(List.of(7L, 9L));
        MutualFollowQueryHydrator hydrator = new MutualFollowQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(7L, 9L), updated.userFeatures().mutualFollowMinhash());
        assertEquals(List.of(), updated.userFeatures().pastRequestTimestamps());
    }

    @Test
    void inferredGrokTopicsHydratorCopiesOnlyTopics() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withInferredGrokTopics(List.of(1, 0, 1));
        InferredGrokTopicsQueryHydrator hydrator = new InferredGrokTopicsQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(1, 0, 1), updated.userFeatures().inferredGrokTopics());
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
        assertEquals(List.of(), updated.userFeatures().inferredGrokTopics());
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
    void ipHydratorCopiesOnlyIpLocation() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withIpLocation("US-NY");
        IpQueryHydrator hydrator = new IpQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals("US-NY", updated.userFeatures().ipLocation());
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
    void cachedMoviesHydratorCopiesCachedMoviesAndFlag() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withCachedMovieIds(List.of("cached1", "cached2"), true);
        CachedMoviesQueryHydrator hydrator = new CachedMoviesQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("cached1", "cached2"), updated.userFeatures().cachedMovieIds());
        assertEquals(true, updated.userFeatures().hasCachedMovies());
    }

    @Test
    void relatedContentsHydratorCopiesContentCategoryIds() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withFollowedGrokTopics(List.of(3, 7, 12));
        RelatedContentsQueryHydrator hydrator = new RelatedContentsQueryHydrator(
            userId -> Optional.of(fetched));
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(3, 7, 12), updated.userFeatures().followedGrokTopics());
        assertEquals(List.of(), updated.userFeatures().followedStarterPacks());
    }

    @Test
    void followedStarterPacksHydratorFetchesFromDedicatedClient() {
        FollowedStarterPacksClient client = mock(FollowedStarterPacksClient.class);
        when(client.getFollowedPackIds("u1")).thenReturn(List.of(1, 0, 1));
        FollowedStarterPacksQueryHydrator hydrator = new FollowedStarterPacksQueryHydrator(client);
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(1, 0, 1), updated.userFeatures().followedStarterPacks());
        assertEquals(List.of(), updated.userFeatures().followedGrokTopics());
    }
}
