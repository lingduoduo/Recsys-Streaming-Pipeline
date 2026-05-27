package com.demo.retrieval.service;

import com.demo.retrieval.service.query_hydrators.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserContextQueryHydratorsTest {

    @Test
    void engagementHistoryHydratorDerivesRetrievalAndActionSequences() {
        List<String> recent = List.of("m1", "m2", "m3", "m4", "m5");
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withActionSequenceMovieIds(List.of()) // source is recentlyRatedMovieIds
            .withRetrievalSequenceMovieIds(List.of());
        // recentlyRatedMovieIds must be set directly via the full constructor; use a minimal feature stub
        MovieLensFeatureClient client = userId -> Optional.of(new MovieLensUserFeatures(
            userId, List.of(), 4.0, 5, recent));

        UserEngagementHistoryQueryHydrator hydrator = new UserEngagementHistoryQueryHydrator(client);
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1", MovieLensUserFeatures.forUser("u1"),
            List.of("watched"), List.of("rated"), List.of("candidate")
        );

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(recent, updated.userFeatures().retrievalSequenceMovieIds());
        assertEquals(recent, updated.userFeatures().actionSequenceMovieIds());
        assertEquals(query.watchedMovieIds(), updated.watchedMovieIds());
        assertEquals(query.ratedMovieIds(), updated.ratedMovieIds());
        assertEquals(query.candidateMovieIds(), updated.candidateMovieIds());
    }

    @Test
    void engagementHistoryHydratorDeduplicatesBeforeTruncating() {
        List<String> withDupes = List.of("a", "b", "a", "c", "b", "d");
        MovieLensFeatureClient client = userId -> Optional.of(
            new MovieLensUserFeatures(userId, List.of(), 0.0, 6, withDupes));

        UserEngagementHistoryQueryHydrator hydrator = new UserEngagementHistoryQueryHydrator(client);
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        // dedup preserves first occurrence order
        assertEquals(List.of("a", "b", "c", "d"), updated.userFeatures().retrievalSequenceMovieIds());
        assertEquals(List.of("a", "b", "c", "d"), updated.userFeatures().actionSequenceMovieIds());
    }

    @Test
    void engagementHistoryHydratorTruncatesRetrievalToMax() {
        List<String> longList = java.util.stream.IntStream.range(0, 150)
            .mapToObj(i -> "m" + i).toList();
        MovieLensFeatureClient client = userId -> Optional.of(
            new MovieLensUserFeatures(userId, List.of(), 0.0, 150, longList));

        UserEngagementHistoryQueryHydrator hydrator = new UserEngagementHistoryQueryHydrator(client);
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(UserEngagementHistoryQueryHydrator.MAX_RETRIEVAL_SEQ_LENGTH,
            updated.userFeatures().retrievalSequenceMovieIds().size());
        assertEquals(UserEngagementHistoryQueryHydrator.MAX_ACTION_SEQ_LENGTH,
            updated.userFeatures().actionSequenceMovieIds().size());
    }

    @Test
    void engagementHistoryHydratorReturnsEmptyForUnknownUser() {
        UserEngagementHistoryQueryHydrator hydrator = new UserEngagementHistoryQueryHydrator(
            userId -> Optional.empty());
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertTrue(updated.userFeatures().retrievalSequenceMovieIds().isEmpty());
        assertTrue(updated.userFeatures().actionSequenceMovieIds().isEmpty());
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

        ScoredMoviesQuery hydrated = hydrator.hydrate(query);
        ScoredMoviesQuery updated = hydrator.update(query, hydrated);

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

        ScoredMoviesQuery hydrated = hydrator.hydrate(query);
        ScoredMoviesQuery updated = hydrator.update(query, hydrated);

        assertEquals("female", updated.userFeatures().inferredGender());
        assertEquals(0.91, updated.userFeatures().inferredGenderScore());
    }

    @Test
    void behaviorEventListHydratorDerivesShortScoringSequence() {
        List<String> recent = List.of("m1", "m2", "m3");
        MovieLensFeatureClient client = userId -> Optional.of(
            new MovieLensUserFeatures(userId, List.of(), 0.0, 3, recent));

        BehaviorEventListQueryHydrator hydrator = new BehaviorEventListQueryHydrator(client);
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(recent, updated.userFeatures().scoringSequenceMovieIds());
        assertEquals(List.of(), updated.userFeatures().retrievalSequenceMovieIds());
    }

    @Test
    void behaviorEventListHydratorTruncatesToScoringMax() {
        List<String> longList = java.util.stream.IntStream.range(0, 60)
            .mapToObj(i -> "m" + i).toList();
        MovieLensFeatureClient client = userId -> Optional.of(
            new MovieLensUserFeatures(userId, List.of(), 0.0, 60, longList));

        BehaviorEventListQueryHydrator hydrator = new BehaviorEventListQueryHydrator(client);
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(BehaviorEventListQueryHydrator.MAX_SCORING_SEQ_LENGTH,
            updated.userFeatures().scoringSequenceMovieIds().size());
        // head of list is preserved (most recent items)
        assertEquals("m0", updated.userFeatures().scoringSequenceMovieIds().get(0));
    }

    @Test
    void behaviorEventListHydratorDeduplicates() {
        List<String> withDupes = List.of("x", "y", "x", "z", "y");
        MovieLensFeatureClient client = userId -> Optional.of(
            new MovieLensUserFeatures(userId, List.of(), 0.0, 5, withDupes));

        BehaviorEventListQueryHydrator hydrator = new BehaviorEventListQueryHydrator(client);
        ScoredMoviesQuery updated = hydrator.update(
            ScoredMoviesQuery.forUser("u1"), hydrator.hydrate(ScoredMoviesQuery.forUser("u1")));

        assertEquals(List.of("x", "y", "z"), updated.userFeatures().scoringSequenceMovieIds());
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
    void mutedUserIdsHydratorCopiesOnlyMutedUserIds() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withMutedUserIds(List.of("muted1", "muted2"));
        MutedUserIdsQueryHydrator hydrator = new MutedUserIdsQueryHydrator(
            userId -> Optional.of(fetched)
        );
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
    void impressionBloomFilterHydratorCopiesOnlyBloomFilter() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withImpressionBloomFilter(List.of(44L, 55L));
        ImpressionBloomFilterQueryHydrator hydrator = new ImpressionBloomFilterQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(44L, 55L), updated.userFeatures().impressionBloomFilter());
        assertEquals(List.of(), updated.userFeatures().inferredGrokTopics());
    }

    @Test
    void impressedMoviesHydratorCopiesOnlyImpressedMovieIds() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withImpressedMovieIds(List.of("m11", "m12"));
        ImpressedMoviesQueryHydrator hydrator = new ImpressedMoviesQueryHydrator(
            userId -> Optional.of(fetched)
        );
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
    void blockedUserIdsHydratorCopiesOnlyBlockedUserIds() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withBlockedUserIds(List.of("blocked1", "blocked2"));
        BlockedUserIdsQueryHydrator hydrator = new BlockedUserIdsQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of("blocked1", "blocked2"), updated.userFeatures().blockedUserIds());
        assertEquals(List.of(), updated.userFeatures().followedUserIds());
    }

    @Test
    void followedUserIdsHydratorCopiesOnlyFollowedUserIds() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withFollowedUserIds(List.of("followed1", "followed2"));
        FollowedUserIdsQueryHydrator hydrator = new FollowedUserIdsQueryHydrator(
            userId -> Optional.of(fetched)
        );
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
    void followedGrokTopicsHydratorCopiesOnlyFollowedTopics() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withFollowedGrokTopics(List.of(0, 1));
        FollowedGrokTopicsQueryHydrator hydrator = new FollowedGrokTopicsQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(0, 1), updated.userFeatures().followedGrokTopics());
        assertEquals(List.of(), updated.userFeatures().followedStarterPacks());
    }

    @Test
    void followedStarterPacksHydratorCopiesOnlyFollowedPacks() {
        MovieLensUserFeatures fetched = MovieLensUserFeatures.forUser("u1")
            .withFollowedStarterPacks(List.of(1, 1));
        FollowedStarterPacksQueryHydrator hydrator = new FollowedStarterPacksQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("u1");

        ScoredMoviesQuery updated = hydrator.update(query, hydrator.hydrate(query));

        assertEquals(List.of(1, 1), updated.userFeatures().followedStarterPacks());
        assertEquals(List.of(), updated.userFeatures().followedGrokTopics());
    }
}
