package com.demo.retrieval.service;

import com.demo.retrieval.service.clients.RedisMovieLensFeatureClient;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static java.util.Map.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisMovieLensFeatureClientTest {

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
    private final RedisMovieLensFeatureClient client = new RedisMovieLensFeatureClient(redis);

    @Test
    void getUserFeaturesParsesStoredRedisHash() {
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries("user:u1:features")).thenReturn(Map.ofEntries(
            entry("favoriteGenres", "sci-fi, adventure, "),
            entry("avgRating", "4.25"),
            entry("ratingCount", "12"),
            entry("recentlyRatedMovieIds", "m1, m2"),
            entry("actionSequenceMovieIds", "m3, m4"),
            entry("retrievalSequenceMovieIds", "m5, m6"),
            entry("scoringSequenceMovieIds", "m7, m8"),
            entry("servedMovieIds", "m9, m10"),
            entry("subscribedUserIds", "friend1, friend2"),
            entry("mutedUserIds", "muted1, muted2"),
            entry("blockedUserIds", "blocked1, blocked2"),
            entry("followedUserIds", "followed1, followed2"),
            entry("pastRequestTimestamps", "1000, 2000, bad"),
            entry("mutualFollowMinhash", "11, 22, nope"),
            entry("inferredGenres", "1, 0, 1, nope"),
            entry("followedGenres", "0, 1, nope"),
            entry("followedCollections", "1, 1, bad"),
            entry("impressionBloomFilter", "44, 55, bad"),
            entry("impressedMovieIds", "m11, m12"),
            entry("cachedMovieIds", "m13, m14"),
            entry("hasCachedMovies", "true"),
            entry("ipLocation", "US-NY"),
            entry("age", "31"),
            entry("gender", "F"),
            entry("occupation", "engineer"),
            entry("zipCode", "10001"),
            entry("inferredGender", "female"),
            entry("inferredGenderScore", "0.93")
        ));

        Optional<MovieLensUserFeatures> result = client.getUserFeatures("u1");

        assertTrue(result.isPresent());
        assertEquals("u1", result.get().userId());
        assertEquals(java.util.List.of("sci-fi", "adventure"), result.get().favoriteGenres());
        assertEquals(4.25, result.get().avgRating());
        assertEquals(12, result.get().ratingCount());
        assertEquals(java.util.List.of("m1", "m2"), result.get().recentlyRatedMovieIds());
        assertEquals(java.util.List.of("m3", "m4"), result.get().actionSequenceMovieIds());
        assertEquals(java.util.List.of("m5", "m6"), result.get().retrievalSequenceMovieIds());
        assertEquals(java.util.List.of("m7", "m8"), result.get().scoringSequenceMovieIds());
        assertEquals(java.util.List.of("m9", "m10"), result.get().servedMovieIds());
        assertEquals(java.util.List.of("friend1", "friend2"), result.get().subscribedUserIds());
        assertEquals(java.util.List.of("muted1", "muted2"), result.get().mutedUserIds());
        assertEquals(java.util.List.of("blocked1", "blocked2"), result.get().blockedUserIds());
        assertEquals(java.util.List.of("followed1", "followed2"), result.get().followedUserIds());
        assertEquals(java.util.List.of(1000L, 2000L), result.get().pastRequestTimestamps());
        assertEquals(java.util.List.of(11L, 22L), result.get().mutualFollowMinhash());
        assertEquals(java.util.List.of(1, 0, 1), result.get().inferredGenres());
        assertEquals(java.util.List.of(0, 1), result.get().followedGenres());
        assertEquals(java.util.List.of(1, 1), result.get().followedCollections());
        assertEquals(java.util.List.of(44L, 55L), result.get().impressionBloomFilter());
        assertEquals(java.util.List.of("m11", "m12"), result.get().impressedMovieIds());
        assertEquals(java.util.List.of("m13", "m14"), result.get().cachedMovieIds());
        assertEquals(true, result.get().hasCachedMovies());
        assertEquals("US-NY", result.get().ipLocation());
        assertEquals(new UserDemographics(31, "F", "engineer", "10001"), result.get().demographics());
        assertEquals("female", result.get().inferredGender());
        assertEquals(0.93, result.get().inferredGenderScore());
    }

    @Test
    void getUserFeaturesReturnsEmptyForMissingHash() {
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries("user:missing:features")).thenReturn(Map.of());

        Optional<MovieLensUserFeatures> result = client.getUserFeatures("missing");

        assertTrue(result.isEmpty());
    }

    @Test
    void getUserFeaturesDefaultsMalformedNumbers() {
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries("user:u2:features")).thenReturn(Map.of(
            "avgRating", "not-a-double",
            "ratingCount", "not-a-long",
            "age", "not-a-long",
            "inferredGenderScore", "not-a-double"
        ));

        Optional<MovieLensUserFeatures> result = client.getUserFeatures("u2");

        assertTrue(result.isPresent());
        assertEquals(0.0, result.get().avgRating());
        assertEquals(0, result.get().ratingCount());
        assertEquals(0, result.get().demographics().age());
        assertEquals(null, result.get().inferredGenderScore());
    }
}
