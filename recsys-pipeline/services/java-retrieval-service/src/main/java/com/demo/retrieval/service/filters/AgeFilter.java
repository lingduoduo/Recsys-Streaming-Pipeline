package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AgeFilter implements CandidateFilter {
    private static final long TWITTER_SNOWFLAKE_EPOCH_MILLIS = 1_288_834_974_657L;

    private final Duration maxAge;
    private final Clock clock;

    public AgeFilter(Duration maxAge) {
        this(maxAge, Clock.systemUTC());
    }

    public AgeFilter(Duration maxAge, Clock clock) {
        this.maxAge = maxAge == null ? Duration.ZERO : maxAge;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate -> isWithinAge(candidate.movieId())));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }

    private boolean isWithinAge(String movieId) {
        return createdAt(movieId)
            .map(createdAt -> !createdAt.isAfter(clock.instant())
                && Duration.between(createdAt, clock.instant()).compareTo(maxAge) <= 0)
            .orElse(false);
    }

    private java.util.Optional<Instant> createdAt(String movieId) {
        try {
            long id = Long.parseUnsignedLong(movieId);
            long timestampMillis = (id >> 22) + TWITTER_SNOWFLAKE_EPOCH_MILLIS;
            return java.util.Optional.of(Instant.ofEpochMilli(timestampMillis));
        } catch (NumberFormatException e) {
            return java.util.Optional.empty();
        }
    }
}
