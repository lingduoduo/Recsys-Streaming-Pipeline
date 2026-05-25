package com.demo.retrieval.service;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import com.demo.retrieval.service.filters.AgeFilter;
import com.demo.retrieval.service.filters.AuthorSocialgraphFilter;
import com.demo.retrieval.service.filters.CandidateFilterResult;
import com.demo.retrieval.service.filters.NewUserTopicIdsFilter;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CandidateFiltersTest {
    private static final long TWITTER_SNOWFLAKE_EPOCH_MILLIS = 1_288_834_974_657L;

    @Test
    void ageFilterKeepsOnlyRecentSnowflakeIds() {
        Instant now = Instant.parse("2026-05-25T12:00:00Z");
        AgeFilter filter = new AgeFilter(Duration.ofDays(2), Clock.fixed(now, ZoneOffset.UTC));
        String recentId = snowflakeId(now.minus(Duration.ofHours(6)));
        String oldId = snowflakeId(now.minus(Duration.ofDays(5)));

        CandidateFilterResult result = filter.filter(
            ScoredMoviesQuery.forUser("u1"),
            List.of(candidate(recentId), candidate(oldId), candidate("not-a-snowflake"))
        );

        assertEquals(List.of(recentId), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of(oldId, "not-a-snowflake"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void authorSocialgraphFilterRemovesMutedBlockedAndBlockingAuthors() {
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1")
                .withMutedUserIds(List.of("muted-author"))
                .withBlockedUserIds(List.of("blocked-author", "blocked-quote")),
            List.of(),
            List.of(),
            List.of()
        );
        List<MovieCandidate> candidates = List.of(
            candidate("muted").withCoreData("muted-author", null, null, null, null),
            candidate("blocked").withCoreData("blocked-author", null, null, null, null),
            candidate("quote_blocked").withQuote(null, "blocked-quote", null, null),
            candidate("author_blocks_viewer").withAuthorBlocksViewer(true),
            candidate("quoted_author_blocks_viewer").withQuote(null, null, true, null),
            candidate("kept").withCoreData("fresh-author", null, null, null, null)
        );

        CandidateFilterResult result = new AuthorSocialgraphFilter().filter(query, candidates);

        assertEquals(List.of("kept"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(
            List.of("muted", "blocked", "quote_blocked", "author_blocks_viewer", "quoted_author_blocks_viewer"),
            result.removed().stream().map(MovieCandidate::movieId).toList()
        );
    }

    @Test
    void newUserTopicFilterKeepsInNetworkAndMatchingTopicCandidates() {
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1").withInferredGrokTopics(List.of(11, 12)),
            List.of(),
            List.of(),
            List.of()
        );
        List<MovieCandidate> candidates = List.of(
            candidate("topic_match").withFilteredTopics(List.of(12), List.of()),
            candidate("in_network").withInNetwork(true),
            candidate("removed").withFilteredTopics(List.of(99), List.of())
        );

        NewUserTopicIdsFilter filter = new NewUserTopicIdsFilter();
        CandidateFilterResult result = filter.filter(query, candidates);

        assertTrue(filter.enable(query));
        assertEquals(List.of("topic_match", "in_network"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("removed"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void newUserTopicFilterDisabledWhenUserHasHistoryOrNoTopics() {
        NewUserTopicIdsFilter filter = new NewUserTopicIdsFilter();

        assertFalse(filter.enable(ScoredMoviesQuery.forUser("u1")));
        assertFalse(filter.enable(new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1").withInferredGrokTopics(List.of(11)),
            List.of("watched"),
            List.of(),
            List.of()
        )));
    }

    private MovieCandidate candidate(String id) {
        return new MovieCandidate(id, 1.0, 0.0, false);
    }

    private String snowflakeId(Instant createdAt) {
        return Long.toUnsignedString((createdAt.toEpochMilli() - TWITTER_SNOWFLAKE_EPOCH_MILLIS) << 22);
    }
}
