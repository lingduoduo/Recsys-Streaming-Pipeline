package com.demo.retrieval.service;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import com.demo.retrieval.service.filters.AgeFilter;
import com.demo.retrieval.service.filters.AuthorSocialgraphFilter;
import com.demo.retrieval.service.filters.AuthorSocialgraphFilter.IneligibleSubscriptionFilter;
import com.demo.retrieval.service.filters.CandidateFilterResult;
import com.demo.retrieval.service.filters.MutedKeywordFilter;
import com.demo.retrieval.service.filters.NewUserTopicIdsFilter;
import com.demo.retrieval.service.filters.RetweetDeduplicationFilter;
import com.demo.retrieval.service.filters.RetweetDeduplicationFilter.DedupConversationFilter;
import com.demo.retrieval.service.filters.RetweetDeduplicationFilter.DropDuplicatesFilter;
import com.demo.retrieval.service.filters.SelfMovieFilter;
import com.demo.retrieval.service.filters.TopicIdsFilter;
import com.demo.retrieval.service.filters.VFFilter;
import com.demo.retrieval.service.filters.VFFilter.AncillaryVFFilter;
import com.demo.retrieval.service.filters.VideoFilter;
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

    @Test
    void selfMovieFilterRemovesViewerAuthoredCandidates() {
        CandidateFilterResult result = new SelfMovieFilter().filter(
            ScoredMoviesQuery.forUser("viewer"),
            List.of(
                candidate("self").withCoreData("viewer", null, null, null, null),
                candidate("other").withCoreData("author", null, null, null, null)
            )
        );

        assertEquals(List.of("other"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("self"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void retweetDeduplicationFilterKeepsFirstSourceOccurrence() {
        List<MovieCandidate> candidates = List.of(
            candidate("original"),
            candidate("reshare_a").withCoreData(null, null, "original", null, null),
            candidate("reshare_b").withCoreData(null, null, "original", null, null),
            candidate("fresh")
        );

        CandidateFilterResult result = new RetweetDeduplicationFilter()
            .filter(ScoredMoviesQuery.forUser("u1"), candidates);

        assertEquals(List.of("original", "fresh"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("reshare_a", "reshare_b"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void vfFilterRemovesCandidatesWithVisibilityReasons() {
        CandidateFilterResult result = new VFFilter().filter(
            ScoredMoviesQuery.forUser("u1"),
            List.of(
                candidate("drop").withVisibility("safety_drop", false, List.of(), null),
                candidate("keep")
            )
        );

        assertEquals(List.of("keep"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("drop"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void videoFilterRemovesVideoCandidatesOnlyWhenEnabled() {
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1"),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            false,
            true
        );
        VideoFilter filter = new VideoFilter();

        CandidateFilterResult result = filter.filter(
            query,
            List.of(
                candidate("video").withQuote(null, null, null, 2_000),
                candidate("text")
            )
        );

        assertTrue(filter.enable(query));
        assertEquals(List.of("text"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("video"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void mutedKeywordFilterRemovesMatchingCoreText() {
        CandidateFilterResult result = new MutedKeywordFilter(List.of("spoiler alert")).filter(
            ScoredMoviesQuery.forUser("u1"),
            List.of(
                candidate("muted").withCoreData(null, null, null, null, "Major spoiler alert in this one"),
                candidate("kept").withCoreData(null, null, null, null, "quiet drama")
            )
        );

        assertEquals(List.of("kept"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("muted"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void topicIdsFilterKeepsRequestedTopicsAndRemovesExcludedTopics() {
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1"),
            List.of(),
            List.of(),
            List.of(),
            List.of(10),
            List.of(99),
            false,
            false
        );

        CandidateFilterResult result = new TopicIdsFilter().filter(
            query,
            List.of(
                candidate("requested").withFilteredTopics(List.of(10), List.of()),
                candidate("excluded").withFilteredTopics(List.of(10, 99), List.of()),
                candidate("other").withFilteredTopics(List.of(42), List.of()),
                candidate("untopiced")
            )
        );

        assertEquals(List.of("requested"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("other", "untopiced", "excluded"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void dropDuplicatesFilterKeepsFirstOccurrence() {
        List<MovieCandidate> candidates = List.of(
            candidate("m1"),
            candidate("m2"),
            candidate("m1"),
            candidate("m3"),
            candidate("m2")
        );

        CandidateFilterResult result = new DropDuplicatesFilter()
            .filter(ScoredMoviesQuery.forUser("u1"), candidates);

        assertEquals(List.of("m1", "m2", "m3"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("m1", "m2"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void dropDuplicatesFilterAllUniquePassesThrough() {
        List<MovieCandidate> candidates = List.of(candidate("a"), candidate("b"), candidate("c"));

        CandidateFilterResult result = new DropDuplicatesFilter()
            .filter(ScoredMoviesQuery.forUser("u1"), candidates);

        assertEquals(List.of("a", "b", "c"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertTrue(result.removed().isEmpty());
    }

    @Test
    void ancillaryVFFilterDropsFlaggedCandidates() {
        List<MovieCandidate> candidates = List.of(
            candidate("normal"),
            candidate("ancillary").withVisibility(null, true, List.of(), null),
            candidate("also_normal").withVisibility(null, false, List.of(), null)
        );

        CandidateFilterResult result = new AncillaryVFFilter()
            .filter(ScoredMoviesQuery.forUser("u1"), candidates);

        assertEquals(List.of("normal", "also_normal"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("ancillary"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void dedupConversationFilterKeepsHighestScoredPerThread() {
        // "reply-a" and "reply-b" share ancestor "root" → only highest popularity kept
        MovieCandidate root     = new MovieCandidate("root",    5.0, 0.0, false);
        MovieCandidate replyA   = new MovieCandidate("reply-a", 3.0, 0.0, false)
            .withVisibility(null, false, List.of("root"), null);
        MovieCandidate replyB   = new MovieCandidate("reply-b", 7.0, 0.0, false)
            .withVisibility(null, false, List.of("root"), null);
        MovieCandidate standalone = new MovieCandidate("standalone", 2.0, 0.0, false);

        CandidateFilterResult result = new DedupConversationFilter()
            .filter(ScoredMoviesQuery.forUser("u1"), List.of(root, replyA, replyB, standalone));

        // "root" wins its own slot (no ancestors → key="root"); "reply-b" wins the thread (7.0 > 3.0)
        assertEquals(List.of("root", "reply-b", "standalone"),
            result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("reply-a"),
            result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void dedupConversationFilterStandaloneMoviesPassThrough() {
        List<MovieCandidate> candidates = List.of(
            new MovieCandidate("m1", 4.0, 0.0, false),
            new MovieCandidate("m2", 2.0, 0.0, false)
        );

        CandidateFilterResult result = new DedupConversationFilter()
            .filter(ScoredMoviesQuery.forUser("u1"), candidates);

        assertEquals(List.of("m1", "m2"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertTrue(result.removed().isEmpty());
    }

    @Test
    void ineligibleSubscriptionFilterDropsUnsubscribedGatedContent() {
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1").withSubscribedUserIds(List.of("creator-99")),
            List.of(), List.of(), List.of()
        );
        List<MovieCandidate> candidates = List.of(
            candidate("public"),
            candidate("subscribed").withSubscriptionAuthorId("creator-99"),
            candidate("not_subscribed").withSubscriptionAuthorId("creator-77")
        );

        CandidateFilterResult result = new IneligibleSubscriptionFilter().filter(query, candidates);

        assertEquals(List.of("public", "subscribed"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("not_subscribed"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void ineligibleSubscriptionFilterKeepsAllPublicWhenNoSubscriptions() {
        List<MovieCandidate> candidates = List.of(
            candidate("public-a"),
            candidate("public-b"),
            candidate("subscription-gated").withSubscriptionAuthorId("any-creator")
        );

        CandidateFilterResult result = new IneligibleSubscriptionFilter()
            .filter(ScoredMoviesQuery.forUser("u1"), candidates);

        assertEquals(List.of("public-a", "public-b"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("subscription-gated"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    private MovieCandidate candidate(String id) {
        return new MovieCandidate(id, 1.0, 0.0, false);
    }

    private String snowflakeId(Instant createdAt) {
        return Long.toUnsignedString((createdAt.toEpochMilli() - TWITTER_SNOWFLAKE_EPOCH_MILLIS) << 22);
    }
}
