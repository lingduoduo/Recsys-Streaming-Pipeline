package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.MovieLensFeatureClient;
import com.demo.retrieval.service.MovieLensUserFeatures;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * Derives the retrieval and action sequences from the user's recent rating history.
 *
 * Mirrors the Rust UserActionSeqQueryHydrator / RetrievalSequenceQueryHydrator pattern:
 * read the raw engagement stream, deduplicate (analogous to dense aggregation), then
 * produce two length-bounded sequences — a longer one for ANN retrieval and a shorter
 * one for sequential model input.
 *
 * recentlyRatedMovieIds (written by MovieLensContextCollectorStreamingJob, ordered
 * newest-first) is the MovieLens analogue of the user action sequence.
 */
@Component
public class UserEngagementHistoryQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_RETRIEVAL_SEQ_LENGTH = 100;
    public static final int MAX_ACTION_SEQ_LENGTH = 50;

    private final MovieLensFeatureClient featureClient;

    public UserEngagementHistoryQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> raw = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::recentlyRatedMovieIds)
            .orElseGet(List::of);

        List<String> deduped = deduplicate(raw);
        List<String> retrievalSeq = truncate(deduped, MAX_RETRIEVAL_SEQ_LENGTH);
        List<String> actionSeq = truncate(deduped, MAX_ACTION_SEQ_LENGTH);

        MovieLensUserFeatures updated = query.userFeatures()
            .withRetrievalSequenceMovieIds(retrievalSeq)
            .withActionSequenceMovieIds(actionSeq);
        return new ScoredMoviesQuery(userId, updated,
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        MovieLensUserFeatures updated = query.userFeatures()
            .withRetrievalSequenceMovieIds(hydrated.userFeatures().retrievalSequenceMovieIds())
            .withActionSequenceMovieIds(hydrated.userFeatures().actionSequenceMovieIds());
        return new ScoredMoviesQuery(query.userId(), updated,
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    private static List<String> deduplicate(List<String> items) {
        return List.copyOf(new LinkedHashSet<>(items));
    }

    private static List<String> truncate(List<String> items, int maxLen) {
        return items.size() > maxLen ? items.subList(0, maxLen) : items;
    }
}
