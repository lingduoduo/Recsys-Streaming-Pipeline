package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.MovieLensFeatureClient;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * Hydrates all three rating-sequence fields in a single feature-store read.
 *
 * Previously UserActionSequenceQueryHydrator, RetrievalSequenceQueryHydrator, and
 * ScoringSequenceQueryHydrator each called getUserFeatures() independently, producing
 * three Redis round-trips for the same user:{id}:features key. This hydrator fetches
 * once, deduplicates once, and derives all three sequences by truncation:
 *
 *   actionSequenceMovieIds    — 50 items   (sequential model input)
 *   retrievalSequenceMovieIds — 100 items  (ANN candidate retrieval)
 *   scoringSequenceMovieIds   — 20 items   (ranking model input)
 */
@Component
public class RatingSequencesQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_ACTION_SEQ_LENGTH = 50;
    public static final int MAX_RETRIEVAL_SEQ_LENGTH = 100;
    public static final int MAX_SCORING_SEQ_LENGTH = 20;

    private final MovieLensFeatureClient featureClient;

    public RatingSequencesQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<String> raw = featureClient.getUserFeatures(query.userId())
            .map(MovieLensUserFeatures::recentlyRatedMovieIds)
            .orElseGet(List::of);
        List<String> deduped = List.copyOf(new LinkedHashSet<>(raw));
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures()
                .withActionSequenceMovieIds(truncate(deduped, MAX_ACTION_SEQ_LENGTH))
                .withRetrievalSequenceMovieIds(truncate(deduped, MAX_RETRIEVAL_SEQ_LENGTH))
                .withScoringSequenceMovieIds(truncate(deduped, MAX_SCORING_SEQ_LENGTH)),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        MovieLensUserFeatures hf = hydrated.userFeatures();
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures()
                .withActionSequenceMovieIds(hf.actionSequenceMovieIds())
                .withRetrievalSequenceMovieIds(hf.retrievalSequenceMovieIds())
                .withScoringSequenceMovieIds(hf.scoringSequenceMovieIds()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    private static List<String> truncate(List<String> items, int maxLen) {
        return items.size() > maxLen ? items.subList(0, maxLen) : items;
    }
}
