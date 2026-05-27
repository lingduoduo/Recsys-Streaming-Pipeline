package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.UserActionAggregationClient;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Hydrates scoringSequenceMovieIds for the ranking model.
 *
 * Mirrors the Rust ScoringSequenceQueryHydrator: calls
 * UserActionAggregationClient.fetch_aggregated_sequence with
 * DenseWithNotInterestedIn aggregation and MaxSeqLengthScoring. MovieLens
 * has no "not interested in" signal, so Dense dedup is used for both sequences.
 * This hydrator truncates to the shorter scoring window.
 */
@Component
public class ScoringSequenceQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_SCORING_SEQ_LENGTH = 20;

    private final UserActionAggregationClient aggregationClient;

    public ScoringSequenceQueryHydrator(UserActionAggregationClient aggregationClient) {
        this.aggregationClient = aggregationClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<String> deduped = aggregationClient.fetchDedupedSequence(query.userId());
        List<String> scoringSeq = truncate(deduped, MAX_SCORING_SEQ_LENGTH);
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withScoringSequenceMovieIds(scoringSeq),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withScoringSequenceMovieIds(
                hydrated.userFeatures().scoringSequenceMovieIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    private static List<String> truncate(List<String> items, int maxLen) {
        return items.size() > maxLen ? items.subList(0, maxLen) : items;
    }
}
