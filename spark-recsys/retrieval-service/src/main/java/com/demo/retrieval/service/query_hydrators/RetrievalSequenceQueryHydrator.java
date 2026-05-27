package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.UserActionAggregationClient;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Hydrates retrievalSequenceMovieIds for ANN/embedding candidate retrieval.
 *
 * Mirrors the Rust RetrievalSequenceQueryHydrator: calls
 * UserActionAggregationClient.fetch_aggregated_sequence with Dense aggregation
 * and MaxSeqLengthRetrieval. The client returns a deduplicated sequence;
 * this hydrator truncates to the retrieval window length.
 */
@Component
public class RetrievalSequenceQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_RETRIEVAL_SEQ_LENGTH = 100;

    private final UserActionAggregationClient aggregationClient;

    public RetrievalSequenceQueryHydrator(UserActionAggregationClient aggregationClient) {
        this.aggregationClient = aggregationClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<String> deduped = aggregationClient.fetchDedupedSequence(query.userId());
        List<String> retrievalSeq = truncate(deduped, MAX_RETRIEVAL_SEQ_LENGTH);
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withRetrievalSequenceMovieIds(retrievalSeq),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withRetrievalSequenceMovieIds(
                hydrated.userFeatures().retrievalSequenceMovieIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    private static List<String> truncate(List<String> items, int maxLen) {
        return items.size() > maxLen ? items.subList(0, maxLen) : items;
    }
}
