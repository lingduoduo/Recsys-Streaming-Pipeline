package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * Redis-backed UserActionAggregationClient.
 *
 * Reads recentlyRatedMovieIds (written newest-first by
 * MovieLensContextCollectorStreamingJob) and deduplicates preserving insertion
 * order, producing a dense sequence of unique movie IDs. Each hydrator then
 * truncates to its own window length.
 *
 * Mirrors the Rust UserActionAggregationClient.fetch_aggregated_sequence with
 * UserActionAggregationType::Dense (RetrievalSequence) and
 * UserActionAggregationType::DenseWithNotInterestedIn (ScoringSequence).
 * MovieLens has no "not interested in" signal, so both use the same dense dedup.
 */
@Component
public class RedisUserActionAggregationClient implements UserActionAggregationClient {

    private final MovieLensFeatureClient featureClient;

    public RedisUserActionAggregationClient(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public List<String> fetchDedupedSequence(String userId) {
        List<String> raw = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::recentlyRatedMovieIds)
            .orElseGet(List::of);
        return List.copyOf(new LinkedHashSet<>(raw));
    }
}
