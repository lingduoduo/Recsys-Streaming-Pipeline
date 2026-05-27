package com.demo.retrieval.service;

import java.util.List;

public interface UserActionAggregationClient {
    /**
     * Returns the user's action sequence deduplicated by movie ID (dense aggregation),
     * preserving recency order. Mirrors the Rust UserActionAggregationClient
     * fetch_aggregated_sequence with UserActionAggregationType::Dense.
     */
    List<String> fetchDedupedSequence(String userId);
}
