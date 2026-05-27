package com.demo.retrieval.service;

import java.util.List;

public interface SimilarityMinHashClient {
    List<Long> getMinHash(String userId);
}
