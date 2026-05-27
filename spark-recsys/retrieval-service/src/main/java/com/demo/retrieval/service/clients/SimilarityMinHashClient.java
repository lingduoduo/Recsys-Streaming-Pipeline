package com.demo.retrieval.service.clients;

import java.util.List;

public interface SimilarityMinHashClient {
    List<Long> getMinHash(String userId);
}
