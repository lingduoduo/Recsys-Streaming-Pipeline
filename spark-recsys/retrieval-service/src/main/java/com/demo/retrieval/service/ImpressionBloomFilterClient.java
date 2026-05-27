package com.demo.retrieval.service;

import java.util.List;

public interface ImpressionBloomFilterClient {
    List<Long> getBloomFilterBits(String userId);
}
