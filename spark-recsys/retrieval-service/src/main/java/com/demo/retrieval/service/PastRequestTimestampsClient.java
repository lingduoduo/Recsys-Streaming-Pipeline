package com.demo.retrieval.service;

import java.util.List;

public interface PastRequestTimestampsClient {
    List<Long> getTimestamps(String userId);
}
