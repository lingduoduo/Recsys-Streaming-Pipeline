package com.demo.retrieval.service;

import java.util.List;

public interface FollowedStarterPacksClient {
    List<Integer> getFollowedPackIds(String userId);
}
