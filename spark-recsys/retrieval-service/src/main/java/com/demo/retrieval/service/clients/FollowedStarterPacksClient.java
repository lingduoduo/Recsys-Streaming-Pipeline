package com.demo.retrieval.service.clients;

import java.util.List;

public interface FollowedStarterPacksClient {
    List<Integer> getFollowedPackIds(String userId);
}
