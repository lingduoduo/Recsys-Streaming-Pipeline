package com.demo.retrieval.service.clients;

import java.util.List;

public interface FollowedCollectionsClient {
    List<Integer> getFollowedPackIds(String userId);
}
