package com.demo.retrieval.service;

import java.util.List;

public interface FollowedGenreTopicsClient {
    List<String> getFollowedGenres(String userId);
}
