package com.demo.retrieval.service;

import java.util.List;

public interface ServedHistoryClient {
    List<String> getRecentlyServedMovieIds(String userId);
}
