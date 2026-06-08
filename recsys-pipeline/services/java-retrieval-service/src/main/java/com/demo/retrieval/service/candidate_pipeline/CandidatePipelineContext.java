package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.ScoredMoviesQuery;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public record CandidatePipelineContext(
    ScoredMoviesQuery query,
    Map<String, Double> popularityMap,
    Set<String> excludedItems,
    Set<String> userGenres,
    Set<String> userTags,
    FilterContext filterContext,
    int resultSize
) {
    public CandidatePipelineContext {
        popularityMap = popularityMap == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(popularityMap));
        excludedItems = excludedItems == null ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(excludedItems));
        userGenres = userGenres == null ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(userGenres));
        userTags = userTags == null ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(userTags));
        filterContext = filterContext == null ? FilterContext.empty() : filterContext;
    }

    public CandidatePipelineContext withQuery(ScoredMoviesQuery hydratedQuery) {
        return new CandidatePipelineContext(
            hydratedQuery,
            popularityMap,
            excludedItems,
            userGenres,
            userTags,
            filterContext,
            resultSize
        );
    }
}
