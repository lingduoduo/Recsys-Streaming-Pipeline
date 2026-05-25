package com.demo.retrieval.service.filters;

import java.util.LinkedHashSet;
import java.util.Set;

public final class TopicIdExpansion {
    private TopicIdExpansion() {
    }

    public static Set<Integer> expand(Set<Integer> topicIds) {
        return topicIds == null ? Set.of() : new LinkedHashSet<>(topicIds);
    }
}
