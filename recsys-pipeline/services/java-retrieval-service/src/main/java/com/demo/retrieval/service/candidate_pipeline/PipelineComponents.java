package com.demo.retrieval.service.candidate_pipeline;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public record PipelineComponents(Map<PipelineStage, List<String>> stages) {
    public PipelineComponents {
        EnumMap<PipelineStage, List<String>> copy = new EnumMap<>(PipelineStage.class);
        if (stages != null) {
            stages.forEach((stage, names) -> copy.put(stage, names == null ? List.of() : List.copyOf(names)));
        }
        stages = Map.copyOf(copy);
    }

    public List<String> names(PipelineStage stage) {
        return stages.getOrDefault(stage, List.of());
    }
}
