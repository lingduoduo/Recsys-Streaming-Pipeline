package com.demo.retrieval.service.candidate_pipeline;

public enum PipelineStage {
    QUERY_HYDRATOR,
    DEPENDENT_QUERY_HYDRATOR,
    SOURCE,
    HYDRATOR,
    FILTER,
    SCORER,
    SELECTOR,
    FINALIZER,
    SIDE_EFFECT
}
