package com.demo.retrieval.service.candidate_pipeline;

@FunctionalInterface
public interface PipelineQueryHydrator {
    CandidatePipelineContext hydrate(CandidatePipelineContext context);

    default boolean enable(CandidatePipelineContext context) {
        return true;
    }

    default String name() {
        return getClass().getSimpleName();
    }
}
