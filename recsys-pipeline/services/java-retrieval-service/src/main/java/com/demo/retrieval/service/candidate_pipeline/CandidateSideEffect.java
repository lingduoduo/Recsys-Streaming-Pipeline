package com.demo.retrieval.service.candidate_pipeline;

@FunctionalInterface
public interface CandidateSideEffect {
    void run(CandidatePipelineContext context, CandidateSideEffectInput input);

    default boolean enable(CandidatePipelineContext context) {
        return true;
    }

    default String name() {
        return getClass().getSimpleName();
    }
}
