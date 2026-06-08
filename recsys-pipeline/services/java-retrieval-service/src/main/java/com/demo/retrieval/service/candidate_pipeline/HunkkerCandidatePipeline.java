package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.candidate_hydrators.CandidateHydrator;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import com.demo.retrieval.service.filters.CandidateFilterResult;

import java.util.ArrayList;
import java.util.List;

public class HunkkerCandidatePipeline {
    private final List<CandidateSource> sources;
    private final List<CandidateHydrator> hydrators;
    private final List<PipelineCandidateFilter> filters;
    private final List<CandidateScorer> scorers;
    private final CandidateSelector selector;
    private final List<CandidateSideEffect> sideEffects;

    public HunkkerCandidatePipeline(
        List<CandidateSource> sources,
        List<CandidateHydrator> hydrators,
        List<PipelineCandidateFilter> filters,
        List<CandidateScorer> scorers,
        CandidateSelector selector,
        List<CandidateSideEffect> sideEffects
    ) {
        this.sources = List.copyOf(sources);
        this.hydrators = List.copyOf(hydrators);
        this.filters = List.copyOf(filters);
        this.scorers = List.copyOf(scorers);
        this.selector = selector;
        this.sideEffects = List.copyOf(sideEffects);
    }

    public CandidatePipelineResult execute(CandidatePipelineContext context) {
        List<MovieCandidate> retrieved = sources.stream()
            .filter(source -> source.enable(context))
            .flatMap(source -> source.fetch(context).stream())
            .toList();

        List<MovieCandidate> hydrated = retrieved;
        for (CandidateHydrator hydrator : hydrators) {
            if (hydrator.enable(context.query())) {
                hydrated = hydrator.hydrate(context.query(), hydrated);
            }
        }

        List<MovieCandidate> current = hydrated;
        List<MovieCandidate> removed = new ArrayList<>();
        for (PipelineCandidateFilter filter : filters) {
            if (filter.enable(context)) {
                CandidateFilterResult result = filter.filter(context, current);
                current = result.kept();
                removed.addAll(result.removed());
            }
        }

        List<MovieCandidate> scored = current;
        for (CandidateScorer scorer : scorers) {
            if (scorer.enable(context)) {
                scored = scorer.score(context, scored);
            }
        }

        CandidateSelection selection = selector.enable(context)
            ? selector.select(context, scored)
            : new CandidateSelection(scored, List.of());
        for (CandidateSideEffect sideEffect : sideEffects) {
            if (sideEffect.enable(context)) {
                sideEffect.run(context, selection.selected(), selection.nonSelected());
            }
        }
        return new CandidatePipelineResult(hydrated, removed, scored, selection.selected(), selection.nonSelected());
    }
}
