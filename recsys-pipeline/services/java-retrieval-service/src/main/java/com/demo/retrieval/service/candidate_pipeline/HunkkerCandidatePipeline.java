package com.demo.retrieval.service.candidate_pipeline;

import com.demo.retrieval.service.candidate_hydrators.CandidateHydrator;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import com.demo.retrieval.service.filters.CandidateFilterResult;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class HunkkerCandidatePipeline {
    private final List<PipelineQueryHydrator> queryHydrators;
    private final List<PipelineQueryHydrator> dependentQueryHydrators;
    private final List<CandidateSource> sources;
    private final List<CandidateHydrator> hydrators;
    private final List<PipelineCandidateFilter> filters;
    private final List<CandidateScorer> scorers;
    private final CandidateSelector selector;
    private final CandidateFinalizer finalizer;
    private final boolean truncateToResultSize;
    private final List<CandidateSideEffect> sideEffects;

    public HunkkerCandidatePipeline(
        List<CandidateSource> sources,
        List<CandidateHydrator> hydrators,
        List<PipelineCandidateFilter> filters,
        List<CandidateScorer> scorers,
        CandidateSelector selector,
        List<CandidateSideEffect> sideEffects
    ) {
        this(builder()
            .sources(sources)
            .hydrators(hydrators)
            .filters(filters)
            .scorers(scorers)
            .selector(selector)
            .sideEffects(sideEffects));
    }

    private HunkkerCandidatePipeline(Builder builder) {
        queryHydrators = List.copyOf(builder.queryHydrators);
        dependentQueryHydrators = List.copyOf(builder.dependentQueryHydrators);
        sources = List.copyOf(builder.sources);
        hydrators = List.copyOf(builder.hydrators);
        filters = List.copyOf(builder.filters);
        scorers = List.copyOf(builder.scorers);
        selector = Objects.requireNonNull(builder.selector, "selector");
        finalizer = builder.finalizer;
        truncateToResultSize = builder.truncateToResultSize;
        sideEffects = List.copyOf(builder.sideEffects);
    }

    public static Builder builder() {
        return new Builder();
    }

    public CandidatePipelineResult execute(CandidatePipelineContext originalContext) {
        CandidatePipelineContext context = hydrateQuery(originalContext, queryHydrators);
        context = hydrateQuery(context, dependentQueryHydrators);
        CandidatePipelineContext hydratedContext = context;

        List<MovieCandidate> retrieved = sources.stream()
            .filter(source -> source.enable(hydratedContext))
            .flatMap(source -> safeList(source.fetch(hydratedContext), source.name()).stream())
            .toList();

        List<MovieCandidate> hydrated = hydrate(context, retrieved, hydrators);
        FilteredCandidates filtered = filter(context, hydrated, filters);
        List<MovieCandidate> scored = score(context, filtered.kept());

        CandidateSelection selection = selector.enable(context)
            ? Objects.requireNonNull(selector.select(context, scored), selector.name() + " returned null")
            : new CandidateSelection(scored, List.of());

        List<MovieCandidate> selected = selection.selected();
        List<MovieCandidate> nonSelected = new ArrayList<>(selection.nonSelected());
        if (truncateToResultSize && selected.size() > Math.max(0, context.resultSize())) {
            int resultSize = Math.max(0, context.resultSize());
            nonSelected.addAll(selected.subList(resultSize, selected.size()));
            selected = List.copyOf(selected.subList(0, resultSize));
        }
        if (finalizer != null && finalizer.enable(context)) {
            selected = safeList(finalizer.finalize(context, selected), finalizer.name());
        }

        CandidatePipelineResult result = new CandidatePipelineResult(
            hydrated,
            filtered.removed(),
            scored,
            selected,
            nonSelected,
            context.query()
        );
        CandidateSideEffectInput input = new CandidateSideEffectInput(selected, nonSelected, result);
        for (CandidateSideEffect sideEffect : sideEffects) {
            if (sideEffect.enable(context)) {
                sideEffect.run(context, input);
            }
        }
        return result;
    }

    public PipelineComponents components() {
        EnumMap<PipelineStage, List<String>> stages = new EnumMap<>(PipelineStage.class);
        stages.put(PipelineStage.QUERY_HYDRATOR, names(queryHydrators, PipelineQueryHydrator::name));
        stages.put(PipelineStage.DEPENDENT_QUERY_HYDRATOR, names(dependentQueryHydrators, PipelineQueryHydrator::name));
        stages.put(PipelineStage.SOURCE, names(sources, CandidateSource::name));
        stages.put(PipelineStage.HYDRATOR, names(hydrators, CandidateHydrator::name));
        stages.put(PipelineStage.FILTER, names(filters, PipelineCandidateFilter::name));
        stages.put(PipelineStage.SCORER, names(scorers, CandidateScorer::name));
        stages.put(PipelineStage.SELECTOR, List.of(selector.name()));
        stages.put(PipelineStage.FINALIZER, finalizer == null ? List.of() : List.of(finalizer.name()));
        stages.put(PipelineStage.SIDE_EFFECT, names(sideEffects, CandidateSideEffect::name));
        return new PipelineComponents(stages);
    }

    private CandidatePipelineContext hydrateQuery(
        CandidatePipelineContext initial,
        List<PipelineQueryHydrator> stageHydrators
    ) {
        CandidatePipelineContext current = initial;
        for (PipelineQueryHydrator hydrator : stageHydrators) {
            if (hydrator.enable(current)) {
                current = Objects.requireNonNull(hydrator.hydrate(current), hydrator.name() + " returned null");
            }
        }
        return current;
    }

    private List<MovieCandidate> hydrate(
        CandidatePipelineContext context,
        List<MovieCandidate> initial,
        List<CandidateHydrator> stageHydrators
    ) {
        List<MovieCandidate> current = initial;
        for (CandidateHydrator hydrator : stageHydrators) {
            if (hydrator.enable(context.query())) {
                List<MovieCandidate> hydrated = safeList(
                    hydrator.hydrate(context.query(), current),
                    hydrator.name()
                );
                requireSameSize(hydrator.name(), current, hydrated);
                current = hydrated;
            }
        }
        return current;
    }

    private FilteredCandidates filter(
        CandidatePipelineContext context,
        List<MovieCandidate> initial,
        List<PipelineCandidateFilter> stageFilters
    ) {
        List<MovieCandidate> current = initial;
        List<MovieCandidate> removed = new ArrayList<>();
        for (PipelineCandidateFilter filter : stageFilters) {
            if (filter.enable(context)) {
                CandidateFilterResult result =
                    Objects.requireNonNull(filter.filter(context, current), filter.name() + " returned null");
                current = safeList(result.kept(), filter.name());
                removed.addAll(safeList(result.removed(), filter.name()));
            }
        }
        return new FilteredCandidates(current, removed);
    }

    private List<MovieCandidate> score(CandidatePipelineContext context, List<MovieCandidate> initial) {
        List<MovieCandidate> current = initial;
        for (CandidateScorer scorer : scorers) {
            if (scorer.enable(context)) {
                current = safeList(scorer.score(context, current), scorer.name());
            }
        }
        return current;
    }

    private static List<MovieCandidate> safeList(List<MovieCandidate> candidates, String componentName) {
        return List.copyOf(Objects.requireNonNull(candidates, componentName + " returned null"));
    }

    private static void requireSameSize(
        String componentName,
        List<MovieCandidate> input,
        List<MovieCandidate> output
    ) {
        if (input.size() != output.size()) {
            throw new IllegalStateException(
                componentName + " changed candidate count from " + input.size() + " to " + output.size()
            );
        }
    }

    private static <T> List<String> names(List<T> components, Function<T, String> name) {
        return components.stream().map(name).toList();
    }

    private record FilteredCandidates(List<MovieCandidate> kept, List<MovieCandidate> removed) {
    }

    public static final class Builder {
        private List<PipelineQueryHydrator> queryHydrators = List.of();
        private List<PipelineQueryHydrator> dependentQueryHydrators = List.of();
        private List<CandidateSource> sources = List.of();
        private List<CandidateHydrator> hydrators = List.of();
        private List<PipelineCandidateFilter> filters = List.of();
        private List<CandidateScorer> scorers = List.of();
        private CandidateSelector selector = (context, candidates) -> new CandidateSelection(candidates, List.of());
        private CandidateFinalizer finalizer;
        private boolean truncateToResultSize;
        private List<CandidateSideEffect> sideEffects = List.of();

        public Builder queryHydrators(List<PipelineQueryHydrator> value) {
            queryHydrators = list(value);
            return this;
        }

        public Builder dependentQueryHydrators(List<PipelineQueryHydrator> value) {
            dependentQueryHydrators = list(value);
            return this;
        }

        public Builder sources(List<CandidateSource> value) {
            sources = list(value);
            return this;
        }

        public Builder hydrators(List<CandidateHydrator> value) {
            hydrators = list(value);
            return this;
        }

        public Builder filters(List<PipelineCandidateFilter> value) {
            filters = list(value);
            return this;
        }

        public Builder scorers(List<CandidateScorer> value) {
            scorers = list(value);
            return this;
        }

        public Builder selector(CandidateSelector value) {
            selector = value;
            return this;
        }

        public Builder finalizer(CandidateFinalizer value) {
            finalizer = value;
            return this;
        }

        public Builder truncateToResultSize(boolean value) {
            truncateToResultSize = value;
            return this;
        }

        public Builder sideEffects(List<CandidateSideEffect> value) {
            sideEffects = list(value);
            return this;
        }

        public HunkkerCandidatePipeline build() {
            return new HunkkerCandidatePipeline(this);
        }

        private static <T> List<T> list(List<T> value) {
            return value == null ? List.of() : List.copyOf(value);
        }
    }
}
