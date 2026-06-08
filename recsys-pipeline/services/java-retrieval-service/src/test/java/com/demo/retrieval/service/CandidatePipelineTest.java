package com.demo.retrieval.service;

import com.demo.retrieval.service.candidate_hydrators.CandidateHydrator;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import com.demo.retrieval.service.candidate_pipeline.CandidatePipelineContext;
import com.demo.retrieval.service.candidate_pipeline.CandidatePipelineResult;
import com.demo.retrieval.service.candidate_pipeline.CandidateSelection;
import com.demo.retrieval.service.candidate_pipeline.CandidateSideEffectInput;
import com.demo.retrieval.service.candidate_pipeline.FilterContext;
import com.demo.retrieval.service.candidate_pipeline.HunkkerCandidatePipeline;
import com.demo.retrieval.service.candidate_pipeline.PipelineQueryHydrator;
import com.demo.retrieval.service.candidate_pipeline.PipelineStage;
import com.demo.retrieval.service.filters.CandidateFilterResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CandidatePipelineTest {
    @Test
    void executesRustInspiredStagesInOrder() {
        List<String> stages = new ArrayList<>();
        AtomicReference<CandidateSideEffectInput> sideEffectInput = new AtomicReference<>();

        PipelineQueryHydrator queryHydrator = namedQueryHydrator("query", stages, "query");
        PipelineQueryHydrator dependentQueryHydrator = namedQueryHydrator("dependent-query", stages, "dependent");
        CandidateHydrator hydrator = namedHydrator("hydrator", stages, 1.0);

        HunkkerCandidatePipeline pipeline = HunkkerCandidatePipeline.builder()
            .queryHydrators(List.of(queryHydrator))
            .dependentQueryHydrators(List.of(dependentQueryHydrator))
            .sources(List.of(context -> {
                stages.add("source");
                assertEquals(List.of("query", "dependent"), context.query().candidateMovieIds());
                return List.of(candidate("a", 0), candidate("b", 0), candidate("c", 0));
            }))
            .hydrators(List.of(hydrator))
            .filters(List.of((context, candidates) -> {
                stages.add("filter");
                return new CandidateFilterResult(candidates.subList(0, 2), candidates.subList(2, 3));
            }))
            .scorers(List.of((context, candidates) -> {
                stages.add("scorer");
                return candidates;
            }))
            .selector((context, candidates) -> {
                stages.add("selector");
                return new CandidateSelection(candidates, List.of());
            })
            .truncateToResultSize(true)
            .finalizer((context, candidates) -> {
                stages.add("finalizer");
                List<MovieCandidate> reversed = new ArrayList<>(candidates);
                Collections.reverse(reversed);
                return reversed;
            })
            .sideEffects(List.of((context, input) -> {
                stages.add("side-effect");
                sideEffectInput.set(input);
            }))
            .build();

        CandidatePipelineResult result = pipeline.execute(context(1));

        assertEquals(
            List.of(
                "query", "dependent-query", "source", "hydrator", "filter", "scorer",
                "selector", "finalizer", "side-effect"
            ),
            stages
        );
        assertEquals(List.of("a"), ids(result.selectedCandidates()));
        assertEquals(List.of("c"), ids(result.filteredCandidates()));
        assertEquals(List.of("b"), ids(result.nonSelectedCandidates()));
        assertEquals(List.of("query", "dependent"), result.query().candidateMovieIds());
        assertEquals(result, sideEffectInput.get().result());
        assertEquals(List.of("query"), pipeline.components().names(PipelineStage.QUERY_HYDRATOR));
        assertEquals(List.of("hydrator"), pipeline.components().names(PipelineStage.HYDRATOR));
    }

    @Test
    void skipsDisabledComponents() {
        HunkkerCandidatePipeline pipeline = HunkkerCandidatePipeline.builder()
            .sources(List.of(new com.demo.retrieval.service.candidate_pipeline.CandidateSource() {
                @Override
                public List<MovieCandidate> fetch(CandidatePipelineContext context) {
                    throw new AssertionError("disabled source ran");
                }

                @Override
                public boolean enable(CandidatePipelineContext context) {
                    return false;
                }
            }))
            .build();

        assertTrue(pipeline.execute(context(10)).selectedCandidates().isEmpty());
    }

    @Test
    void rejectsHydratorsThatChangeCandidateCount() {
        HunkkerCandidatePipeline pipeline = HunkkerCandidatePipeline.builder()
            .sources(List.of(context -> List.of(candidate("a", 0))))
            .hydrators(List.of((query, candidates) -> List.of()))
            .build();

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> pipeline.execute(context(10)));
        assertTrue(error.getMessage().contains("changed candidate count from 1 to 0"));
    }

    private static PipelineQueryHydrator namedQueryHydrator(
        String name,
        List<String> stages,
        String candidateId
    ) {
        return new PipelineQueryHydrator() {
            @Override
            public CandidatePipelineContext hydrate(CandidatePipelineContext context) {
                stages.add(name);
                List<String> ids = new ArrayList<>(context.query().candidateMovieIds());
                ids.add(candidateId);
                ScoredMoviesQuery query = context.query();
                return context.withQuery(new ScoredMoviesQuery(
                    query.userId(),
                    query.userFeatures(),
                    query.watchedMovieIds(),
                    query.ratedMovieIds(),
                    ids,
                    query.genreIds(),
                    query.excludedGenreIds(),
                    query.bulkTopicRequest(),
                    query.excludeVideos()
                ));
            }

            @Override
            public String name() {
                return name;
            }
        };
    }

    private static CandidateHydrator namedHydrator(String name, List<String> stages, double score) {
        return new CandidateHydrator() {
            @Override
            public List<MovieCandidate> hydrate(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
                stages.add(name);
                return candidates.stream()
                    .map(candidate -> candidate(candidate.movieId(), score))
                    .toList();
            }

            @Override
            public String name() {
                return name;
            }
        };
    }

    private static CandidatePipelineContext context(int resultSize) {
        return new CandidatePipelineContext(
            ScoredMoviesQuery.forUser("user"),
            null,
            null,
            null,
            null,
            FilterContext.empty(),
            resultSize
        );
    }

    private static MovieCandidate candidate(String id, double contentScore) {
        return new MovieCandidate(id, 0.0, contentScore, false);
    }

    private static List<String> ids(List<MovieCandidate> candidates) {
        return candidates.stream().map(MovieCandidate::movieId).toList();
    }
}
