package com.demo.retrieval.evaluation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FiniteHorizonEnvironmentTest {
    @TempDir Path tempDir;

    @Test
    void seededCandidatePoolsAreReproducibleBoundedAndImmutable() throws Exception {
        MovieLensDataset data = dataset("""
                userId,movieId,rating,timestamp
                1,10,5,0
                1,20,4,0
                1,30,3,0
                2,20,5,0
                2,40,4,0
                3,40,3,0
                """);
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 3, 2, -1.0);

        FiniteHorizonEnvironment.State first = environment.initialState(1, new Random(17));
        FiniteHorizonEnvironment.State second = environment.initialState(1, new Random(17));

        assertThat(first).isEqualTo(second);
        assertThat(first.availableActions()).hasSizeLessThanOrEqualTo(3).doesNotHaveDuplicates();
        assertThatThrownBy(() -> first.availableActions().add(99))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void transitionRemovesOnlySelectedActionAndIncrementsStep() throws Exception {
        FiniteHorizonEnvironment environment = environment(3, 3, -1.0);
        FiniteHorizonEnvironment.State state =
                new FiniteHorizonEnvironment.State(1, List.of(10, 20, 30), 0);

        FiniteHorizonEnvironment.Step result = environment.step(state, 20);

        assertThat(result.nextState().availableActions()).containsExactly(10, 30);
        assertThat(result.nextState().step()).isEqualTo(1);
        assertThat(result.done()).isFalse();
    }

    @Test
    void unavailableActionFails() throws Exception {
        FiniteHorizonEnvironment environment = environment(3, 3, -1.0);
        FiniteHorizonEnvironment.State state =
                new FiniteHorizonEnvironment.State(1, List.of(10), 0);

        assertThatThrownBy(() -> environment.step(state, 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("available");
    }

    @Test
    void ratedRewardIsCenteredAndUnratedRewardRepresentsUnknownFeedback() throws Exception {
        FiniteHorizonEnvironment environment = environment(3, 3, -0.25);

        assertThat(environment.step(new FiniteHorizonEnvironment.State(1, List.of(10), 0), 10)
                .reward()).isEqualTo(2.0);
        assertThat(environment.step(new FiniteHorizonEnvironment.State(1, List.of(30), 0), 30)
                .reward()).isEqualTo(-0.25);
    }

    @Test
    void terminatesAtSlateSizeOrCandidateExhaustion() throws Exception {
        FiniteHorizonEnvironment slateLimited = environment(3, 1, -1.0);
        FiniteHorizonEnvironment exhausted = environment(3, 3, -1.0);

        assertThat(slateLimited.step(
                new FiniteHorizonEnvironment.State(1, List.of(10, 20), 0), 10).done()).isTrue();
        assertThat(exhausted.step(
                new FiniteHorizonEnvironment.State(1, List.of(10), 0), 10).done()).isTrue();
    }

    @Test
    void rolloutUsesExactDiscountPowers() throws Exception {
        FiniteHorizonEnvironment environment = environment(2, 2, -1.0);
        EvaluationPolicy ordered = (state, random) -> state.availableActions().get(0);

        FiniteHorizonEnvironment.Rollout result =
                environment.rollout(1, ordered, new Random(4), 0.5);

        assertThat(result.discountedReturn()).isEqualTo(2.5);
        assertThat(result.steps()).isEqualTo(2);
    }

    @Test
    void rejectsInvalidConfigurationAndDiscount() throws Exception {
        MovieLensDataset data = dataset(baseCsv());
        assertThatThrownBy(() -> new FiniteHorizonEnvironment(data, 0, 1, -1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FiniteHorizonEnvironment(data, 1, 0, -1.0))
                .isInstanceOf(IllegalArgumentException.class);
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 2, 2, -1.0);
        assertThatThrownBy(() -> environment.rollout(1, EvaluationPolicy.uniform(), new Random(), 1.01))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private FiniteHorizonEnvironment environment(int poolSize, int slateSize, double unratedReward)
            throws Exception {
        return new FiniteHorizonEnvironment(dataset(baseCsv()), poolSize, slateSize, unratedReward);
    }

    private String baseCsv() {
        return """
                userId,movieId,rating,timestamp
                1,10,5,0
                1,20,4,0
                2,10,3,0
                2,20,2,0
                2,30,5,0
                """;
    }

    private MovieLensDataset dataset(String csv) throws Exception {
        Path path = tempDir.resolve("ratings-" + Math.abs(csv.hashCode()) + ".csv");
        Files.writeString(path, csv);
        return MovieLensDataset.load(path, 0, 0);
    }
}
