package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.sequence.SequenceClient;
import com.demo.retrieval.service.sequence.SequenceSchemaConstants;
import com.demo.retrieval.service.sequence.SequenceSlice;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RatingSequencesQueryHydratorSequenceStoreTest {

    /** Records the requested columns so column pruning is asserted, not assumed. */
    private static final class RecordingSequenceClient implements SequenceClient {
        Set<String> requestedColumns;
        int requestedMaxRows;
        Duration requestedLookback;
        final List<String> itemIds;

        RecordingSequenceClient(List<String> itemIds) {
            this.itemIds = itemIds;
        }

        @Override
        public SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback) {
            this.requestedColumns = columns;
            this.requestedMaxRows = maxRows;
            this.requestedLookback = lookback;
            List<String> timestamps = new ArrayList<>();
            for (int i = 0; i < itemIds.size(); i++) {
                timestamps.add(String.valueOf(1_000_000L - i));
            }
            Map<String, List<String>> columnMap = new LinkedHashMap<>();
            columnMap.put(SequenceSchemaConstants.COL_ITEM_ID, itemIds);
            columnMap.put(SequenceSchemaConstants.COL_TS, timestamps);
            return new SequenceSlice(columnMap, itemIds.size());
        }
    }

    private static List<String> ids(String prefix, int count) {
        List<String> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(prefix + i);
        }
        return out;
    }

    private static ScoredMoviesQuery query() {
        return ScoredMoviesQuery.forUser("u1");
    }

    private static MovieLensUserFeatures legacyFeatures(List<String> rated) {
        return new MovieLensUserFeatures("u1", List.of("drama"), 4.0, rated.size(), rated);
    }

    @Test
    void offModeIgnoresTheSequenceStoreEntirely() {
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(ids("new", 200));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 10))), sequenceClient, "off", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(ids("legacy", 10), result.actionSequenceMovieIds());
        assertNull(sequenceClient.requestedColumns, "sequence store must not be read in off mode");
    }

    @Test
    void shadowModeReadsBothButServesTheLegacyResult() {
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(ids("new", 200));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 10))), sequenceClient, "shadow", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(ids("legacy", 10), result.actionSequenceMovieIds());
        assertTrue(sequenceClient.requestedColumns != null, "shadow mode must still read the store");
    }

    @Test
    void onModeServesSequencesLongerThanTheLegacyFiftyItemCap() {
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(ids("m", 200));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 50))), sequenceClient, "on", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        // The criterion the whole design exists to satisfy.
        assertEquals(100, result.retrievalSequenceMovieIds().size());
        assertEquals(50, result.actionSequenceMovieIds().size());
        assertEquals(20, result.scoringSequenceMovieIds().size());
        assertEquals("m0", result.retrievalSequenceMovieIds().get(0));
    }

    @Test
    void onModeRequestsOnlyItemIdAndTimestampColumns() {
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(ids("m", 10));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(List.of())), sequenceClient, "on", 90
        );

        hydrator.hydrate(query());

        assertEquals(Set.of(SequenceSchemaConstants.COL_ITEM_ID, SequenceSchemaConstants.COL_TS),
            sequenceClient.requestedColumns);
        assertEquals(RatingSequencesQueryHydrator.MAX_RETRIEVAL_SEQ_LENGTH, sequenceClient.requestedMaxRows);
        assertEquals(Duration.ofDays(90), sequenceClient.requestedLookback);
    }

    @Test
    void onModeDeduplicatesWhilePreservingRecencyOrder() {
        RecordingSequenceClient sequenceClient =
            new RecordingSequenceClient(List.of("m1", "m2", "m1", "m3", "m2"));
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(List.of())), sequenceClient, "on", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(List.of("m1", "m2", "m3"), result.retrievalSequenceMovieIds());
    }

    @Test
    void onModeServesAnEmptySequenceRatherThanFallingBackToLegacy() {
        // An empty sequence for a user with no events is a correct answer. Falling back would
        // mask exactly the bug shadow mode exists to catch.
        RecordingSequenceClient sequenceClient = new RecordingSequenceClient(List.of());
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 10))), sequenceClient, "on", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(List.of(), result.actionSequenceMovieIds());
    }

    @Test
    void onModeFallsBackToLegacyWhenTheSequenceStoreThrows() {
        SequenceClient failing = (userId, kind, columns, maxRows, lookback) -> {
            throw new IllegalStateException("redis down");
        };
        RatingSequencesQueryHydrator hydrator = new RatingSequencesQueryHydrator(
            userId -> Optional.of(legacyFeatures(ids("legacy", 10))), failing, "on", 90
        );

        MovieLensUserFeatures result = hydrator.hydrate(query()).userFeatures();

        assertEquals(ids("legacy", 10), result.actionSequenceMovieIds());
    }
}
