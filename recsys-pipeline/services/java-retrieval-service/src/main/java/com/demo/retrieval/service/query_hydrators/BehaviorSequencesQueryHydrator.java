package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.sequence.SequenceClient;
import com.demo.retrieval.service.sequence.SequenceSchemaConstants;
import com.demo.retrieval.service.sequence.SequenceSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Turns the unified {@code behavior} sequence into recent watched history.
 *
 * The sequence holds every behavioral action a user took — searches included — so this
 * adapter keeps only the ones that name an item the user showed intent toward, and drops the
 * empty sentinel a search occupies to keep the columnar rows aligned.
 *
 * The first slice deliberately writes into the existing {@code watchedMovieIds} view rather
 * than adding a field to the public query, so nothing downstream has to change to benefit.
 * Selected by {@code recsys.sequence.mode}, the same switch {@link RatingSequencesQueryHydrator}
 * reads:
 *   off    — legacy watched history only, no read at all
 *   shadow — read and log the comparison, still serve legacy
 *   on     — behavior history first, then legacy, de-duplicated and bounded
 */
public class BehaviorSequencesQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final String MODE_OFF = "off";
    public static final String MODE_SHADOW = "shadow";
    public static final String MODE_ON = "on";

    /** Actions that name an item the user engaged with. A search names a query, not a movie. */
    private static final Set<String> ITEM_BEARING_ACTIONS = Set.of("result_view", "detail_view", "click");

    private static final Logger log = LoggerFactory.getLogger(BehaviorSequencesQueryHydrator.class);
    private static final Set<String> READ_COLUMNS = Set.of(
        SequenceSchemaConstants.COL_ITEM_ID,
        SequenceSchemaConstants.COL_TS,
        SequenceSchemaConstants.COL_ACTION);

    private final SequenceClient sequenceClient;
    private final String mode;
    private final Duration lookback;
    private final int maxItems;

    public BehaviorSequencesQueryHydrator(
        SequenceClient sequenceClient,
        String mode,
        int lookbackDays,
        int maxItems
    ) {
        this.sequenceClient = sequenceClient;
        this.mode = resolveMode(mode);
        this.lookback = Duration.ofDays(Math.max(1, lookbackDays));
        this.maxItems = Math.max(1, maxItems);
    }

    private static String resolveMode(String mode) {
        String normalized = mode == null ? MODE_OFF : mode.trim().toLowerCase();
        if (MODE_OFF.equals(normalized) || MODE_SHADOW.equals(normalized) || MODE_ON.equals(normalized)) {
            return normalized;
        }
        log.warn("Unrecognized recsys.sequence.mode '{}', treating as '{}'", mode, MODE_OFF);
        return MODE_OFF;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<String> watched = query.watchedMovieIds();

        if (!MODE_OFF.equals(mode) && sequenceClient != null) {
            try {
                List<String> fromStore = readBehaviorItems(query.userId());
                if (MODE_SHADOW.equals(mode)) {
                    log.info("behavior-shadow user={} legacyLen={} storeLen={}",
                        query.userId(), watched.size(), fromStore.size());
                } else {
                    watched = merge(fromStore, query.watchedMovieIds());
                }
            } catch (RuntimeException e) {
                log.warn("Behavior sequence read failed for user {}, using legacy history: {}",
                    query.userId(), e.getMessage());
            }
        }

        return withWatched(query, watched);
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return withWatched(query, hydrated.watchedMovieIds());
    }

    private static ScoredMoviesQuery withWatched(ScoredMoviesQuery query, List<String> watched) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures(),
            watched,
            query.ratedMovieIds(),
            query.candidateMovieIds(),
            query.genreIds(),
            query.excludedGenreIds(),
            query.bulkTopicRequest(),
            query.excludeVideos()
        );
    }

    /**
     * The item ids of item-bearing actions, newest first.
     *
     * The item and action columns are positionally aligned, so a slice whose columns disagree
     * in length is read only as far as the shorter one rather than throwing on the first row
     * that has no partner.
     */
    private List<String> readBehaviorItems(String userId) {
        SequenceSlice slice = sequenceClient.read(
            userId, SequenceSchemaConstants.KIND_BEHAVIOR, READ_COLUMNS, maxItems, lookback);
        List<String> itemIds = slice.itemIds();
        List<String> actions = slice.column(SequenceSchemaConstants.COL_ACTION);

        List<String> out = new ArrayList<>(Math.min(itemIds.size(), actions.size()));
        for (int i = 0; i < itemIds.size() && i < actions.size(); i++) {
            String itemId = itemIds.get(i);
            if (itemId != null && !itemId.isBlank() && ITEM_BEARING_ACTIONS.contains(actions.get(i))) {
                out.add(itemId);
            }
        }
        return out;
    }

    /** Behavior first, then whatever legacy history the query already carried. */
    private List<String> merge(List<String> behavior, List<String> legacy) {
        LinkedHashSet<String> merged = new LinkedHashSet<>(behavior);
        merged.addAll(legacy);
        List<String> out = new ArrayList<>(merged);
        return List.copyOf(out.size() > maxItems ? out.subList(0, maxItems) : out);
    }
}
