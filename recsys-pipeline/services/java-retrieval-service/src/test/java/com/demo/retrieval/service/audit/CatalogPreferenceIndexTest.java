package com.demo.retrieval.service.audit;

import com.demo.retrieval.service.content.NormalizedProfile;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogPreferenceIndexTest {

    private static NormalizedProfile item(Set<String> genres, Set<String> tags) {
        return new NormalizedProfile("", genres, tags, tags, "", false, 0L);
    }

    private static CatalogPreferenceIndex index() {
        Map<String, NormalizedProfile> catalog = new LinkedHashMap<>();
        catalog.put("item9", item(Set.of("sci-fi", "adventure"), Set.of("space")));
        catalog.put("item1", item(Set.of("sci-fi"), Set.of("space", "classic")));
        catalog.put("item5", item(Set.of("drama"), Set.of()));
        return CatalogPreferenceIndex.build(catalog);
    }

    @Test
    void genreLookupReturnsMatchingItemsSortedById() {
        assertEquals(List.of("item1", "item9"), index().lookup(CatalogPreferenceIndex.KIND_GENRE, "sci-fi"));
        assertEquals(List.of("item5"), index().lookup(CatalogPreferenceIndex.KIND_GENRE, "drama"));
    }

    @Test
    void tagLookupIsSeparateFromGenres() {
        assertEquals(List.of("item1", "item9"), index().lookup(CatalogPreferenceIndex.KIND_TAG, "space"));
        assertEquals(List.of("item1"), index().lookup(CatalogPreferenceIndex.KIND_TAG, "classic"));
        assertTrue(index().lookup(CatalogPreferenceIndex.KIND_TAG, "sci-fi").isEmpty());
    }

    @Test
    void missingValueOrUnknownKindReturnsEmpty() {
        assertTrue(index().lookup(CatalogPreferenceIndex.KIND_GENRE, "film-noir").isEmpty());
        assertTrue(index().lookup("keyword", "space").isEmpty());
        assertTrue(index().lookup(CatalogPreferenceIndex.KIND_GENRE, null).isEmpty());
    }

    @Test
    void catalogSizeCountsItemsNotPostings() {
        assertEquals(3, index().catalogSize());
        assertEquals(0, CatalogPreferenceIndex.build(Map.of()).catalogSize());
    }
}
