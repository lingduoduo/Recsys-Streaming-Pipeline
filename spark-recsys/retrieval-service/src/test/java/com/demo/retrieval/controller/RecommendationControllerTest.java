package com.demo.retrieval.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.web.servlet.MockMvc;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(RecommendationController.class)
@SuppressWarnings({"unchecked", "null"})
class RecommendationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private StringRedisTemplate redis;

    // --- /embedding ---

    @Test
    void embeddingReturnsVectorFromRedis() throws Exception {
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        when(ops.get("i2vEmb:item1")).thenReturn("0.1 0.2 0.3");

        mockMvc.perform(get("/embedding/item1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.item").value("item1"))
            .andExpect(jsonPath("$.embedding[0]").value(0.1))
            .andExpect(jsonPath("$.embedding[1]").value(0.2));
    }

    @Test
    void embeddingReturnsEmptyListForUnknownItem() throws Exception {
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        when(ops.get("i2vEmb:unknown")).thenReturn(null);

        mockMvc.perform(get("/embedding/unknown"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.embedding").isEmpty());
    }

    @Test
    void embeddingReturnsCorruptDataErrorOnBadValues() throws Exception {
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        when(ops.get("i2vEmb:item1")).thenReturn("0.1 NOT_A_NUMBER 0.3");

        mockMvc.perform(get("/embedding/item1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.error").value("corrupt_data"));
    }

    @Test
    void embeddingReturnsBadRequestForInvalidItemId() throws Exception {
        mockMvc.perform(get("/embedding/../../etc/passwd"))
            .andExpect(status().isBadRequest());
    }

    // --- /recommend ---

    @Test
    void recommendExcludesRecentlyViewedItems() throws Exception {
        ListOperations<String, String> listOps = mock(ListOperations.class);
        ZSetOperations<String, String> zsetOps = mock(ZSetOperations.class);
        when(redis.opsForList()).thenReturn(listOps);
        when(redis.opsForZSet()).thenReturn(zsetOps);
        when(listOps.range("user:u1:recent", 0L, 5L)).thenReturn(List.of("item1", "item2"));
        Set<String> popular = new LinkedHashSet<>(List.of("item1", "item3", "item4"));
        when(zsetOps.reverseRange("global:item_popularity", 0L, 11L)).thenReturn(popular);

        mockMvc.perform(get("/recommend/u1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.recommendations[0]").value("item3"))
            .andExpect(jsonPath("$.recommendations[1]").value("item4"));
    }

    @Test
    void recommendReturnsEmptyListWhenRedisIsEmpty() throws Exception {
        ListOperations<String, String> listOps = mock(ListOperations.class);
        ZSetOperations<String, String> zsetOps = mock(ZSetOperations.class);
        when(redis.opsForList()).thenReturn(listOps);
        when(redis.opsForZSet()).thenReturn(zsetOps);
        when(listOps.range(anyString(), anyLong(), anyLong())).thenReturn(List.of());
        when(zsetOps.reverseRange(anyString(), anyLong(), anyLong())).thenReturn(Set.of());

        mockMvc.perform(get("/recommend/u1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.recommendations").isEmpty());
    }

    @Test
    void recommendReturnsBadRequestForInvalidUserId() throws Exception {
        mockMvc.perform(get("/recommend/<script>alert(1)</script>"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void recommendBoundsLimitToMaxLimit() throws Exception {
        ListOperations<String, String> listOps = mock(ListOperations.class);
        ZSetOperations<String, String> zsetOps = mock(ZSetOperations.class);
        when(redis.opsForList()).thenReturn(listOps);
        when(redis.opsForZSet()).thenReturn(zsetOps);
        when(listOps.range(anyString(), anyLong(), anyLong())).thenReturn(List.of());
        when(zsetOps.reverseRange(anyString(), anyLong(), anyLong())).thenReturn(Set.of());

        // limit=999 must be capped to MAX_LIMIT=50: range call uses (0, 49)
        mockMvc.perform(get("/recommend/u1?limit=999"))
            .andExpect(status().isOk());
    }
}
