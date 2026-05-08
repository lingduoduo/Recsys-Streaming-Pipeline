package com.demo.retrieval.controller;

import com.demo.retrieval.service.HybridRecommendationService;
import com.demo.retrieval.service.RecommendationResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(RecommendationController.class)
@SuppressWarnings({"unchecked", "null"})
class RecommendationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private StringRedisTemplate redis;

    @MockBean
    private HybridRecommendationService recommendationService;

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
        mockMvc.perform(get("/embedding/item!"))
            .andExpect(status().isBadRequest());
    }

    // --- /recommend ---

    @Test
    void recommendExcludesRecentlyViewedItems() throws Exception {
        when(recommendationService.recommend("u1", 6)).thenReturn(
            new RecommendationResult(
                "u1",
                List.of("item1", "item2"),
                List.of("item3", "item4"),
                List.of(Map.of("item", "item3", "coldStart", true)),
                Map.of("pseudoRegret", 0.1)
            )
        );

        mockMvc.perform(get("/recommend/u1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.recent[0]").value("item1"))
            .andExpect(jsonPath("$.recommendations[0]").value("item3"))
            .andExpect(jsonPath("$.recommendations[1]").value("item4"))
            .andExpect(jsonPath("$.diagnostics[0].item").value("item3"))
            .andExpect(jsonPath("$.metrics.pseudoRegret").value(0.1));
    }

    @Test
    void recommendReturnsEmptyListWhenRedisIsEmpty() throws Exception {
        when(recommendationService.recommend("u1", 6)).thenReturn(
            new RecommendationResult("u1", List.of(), List.of(), List.of(), Map.of("eligibleCandidateCount", 0))
        );

        mockMvc.perform(get("/recommend/u1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.recommendations").isEmpty());
    }

    @Test
    void recommendReturnsBadRequestForInvalidUserId() throws Exception {
        mockMvc.perform(get("/recommend/user!"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void recommendBoundsLimitToMaxLimit() throws Exception {
        when(recommendationService.recommend("u1", 50)).thenReturn(
            new RecommendationResult("u1", List.of(), List.of(), List.of(), Map.of())
        );

        mockMvc.perform(get("/recommend/u1?limit=999"))
            .andExpect(status().isOk());
    }

    @Test
    void feedbackEndpointDelegatesToRecommendationService() throws Exception {
        when(recommendationService.recordFeedback(any())).thenReturn(Map.of("status", "ok", "clicked", true));

        mockMvc.perform(post("/feedback")
                .contentType("application/json")
                .content("{\"user\":\"u1\",\"item\":\"item4\",\"clicked\":true,\"reward\":1.0}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"))
            .andExpect(jsonPath("$.clicked").value(true));
    }

    @Test
    void metricsEndpointReturnsAggregateMetrics() throws Exception {
        when(recommendationService.getAggregateMetrics()).thenReturn(Map.of("ctr", 0.25, "requests", 4));

        mockMvc.perform(get("/metrics"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.ctr").value(0.25))
            .andExpect(jsonPath("$.requests").value(4));
    }
}
