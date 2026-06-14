package com.demo.retrieval.service;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class TwoTowerPredictionServiceTest {

    @Test
    void isDisabledWhenEnvVarsNotSet() {
        // No ONNX_USER_TOWER_PATH, ONNX_ITEM_TOWER_PATH, ONNX_RANKING_PATH set
        TwoTowerPredictionService svc = new TwoTowerPredictionService();
        assertThat(svc.isEnabled()).isFalse();
    }

    @Test
    void predictBatchReturnsEmptyWhenDisabled() {
        TwoTowerPredictionService svc = new TwoTowerPredictionService();
        Map<String, Double> result = svc.predictBatch("user1", List.of("item1", "item2"));
        assertThat(result).isEmpty();
    }
}
