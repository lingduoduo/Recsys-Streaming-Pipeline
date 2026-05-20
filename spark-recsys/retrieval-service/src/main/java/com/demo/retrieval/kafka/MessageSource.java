package com.demo.retrieval.kafka;

import java.util.List;

public interface MessageSource {
    List<KafkaMessage> poll(int timeoutMs);
    void commitOffsets();
    List<PartitionLag> getPartitionLags();
}
