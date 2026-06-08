package com.demo.retrieval.kafka;

public record PartitionLag(int partitionId, long lag) {
    public int getPartitionId() { return partitionId; }
    public long getLag()        { return lag; }
}
