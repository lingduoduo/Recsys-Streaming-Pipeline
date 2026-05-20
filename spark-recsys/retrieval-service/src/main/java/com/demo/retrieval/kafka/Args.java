package com.demo.retrieval.kafka;

public record Args(
        String saslPassword,
        String producerSaslPassword,
        String securityProtocol,
        String saslMechanism,
        String saslUsername,
        String producerSaslMechanism,
        String producerSaslUsername,
        String kafkaGroupId,
        String inNetworkEventsConsumerDest,
        String autoOffsetReset,
        long fetchTimeoutMs,
        boolean skipToLatest,
        boolean serving,
        int numPartitions,
        int kafkaNumThreads,
        long lagMonitorIntervalSecs,
        int kafkaBatchSize) {

    public String getSaslPassword()                  { return saslPassword; }
    public String getProducerSaslPassword()          { return producerSaslPassword; }
    public String getSecurityProtocol()              { return securityProtocol; }
    public String getSaslMechanism()                 { return saslMechanism; }
    public String getSaslUsername()                  { return saslUsername; }
    public String getProducerSaslMechanism()         { return producerSaslMechanism; }
    public String getProducerSaslUsername()          { return producerSaslUsername; }
    public String getKafkaGroupId()                  { return kafkaGroupId; }
    public String getInNetworkEventsConsumerDest()   { return inNetworkEventsConsumerDest; }
    public String getAutoOffsetReset()               { return autoOffsetReset; }
    public long getFetchTimeoutMs()                  { return fetchTimeoutMs; }
    public boolean isSkipToLatest()                  { return skipToLatest; }
    public boolean isServing()                       { return serving; }
    public int getNumPartitions()                    { return numPartitions; }
    public int getKafkaNumThreads()                  { return kafkaNumThreads; }
    public long getLagMonitorIntervalSecs()          { return lagMonitorIntervalSecs; }
    public int getKafkaBatchSize()                   { return kafkaBatchSize; }
}
