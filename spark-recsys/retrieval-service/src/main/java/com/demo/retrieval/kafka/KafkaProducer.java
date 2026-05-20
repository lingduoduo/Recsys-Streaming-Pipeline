package com.demo.retrieval.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer {

    private final KafkaProducerConfig config;
    private final String defaultTopic;
    private org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> delegate;

    public KafkaProducer(KafkaProducerConfig config) {
        this.config = config;
        this.defaultTopic = config.baseConfig().topic();
    }

    public void start() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.baseConfig().dest());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        this.delegate = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    public void send(byte[] payload) {
        send(defaultTopic, payload);
    }

    public void send(String topic, byte[] payload) {
        delegate.send(new ProducerRecord<>(topic, payload));
    }

    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}
