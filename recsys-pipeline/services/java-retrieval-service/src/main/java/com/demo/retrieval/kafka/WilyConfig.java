package com.demo.retrieval.kafka;

public record WilyConfig() {
    public static WilyConfig defaultConfig() { return new WilyConfig(); }
}
