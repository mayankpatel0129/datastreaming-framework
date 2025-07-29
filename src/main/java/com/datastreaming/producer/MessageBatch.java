package com.datastreaming.producer;

import java.util.List;

public class MessageBatch {
    
    public static class Message {
        private final String key;
        private final String value;
        private final String correlationId;

        public Message(String key, String value, String correlationId) {
            this.key = key;
            this.value = value;
            this.correlationId = correlationId;
        }

        public String getKey() { return key; }
        public String getValue() { return value; }
        public String getCorrelationId() { return correlationId; }
    }
}