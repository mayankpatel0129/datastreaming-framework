package com.datastreaming.framework.core.tracking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Represents the status of a message in the streaming pipeline
 */
public class MessageStatus {
    
    private final String correlationId;
    private final String instanceId;
    private final String sourceQueue;
    private final String targetTopic;
    private final long createdAt;
    private volatile DeliveryStatus deliveryStatus;
    private volatile String kafkaPartition;
    private volatile Long kafkaOffset;
    private volatile String errorMessage;
    private volatile int retryCount;
    private volatile long lastRetryAt;
    
    @JsonCreator
    public MessageStatus(
            @JsonProperty("correlationId") String correlationId,
            @JsonProperty("instanceId") String instanceId,
            @JsonProperty("sourceQueue") String sourceQueue,
            @JsonProperty("targetTopic") String targetTopic,
            @JsonProperty("createdAt") long createdAt,
            @JsonProperty("deliveryStatus") DeliveryStatus deliveryStatus) {
        this.correlationId = correlationId;
        this.instanceId = instanceId;
        this.sourceQueue = sourceQueue;
        this.targetTopic = targetTopic;
        this.createdAt = createdAt;
        this.deliveryStatus = deliveryStatus;
        this.retryCount = 0;
    }
    
    public void markDelivered(RecordMetadata metadata) {
        this.deliveryStatus = DeliveryStatus.DELIVERED;
        this.kafkaPartition = String.valueOf(metadata.partition());
        this.kafkaOffset = metadata.offset();
    }
    
    public void markFailed(String errorMessage) {
        this.deliveryStatus = DeliveryStatus.FAILED;
        this.errorMessage = errorMessage;
    }
    
    public void incrementRetryCount() {
        this.retryCount++;
        this.lastRetryAt = System.currentTimeMillis();
    }
    
    // Getters
    public String getCorrelationId() { return correlationId; }
    public String getInstanceId() { return instanceId; }
    public String getSourceQueue() { return sourceQueue; }
    public String getTargetTopic() { return targetTopic; }
    public long getCreatedAt() { return createdAt; }
    public DeliveryStatus getDeliveryStatus() { return deliveryStatus; }
    public String getKafkaPartition() { return kafkaPartition; }
    public Long getKafkaOffset() { return kafkaOffset; }
    public String getErrorMessage() { return errorMessage; }
    public int getRetryCount() { return retryCount; }
    public long getLastRetryAt() { return lastRetryAt; }
}