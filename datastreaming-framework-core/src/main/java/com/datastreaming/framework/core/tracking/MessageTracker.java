package com.datastreaming.framework.core.tracking;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Interface for message tracking implementations
 */
public interface MessageTracker {
    
    /**
     * Start tracking a message
     */
    void trackMessage(String correlationId, String sourceQueue, String targetTopic);
    
    /**
     * Record successful message delivery
     */
    void recordSuccess(String correlationId, RecordMetadata metadata);
    
    /**
     * Record failed message delivery
     */
    void recordFailure(String correlationId, String errorMessage);
    
    /**
     * Get current reconciliation statistics
     */
    ReconciliationStats getStats();
    
    /**
     * Check if the tracker is healthy
     */
    boolean isHealthy();
}