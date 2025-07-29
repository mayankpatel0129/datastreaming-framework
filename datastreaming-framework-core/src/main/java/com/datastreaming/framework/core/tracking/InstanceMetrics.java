package com.datastreaming.framework.core.tracking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metrics for a streaming framework instance
 */
public class InstanceMetrics {
    
    private final String instanceId;
    private final long timestamp;
    private final long totalTracked;
    private final long successfulDeliveries;
    private final long failedDeliveries;
    private final int pendingCount;
    private final int timeoutCount;
    private final int retryCount;
    
    @JsonCreator
    public InstanceMetrics(
            @JsonProperty("instanceId") String instanceId,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("totalTracked") long totalTracked,
            @JsonProperty("successfulDeliveries") long successfulDeliveries,
            @JsonProperty("failedDeliveries") long failedDeliveries,
            @JsonProperty("pendingCount") int pendingCount,
            @JsonProperty("timeoutCount") int timeoutCount,
            @JsonProperty("retryCount") int retryCount) {
        this.instanceId = instanceId;
        this.timestamp = timestamp;
        this.totalTracked = totalTracked;
        this.successfulDeliveries = successfulDeliveries;
        this.failedDeliveries = failedDeliveries;
        this.pendingCount = pendingCount;
        this.timeoutCount = timeoutCount;
        this.retryCount = retryCount;
    }
    
    public String getInstanceId() { return instanceId; }
    public long getTimestamp() { return timestamp; }
    public long getTotalTracked() { return totalTracked; }
    public long getSuccessfulDeliveries() { return successfulDeliveries; }
    public long getFailedDeliveries() { return failedDeliveries; }
    public int getPendingCount() { return pendingCount; }
    public int getTimeoutCount() { return timeoutCount; }
    public int getRetryCount() { return retryCount; }
}