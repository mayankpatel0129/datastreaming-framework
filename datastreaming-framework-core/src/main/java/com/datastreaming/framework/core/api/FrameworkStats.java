package com.datastreaming.framework.core.api;

import java.time.Instant;
import java.util.Map;

/**
 * Framework statistics and metrics
 */
public class FrameworkStats {
    private final long totalMessagesProcessed;
    private final long totalErrors;
    private final long pendingMessages;
    private final int activeWorkers;
    private final int totalWorkers;
    private final Instant startTime;
    private final Map<String, Object> additionalMetrics;

    public FrameworkStats(long totalMessagesProcessed, 
                         long totalErrors, 
                         long pendingMessages,
                         int activeWorkers, 
                         int totalWorkers, 
                         Instant startTime,
                         Map<String, Object> additionalMetrics) {
        this.totalMessagesProcessed = totalMessagesProcessed;
        this.totalErrors = totalErrors;
        this.pendingMessages = pendingMessages;
        this.activeWorkers = activeWorkers;
        this.totalWorkers = totalWorkers;
        this.startTime = startTime;
        this.additionalMetrics = additionalMetrics;
    }

    public long getTotalMessagesProcessed() { return totalMessagesProcessed; }
    public long getTotalErrors() { return totalErrors; }
    public long getPendingMessages() { return pendingMessages; }
    public int getActiveWorkers() { return activeWorkers; }
    public int getTotalWorkers() { return totalWorkers; }
    public Instant getStartTime() { return startTime; }
    public Map<String, Object> getAdditionalMetrics() { return additionalMetrics; }

    public double getErrorRate() {
        return totalMessagesProcessed > 0 ? (double) totalErrors / totalMessagesProcessed : 0.0;
    }

    public double getWorkerHealthRatio() {
        return totalWorkers > 0 ? (double) activeWorkers / totalWorkers : 0.0;
    }
}