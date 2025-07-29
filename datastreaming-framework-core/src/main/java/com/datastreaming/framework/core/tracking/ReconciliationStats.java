package com.datastreaming.framework.core.tracking;

/**
 * Statistics for reconciliation process
 */
public class ReconciliationStats {
    
    private final long totalTracked;
    private final int pending;
    private final int delivered;
    private final int failed;
    private final long successfulDeliveries;
    private final long failedDeliveries;
    
    public ReconciliationStats(long totalTracked, int pending, int delivered, int failed, 
                             long successfulDeliveries, long failedDeliveries) {
        this.totalTracked = totalTracked;
        this.pending = pending;
        this.delivered = delivered;
        this.failed = failed;
        this.successfulDeliveries = successfulDeliveries;
        this.failedDeliveries = failedDeliveries;
    }
    
    // Getters
    public long getTotalTracked() { return totalTracked; }
    public int getPending() { return pending; }
    public int getDelivered() { return delivered; }
    public int getFailed() { return failed; }
    public long getSuccessfulDeliveries() { return successfulDeliveries; }
    public long getFailedDeliveries() { return failedDeliveries; }
    
    public double getSuccessRate() {
        return totalTracked > 0 ? (double) successfulDeliveries / totalTracked : 0.0;
    }
    
    public double getFailureRate() {
        return totalTracked > 0 ? (double) failedDeliveries / totalTracked : 0.0;
    }
}