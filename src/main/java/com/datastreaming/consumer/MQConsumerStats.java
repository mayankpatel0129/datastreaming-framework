package com.datastreaming.consumer;

public class MQConsumerStats {
    private final String queueName;
    private final String targetTopic;
    private final int totalWorkers;
    private final int activeWorkers;
    private final long messagesProcessed;
    private final long errorCount;
    private final boolean isRunning;

    public MQConsumerStats(String queueName, String targetTopic, int totalWorkers, 
                          int activeWorkers, long messagesProcessed, long errorCount, boolean isRunning) {
        this.queueName = queueName;
        this.targetTopic = targetTopic;
        this.totalWorkers = totalWorkers;
        this.activeWorkers = activeWorkers;
        this.messagesProcessed = messagesProcessed;
        this.errorCount = errorCount;
        this.isRunning = isRunning;
    }

    public MQConsumerStats() {
        this("", "", 0, 0, 0L, 0L, false);
    }

    public void addPoolStats(MQConsumerStats other) {
        // This would aggregate stats from multiple pools
    }

    // Getters
    public String getQueueName() { return queueName; }
    public String getTargetTopic() { return targetTopic; }
    public int getTotalWorkers() { return totalWorkers; }
    public int getActiveWorkers() { return activeWorkers; }
    public long getMessagesProcessed() { return messagesProcessed; }
    public long getErrorCount() { return errorCount; }
    public boolean isRunning() { return isRunning; }
}