package com.datastreaming.reconciliation;

import com.datastreaming.config.ProcessorProfile;
import com.datastreaming.config.ProfileManager;
import com.datastreaming.monitoring.MetricsCollector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class MessageTracker {

    private static final Logger logger = LoggerFactory.getLogger(MessageTracker.class);

    @Autowired
    private ProfileManager profileManager;

    @Autowired
    private MetricsCollector metricsCollector;

    private final ConcurrentHashMap<String, MessageStatus> messageStatusMap = new ConcurrentHashMap<>();
    private final AtomicLong totalTrackedMessages = new AtomicLong(0);
    private final AtomicLong successfulDeliveries = new AtomicLong(0);
    private final AtomicLong failedDeliveries = new AtomicLong(0);

    private ScheduledExecutorService reconciliationExecutor;
    private volatile boolean isRunning = false;

    @PostConstruct
    public void initialize() {
        logger.info("Initializing Message Tracker");
        
        reconciliationExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "message-reconciliation");
            t.setDaemon(true);
            return t;
        });

        ProcessorProfile profile = profileManager.getActiveProfile();
        long intervalMs = profile.getReconciliationConfiguration().getReconciliationIntervalMs();

        reconciliationExecutor.scheduleAtFixedRate(
            this::performReconciliation, 
            intervalMs, 
            intervalMs, 
            TimeUnit.MILLISECONDS
        );

        // Cleanup old entries periodically
        reconciliationExecutor.scheduleAtFixedRate(
            this::cleanupOldEntries, 
            300000L, // 5 minutes
            300000L, 
            TimeUnit.MILLISECONDS
        );

        isRunning = true;
        logger.info("Message Tracker initialized with reconciliation interval: {}ms", intervalMs);
    }

    public void trackMessage(String correlationId, String sourceQueue, String targetTopic) {
        MessageStatus status = new MessageStatus(correlationId, sourceQueue, targetTopic);
        messageStatusMap.put(correlationId, status);
        totalTrackedMessages.incrementAndGet();
        
        logger.debug("Tracking message: {} from queue: {} to topic: {}", 
                    correlationId, sourceQueue, targetTopic);
    }

    public void recordSuccess(String correlationId, RecordMetadata metadata) {
        MessageStatus status = messageStatusMap.get(correlationId);
        if (status != null) {
            status.markDelivered(metadata);
            successfulDeliveries.incrementAndGet();
            
            logger.debug("Message delivered successfully: {} to partition: {}, offset: {}", 
                        correlationId, metadata.partition(), metadata.offset());
        } else {
            logger.warn("Received success notification for untracked message: {}", correlationId);
        }
    }

    public void recordFailure(String correlationId, String errorMessage) {
        MessageStatus status = messageStatusMap.get(correlationId);
        if (status != null) {
            status.markFailed(errorMessage);
            failedDeliveries.incrementAndGet();
            
            logger.error("Message delivery failed: {} with error: {}", correlationId, errorMessage);
        } else {
            logger.warn("Received failure notification for untracked message: {}", correlationId);
        }
    }

    @Scheduled(fixedRateString = "#{@profileManager.getActiveProfile().getReconciliationConfiguration().getReconciliationIntervalMs()}")
    public void performReconciliation() {
        if (!isRunning) {
            return;
        }

        logger.debug("Starting reconciliation process");
        
        ProcessorProfile.ReconciliationConfiguration config = 
            profileManager.getActiveProfile().getReconciliationConfiguration();
        
        long currentTime = System.currentTimeMillis();
        long timeoutThreshold = currentTime - (config.getRetryBackoffMs() * config.getMaxRetryAttempts());
        
        int pendingCount = 0;
        int timeoutCount = 0;
        int retryCount = 0;

        for (MessageStatus status : messageStatusMap.values()) {
            if (status.getDeliveryStatus() == DeliveryStatus.PENDING) {
                pendingCount++;
                
                if (status.getCreatedAt() < timeoutThreshold) {
                    if (status.getRetryCount() < config.getMaxRetryAttempts()) {
                        // Schedule retry
                        scheduleRetry(status, config);
                        retryCount++;
                    } else {
                        // Send to dead letter queue if enabled
                        if (config.isEnableDeadLetterQueue()) {
                            sendToDeadLetterQueue(status, config.getDeadLetterTopic());
                        }
                        status.markFailed("Max retry attempts exceeded");
                        timeoutCount++;
                    }
                }
            }
        }

        logger.info("Reconciliation completed. Pending: {}, Timeouts: {}, Retries: {}", 
                   pendingCount, timeoutCount, retryCount);

        // Update metrics
        metricsCollector.recordReconciliationStats(pendingCount, timeoutCount, retryCount);
    }

    private void scheduleRetry(MessageStatus status, ProcessorProfile.ReconciliationConfiguration config) {
        status.incrementRetryCount();
        
        reconciliationExecutor.schedule(() -> {
            try {
                // Trigger retry mechanism
                logger.info("Retrying message delivery for correlationId: {}, attempt: {}", 
                           status.getCorrelationId(), status.getRetryCount());
                
                // This would typically involve re-queuing the message or triggering a retry
                // Implementation depends on the specific retry strategy
                
            } catch (Exception e) {
                logger.error("Error during retry for correlationId: {}", status.getCorrelationId(), e);
            }
        }, config.getRetryBackoffMs(), TimeUnit.MILLISECONDS);
    }

    private void sendToDeadLetterQueue(MessageStatus status, String deadLetterTopic) {
        try {
            // Send message to dead letter queue
            logger.warn("Sending message to dead letter queue: {} for correlationId: {}", 
                       deadLetterTopic, status.getCorrelationId());
            
            // Implementation would send the failed message to DLQ
            // This is a placeholder for the actual DLQ implementation
            
        } catch (Exception e) {
            logger.error("Failed to send message to dead letter queue for correlationId: {}", 
                        status.getCorrelationId(), e);
        }
    }

    private void cleanupOldEntries() {
        long cutoffTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24); // Keep for 24 hours
        
        int removed = 0;
        for (String correlationId : messageStatusMap.keySet()) {
            MessageStatus status = messageStatusMap.get(correlationId);
            if (status != null && 
                (status.getDeliveryStatus() == DeliveryStatus.DELIVERED || 
                 status.getDeliveryStatus() == DeliveryStatus.FAILED) &&
                status.getCreatedAt() < cutoffTime) {
                
                messageStatusMap.remove(correlationId);
                removed++;
            }
        }
        
        if (removed > 0) {
            logger.info("Cleaned up {} old message tracking entries", removed);
        }
    }

    public ReconciliationStats getStats() {
        int pending = 0;
        int delivered = 0;
        int failed = 0;
        
        for (MessageStatus status : messageStatusMap.values()) {
            switch (status.getDeliveryStatus()) {
                case PENDING -> pending++;
                case DELIVERED -> delivered++;
                case FAILED -> failed++;
            }
        }
        
        return new ReconciliationStats(
            totalTrackedMessages.get(),
            pending,
            delivered,
            failed,
            successfulDeliveries.get(),
            failedDeliveries.get()
        );
    }

    public boolean isHealthy() {
        ReconciliationStats stats = getStats();
        double failureRate = stats.getTotalTracked() > 0 ? 
            (double) stats.getFailed() / stats.getTotalTracked() : 0.0;
        
        return isRunning && failureRate < 0.05; // Less than 5% failure rate
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Message Tracker");
        isRunning = false;
        
        if (reconciliationExecutor != null && !reconciliationExecutor.isShutdown()) {
            reconciliationExecutor.shutdown();
            try {
                if (!reconciliationExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    reconciliationExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                reconciliationExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        logger.info("Message Tracker shutdown completed");
    }

    public static class MessageStatus {
        private final String correlationId;
        private final String sourceQueue;
        private final String targetTopic;
        private final long createdAt;
        private volatile DeliveryStatus deliveryStatus;
        private volatile RecordMetadata recordMetadata;
        private volatile String errorMessage;
        private volatile int retryCount;
        private volatile long lastRetryAt;

        public MessageStatus(String correlationId, String sourceQueue, String targetTopic) {
            this.correlationId = correlationId;
            this.sourceQueue = sourceQueue;
            this.targetTopic = targetTopic;
            this.createdAt = System.currentTimeMillis();
            this.deliveryStatus = DeliveryStatus.PENDING;
            this.retryCount = 0;
        }

        public void markDelivered(RecordMetadata metadata) {
            this.deliveryStatus = DeliveryStatus.DELIVERED;
            this.recordMetadata = metadata;
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
        public String getSourceQueue() { return sourceQueue; }
        public String getTargetTopic() { return targetTopic; }
        public long getCreatedAt() { return createdAt; }
        public DeliveryStatus getDeliveryStatus() { return deliveryStatus; }
        public RecordMetadata getRecordMetadata() { return recordMetadata; }
        public String getErrorMessage() { return errorMessage; }
        public int getRetryCount() { return retryCount; }
        public long getLastRetryAt() { return lastRetryAt; }
    }

    public enum DeliveryStatus {
        PENDING, DELIVERED, FAILED
    }

    public static class ReconciliationStats {
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
    }
}