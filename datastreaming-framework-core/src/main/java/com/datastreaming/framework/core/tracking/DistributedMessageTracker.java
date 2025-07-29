package com.datastreaming.framework.core.tracking;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed message tracker using Redis for cross-instance reconciliation
 */
@Component
@ConditionalOnProperty(name = "datastreaming.reconciliation.mode", havingValue = "distributed")
public class DistributedMessageTracker implements MessageTracker {

    private static final Logger logger = LoggerFactory.getLogger(DistributedMessageTracker.class);
    
    private static final String TRACKING_KEY_PREFIX = "streaming:tracking:";
    private static final String INSTANCE_KEY_PREFIX = "streaming:instance:";
    private static final String METRICS_KEY_PREFIX = "streaming:metrics:";
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Value("${datastreaming.instance.id:${HOSTNAME:unknown}}")
    private String instanceId;
    
    @Value("${datastreaming.reconciliation.ttlHours:24}")
    private int ttlHours;
    
    @Value("${datastreaming.reconciliation.reconciliationIntervalMs:30000}")
    private long reconciliationIntervalMs;
    
    @Value("${datastreaming.reconciliation.maxRetryAttempts:3}")
    private int maxRetryAttempts;
    
    @Value("${datastreaming.reconciliation.retryBackoffMs:5000}")
    private long retryBackoffMs;
    
    // Local metrics for performance
    private final AtomicLong localTrackedMessages = new AtomicLong(0);
    private final AtomicLong localSuccessfulDeliveries = new AtomicLong(0);
    private final AtomicLong localFailedDeliveries = new AtomicLong(0);
    
    // Cache for recent operations to reduce Redis calls
    private final Map<String, MessageStatus> recentStatusCache = new ConcurrentHashMap<>();
    
    private volatile boolean isRunning = false;
    
    @PostConstruct
    public void initialize() {
        logger.info("Initializing Distributed Message Tracker for instance: {}", instanceId);
        
        try {
            // Register this instance in Redis
            registerInstance();
            
            isRunning = true;
            logger.info("Distributed Message Tracker initialized with Redis backend");
            
        } catch (Exception e) {
            logger.error("Failed to initialize Distributed Message Tracker", e);
            throw new RuntimeException("Redis initialization failed", e);
        }
    }
    
    private void registerInstance() {
        try {
            InstanceInfo instanceInfo = new InstanceInfo(instanceId, Instant.now().toEpochMilli(), "ACTIVE");
            String instanceKey = INSTANCE_KEY_PREFIX + instanceId;
            String instanceJson = objectMapper.writeValueAsString(instanceInfo);
            
            redisTemplate.opsForValue().set(instanceKey, instanceJson, Duration.ofMinutes(5));
            logger.info("Registered instance {} in distributed tracking system", instanceId);
            
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize instance info", e);
        }
    }
    
    @Override
    public void trackMessage(String correlationId, String sourceQueue, String targetTopic) {
        try {
            MessageStatus status = new MessageStatus(
                correlationId, instanceId, sourceQueue, targetTopic, 
                System.currentTimeMillis(), DeliveryStatus.PENDING
            );
            
            String key = TRACKING_KEY_PREFIX + correlationId;
            String statusJson = objectMapper.writeValueAsString(status);
            
            // Store in Redis with TTL
            redisTemplate.opsForValue().set(key, statusJson, Duration.ofHours(ttlHours));
            
            // Cache locally for quick access
            recentStatusCache.put(correlationId, status);
            localTrackedMessages.incrementAndGet();
            
            logger.debug("Tracking message: {} from queue: {} to topic: {} on instance: {}", 
                        correlationId, sourceQueue, targetTopic, instanceId);
                        
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize message status for tracking", e);
        }
    }
    
    @Override
    public void recordSuccess(String correlationId, RecordMetadata metadata) {
        try {
            String key = TRACKING_KEY_PREFIX + correlationId;
            String statusJson = redisTemplate.opsForValue().get(key);
            
            if (statusJson != null) {
                MessageStatus status = objectMapper.readValue(statusJson, MessageStatus.class);
                status.markDelivered(metadata);
                
                // Update in Redis
                String updatedJson = objectMapper.writeValueAsString(status);
                redisTemplate.opsForValue().set(key, updatedJson, Duration.ofHours(ttlHours));
                
                // Update local cache
                recentStatusCache.put(correlationId, status);
                localSuccessfulDeliveries.incrementAndGet();
                
                logger.debug("Message delivered successfully: {} to partition: {}, offset: {} by instance: {}", 
                            correlationId, metadata.partition(), metadata.offset(), instanceId);
                            
            } else {
                logger.warn("Received success notification for untracked message: {}", correlationId);
            }
            
        } catch (Exception e) {
            logger.error("Failed to record success for message: {}", correlationId, e);
        }
    }
    
    @Override
    public void recordFailure(String correlationId, String errorMessage) {
        try {
            String key = TRACKING_KEY_PREFIX + correlationId;
            String statusJson = redisTemplate.opsForValue().get(key);
            
            if (statusJson != null) {
                MessageStatus status = objectMapper.readValue(statusJson, MessageStatus.class);
                status.markFailed(errorMessage);
                
                // Update in Redis
                String updatedJson = objectMapper.writeValueAsString(status);
                redisTemplate.opsForValue().set(key, updatedJson, Duration.ofHours(ttlHours));
                
                // Update local cache
                recentStatusCache.put(correlationId, status);
                localFailedDeliveries.incrementAndGet();
                
                logger.error("Message delivery failed: {} with error: {} on instance: {}", 
                           correlationId, errorMessage, instanceId);
                           
            } else {
                logger.warn("Received failure notification for untracked message: {}", correlationId);
            }
            
        } catch (Exception e) {
            logger.error("Failed to record failure for message: {}", correlationId, e);
        }
    }
    
    @Scheduled(fixedRateString = "#{${datastreaming.reconciliation.reconciliationIntervalMs:30000}}")
    public void performReconciliation() {
        if (!isRunning) {
            return;
        }
        
        logger.debug("Starting distributed reconciliation process on instance: {}", instanceId);
        
        try {
            // Get all tracking keys
            Set<String> trackingKeys = redisTemplate.keys(TRACKING_KEY_PREFIX + "*");
            if (trackingKeys == null) {
                return;
            }
            
            long currentTime = System.currentTimeMillis();
            long timeoutThreshold = currentTime - (retryBackoffMs * maxRetryAttempts);
            
            int pendingCount = 0;
            int timeoutCount = 0;
            int retryCount = 0;
            
            for (String key : trackingKeys) {
                try {
                    String statusJson = redisTemplate.opsForValue().get(key);
                    if (statusJson == null) continue;
                    
                    MessageStatus status = objectMapper.readValue(statusJson, MessageStatus.class);
                    
                    if (status.getDeliveryStatus() == DeliveryStatus.PENDING) {
                        pendingCount++;
                        
                        if (status.getCreatedAt() < timeoutThreshold) {
                            if (status.getRetryCount() < maxRetryAttempts) {
                                // Schedule retry
                                scheduleRetry(status);
                                retryCount++;
                            } else {
                                // Mark as failed after max retries
                                status.markFailed("Max retry attempts exceeded");
                                String updatedJson = objectMapper.writeValueAsString(status);
                                redisTemplate.opsForValue().set(key, updatedJson, Duration.ofHours(ttlHours));
                                timeoutCount++;
                            }
                        }
                    }
                    
                } catch (Exception e) {
                    logger.error("Error processing tracking key: {}", key, e);
                }
            }
            
            logger.info("Distributed reconciliation completed on instance: {}. Pending: {}, Timeouts: {}, Retries: {}", 
                       instanceId, pendingCount, timeoutCount, retryCount);
            
            // Update instance metrics
            updateInstanceMetrics(pendingCount, timeoutCount, retryCount);
            
        } catch (Exception e) {
            logger.error("Error during distributed reconciliation", e);
        }
    }
    
    private void scheduleRetry(MessageStatus status) {
        try {
            status.incrementRetryCount();
            
            String key = TRACKING_KEY_PREFIX + status.getCorrelationId();
            String updatedJson = objectMapper.writeValueAsString(status);
            redisTemplate.opsForValue().set(key, updatedJson, Duration.ofHours(ttlHours));
            
            logger.info("Scheduled retry for message: {}, attempt: {}, instance: {}", 
                       status.getCorrelationId(), status.getRetryCount(), instanceId);
                       
        } catch (JsonProcessingException e) {
            logger.error("Failed to schedule retry for message: {}", status.getCorrelationId(), e);
        }
    }
    
    private void updateInstanceMetrics(int pendingCount, int timeoutCount, int retryCount) {
        try {
            InstanceMetrics metrics = new InstanceMetrics(
                instanceId,
                System.currentTimeMillis(),
                localTrackedMessages.get(),
                localSuccessfulDeliveries.get(),
                localFailedDeliveries.get(),
                pendingCount,
                timeoutCount,
                retryCount
            );
            
            String metricsKey = METRICS_KEY_PREFIX + instanceId;
            String metricsJson = objectMapper.writeValueAsString(metrics);
            
            redisTemplate.opsForValue().set(metricsKey, metricsJson, Duration.ofMinutes(5));
            
        } catch (JsonProcessingException e) {
            logger.error("Failed to update instance metrics", e);
        }
    }
    
    @Override
    public ReconciliationStats getStats() {
        try {
            // Get aggregate stats from all instances
            Set<String> metricsKeys = redisTemplate.keys(METRICS_KEY_PREFIX + "*");
            if (metricsKeys == null) {
                return new ReconciliationStats(0, 0, 0, 0, 0, 0);
            }
            
            long totalTracked = 0;
            long totalSuccessful = 0;
            long totalFailed = 0;
            int totalPending = 0;
            int totalTimeouts = 0;
            int totalRetries = 0;
            
            for (String key : metricsKeys) {
                try {
                    String metricsJson = redisTemplate.opsForValue().get(key);
                    if (metricsJson != null) {
                        InstanceMetrics metrics = objectMapper.readValue(metricsJson, InstanceMetrics.class);
                        totalTracked += metrics.getTotalTracked();
                        totalSuccessful += metrics.getSuccessfulDeliveries();
                        totalFailed += metrics.getFailedDeliveries();
                        totalPending += metrics.getPendingCount();
                        totalTimeouts += metrics.getTimeoutCount();
                        totalRetries += metrics.getRetryCount();
                    }
                } catch (Exception e) {
                    logger.error("Error reading metrics from key: {}", key, e);
                }
            }
            
            return new ReconciliationStats(
                totalTracked, totalPending, (int) totalSuccessful, 
                (int) totalFailed, totalSuccessful, totalFailed
            );
            
        } catch (Exception e) {
            logger.error("Error getting aggregate stats", e);
            return new ReconciliationStats(0, 0, 0, 0, 0, 0);
        }
    }
    
    @Override
    public boolean isHealthy() {
        try {
            // Test Redis connectivity
            redisTemplate.opsForValue().get("health-check");
            
            ReconciliationStats stats = getStats();
            double failureRate = stats.getTotalTracked() > 0 ? 
                (double) stats.getFailed() / stats.getTotalTracked() : 0.0;
            
            return isRunning && failureRate < 0.05; // Less than 5% failure rate
            
        } catch (Exception e) {
            logger.error("Health check failed", e);
            return false;
        }
    }
    
    public List<String> getActiveInstances() {
        try {
            Set<String> instanceKeys = redisTemplate.keys(INSTANCE_KEY_PREFIX + "*");
            List<String> activeInstances = new ArrayList<>();
            
            if (instanceKeys != null) {
                for (String key : instanceKeys) {
                    try {
                        String instanceJson = redisTemplate.opsForValue().get(key);
                        if (instanceJson != null) {
                            InstanceInfo info = objectMapper.readValue(instanceJson, InstanceInfo.class);
                            activeInstances.add(info.getInstanceId());
                        }
                    } catch (Exception e) {
                        logger.error("Error reading instance info from key: {}", key, e);
                    }
                }
            }
            
            return activeInstances;
            
        } catch (Exception e) {
            logger.error("Error getting active instances", e);
            return Collections.emptyList();
        }
    }
    
    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void heartbeat() {
        try {
            registerInstance(); // Refresh instance registration
            
            // Clean up local cache
            long cutoffTime = System.currentTimeMillis() - Duration.ofHours(1).toMillis();
            recentStatusCache.entrySet().removeIf(entry -> 
                entry.getValue().getCreatedAt() < cutoffTime);
                
        } catch (Exception e) {
            logger.error("Error during heartbeat", e);
        }
    }
    
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Distributed Message Tracker on instance: {}", instanceId);
        isRunning = false;
        
        try {
            // Remove instance registration
            String instanceKey = INSTANCE_KEY_PREFIX + instanceId;
            redisTemplate.delete(instanceKey);
            
            logger.info("Distributed Message Tracker shutdown completed for instance: {}", instanceId);
            
        } catch (Exception e) {
            logger.error("Error during shutdown", e);
        }
    }
}