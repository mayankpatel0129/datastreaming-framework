package com.datastreaming.framework.core.metrics;

import com.datastreaming.framework.core.tracking.DistributedMessageTracker;
import com.datastreaming.framework.core.tracking.InstanceMetrics;
import com.datastreaming.framework.core.tracking.ReconciliationStats;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and aggregates metrics across all streaming framework instances
 */
@Component
@ConditionalOnProperty(name = "datastreaming.monitoring.metricsEnabled", havingValue = "true", matchIfMissing = true)
public class AggregateMetricsCollector {

    private static final Logger logger = LoggerFactory.getLogger(AggregateMetricsCollector.class);
    
    private static final String METRICS_KEY_PREFIX = "streaming:metrics:";
    
    @Autowired
    private MeterRegistry meterRegistry;
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired(required = false)
    private DistributedMessageTracker distributedTracker;
    
    @Value("${datastreaming.monitoring.metricsPrefix:datastreaming}")
    private String metricsPrefix;
    
    // Aggregate metrics
    private final AtomicLong totalMessagesAcrossInstances = new AtomicLong(0);
    private final AtomicLong totalSuccessfulAcrossInstances = new AtomicLong(0);
    private final AtomicLong totalFailedAcrossInstances = new AtomicLong(0);
    private final AtomicLong totalPendingAcrossInstances = new AtomicLong(0);
    private final AtomicLong activeInstanceCount = new AtomicLong(0);
    
    // Micrometer metrics
    private Counter aggregateMessagesProcessed;
    private Counter aggregateSuccessfulDeliveries;
    private Counter aggregateFailedDeliveries;
    private Timer aggregateProcessingTime;
    
    @PostConstruct
    public void initialize() {
        logger.info("Initializing Aggregate Metrics Collector");
        
        // Initialize Micrometer metrics
        aggregateMessagesProcessed = Counter.builder(metricsPrefix + ".aggregate.messages.processed.total")
                .description("Total messages processed across all instances")
                .register(meterRegistry);
                
        aggregateSuccessfulDeliveries = Counter.builder(metricsPrefix + ".aggregate.messages.successful.total")
                .description("Total successful message deliveries across all instances")
                .register(meterRegistry);
                
        aggregateFailedDeliveries = Counter.builder(metricsPrefix + ".aggregate.messages.failed.total")
                .description("Total failed message deliveries across all instances")
                .register(meterRegistry);
                
        aggregateProcessingTime = Timer.builder(metricsPrefix + ".aggregate.processing.time")
                .description("Aggregate message processing time across instances")
                .register(meterRegistry);
        
        // Register gauges for real-time metrics
        Gauge.builder(metricsPrefix + ".aggregate.instances.active", this, AggregateMetricsCollector::getActiveInstancesCount)
                .description("Number of active streaming instances")
                .register(meterRegistry);
                
        Gauge.builder(metricsPrefix + ".aggregate.messages.pending", this, AggregateMetricsCollector::getTotalPendingMessages)
                .description("Total pending messages across all instances")
                .register(meterRegistry);
                
        Gauge.builder(metricsPrefix + ".aggregate.success.rate", this, AggregateMetricsCollector::getOverallSuccessRate)
                .description("Overall success rate across all instances")
                .register(meterRegistry);
                
        Gauge.builder(metricsPrefix + ".aggregate.failure.rate", this, AggregateMetricsCollector::getOverallFailureRate)
                .description("Overall failure rate across all instances")
                .register(meterRegistry);
        
        logger.info("Aggregate Metrics Collector initialized");
    }
    
    @Scheduled(fixedRateString = "#{${datastreaming.monitoring.metricsIntervalMs:30000}}")
    public void collectAggregateMetrics() {
        try {
            logger.debug("Collecting aggregate metrics across all instances");
            
            if (distributedTracker != null) {
                collectDistributedMetrics();
            } else {
                logger.debug("Distributed tracker not available, collecting Redis metrics directly");
                collectRedisMetrics();
            }
            
        } catch (Exception e) {
            logger.error("Error collecting aggregate metrics", e);
        }
    }
    
    private void collectDistributedMetrics() {
        try {
            // Get overall statistics from distributed tracker
            ReconciliationStats stats = distributedTracker.getStats();
            List<String> activeInstances = distributedTracker.getActiveInstances();
            
            // Update aggregate counters
            totalMessagesAcrossInstances.set(stats.getTotalTracked());
            totalSuccessfulAcrossInstances.set(stats.getSuccessfulDeliveries());
            totalFailedAcrossInstances.set(stats.getFailedDeliveries());
            totalPendingAcrossInstances.set(stats.getPending());
            activeInstanceCount.set(activeInstances.size());
            
            logger.debug("Collected distributed metrics: instances={}, total={}, successful={}, failed={}, pending={}", 
                        activeInstances.size(), stats.getTotalTracked(), 
                        stats.getSuccessfulDeliveries(), stats.getFailedDeliveries(), stats.getPending());
                        
        } catch (Exception e) {
            logger.error("Error collecting distributed metrics", e);
        }
    }
    
    private void collectRedisMetrics() {
        try {
            Set<String> metricsKeys = redisTemplate.keys(METRICS_KEY_PREFIX + "*");
            if (metricsKeys == null) {
                return;
            }
            
            long totalTracked = 0;
            long totalSuccessful = 0;
            long totalFailed = 0;
            long totalPending = 0;
            int activeInstances = 0;
            
            for (String key : metricsKeys) {
                try {
                    String metricsJson = redisTemplate.opsForValue().get(key);
                    if (metricsJson != null) {
                        InstanceMetrics metrics = objectMapper.readValue(metricsJson, InstanceMetrics.class);
                        
                        totalTracked += metrics.getTotalTracked();
                        totalSuccessful += metrics.getSuccessfulDeliveries();
                        totalFailed += metrics.getFailedDeliveries();
                        totalPending += metrics.getPendingCount();
                        activeInstances++;
                    }
                } catch (Exception e) {
                    logger.error("Error reading metrics from key: {}", key, e);
                }
            }
            
            // Update aggregate metrics
            totalMessagesAcrossInstances.set(totalTracked);
            totalSuccessfulAcrossInstances.set(totalSuccessful);
            totalFailedAcrossInstances.set(totalFailed);
            totalPendingAcrossInstances.set(totalPending);
            activeInstanceCount.set(activeInstances);
            
            logger.debug("Collected Redis metrics: instances={}, total={}, successful={}, failed={}, pending={}", 
                        activeInstances, totalTracked, totalSuccessful, totalFailed, totalPending);
                        
        } catch (Exception e) {
            logger.error("Error collecting Redis metrics", e);
        }
    }
    
    public Map<String, Object> getAggregateMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        metrics.put("activeInstances", activeInstanceCount.get());
        metrics.put("totalMessages", totalMessagesAcrossInstances.get());
        metrics.put("successfulDeliveries", totalSuccessfulAcrossInstances.get());
        metrics.put("failedDeliveries", totalFailedAcrossInstances.get());
        metrics.put("pendingMessages", totalPendingAcrossInstances.get());
        metrics.put("successRate", getOverallSuccessRate());
        metrics.put("failureRate", getOverallFailureRate());
        metrics.put("timestamp", System.currentTimeMillis());
        
        return metrics;
    }
    
    public Map<String, InstanceMetrics> getPerInstanceMetrics() {
        Map<String, InstanceMetrics> instanceMetrics = new HashMap<>();
        
        try {
            Set<String> metricsKeys = redisTemplate.keys(METRICS_KEY_PREFIX + "*");
            if (metricsKeys != null) {
                for (String key : metricsKeys) {
                    try {
                        String metricsJson = redisTemplate.opsForValue().get(key);
                        if (metricsJson != null) {
                            InstanceMetrics metrics = objectMapper.readValue(metricsJson, InstanceMetrics.class);
                            instanceMetrics.put(metrics.getInstanceId(), metrics);
                        }
                    } catch (Exception e) {
                        logger.error("Error reading instance metrics from key: {}", key, e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error getting per-instance metrics", e);
        }
        
        return instanceMetrics;
    }
    
    // Gauge methods for Micrometer
    public double getActiveInstancesCount() {
        return activeInstanceCount.get();
    }
    
    public double getTotalPendingMessages() {
        return totalPendingAcrossInstances.get();
    }
    
    public double getOverallSuccessRate() {
        long total = totalMessagesAcrossInstances.get();
        return total > 0 ? (double) totalSuccessfulAcrossInstances.get() / total : 0.0;
    }
    
    public double getOverallFailureRate() {
        long total = totalMessagesAcrossInstances.get();
        return total > 0 ? (double) totalFailedAcrossInstances.get() / total : 0.0;
    }
    
    public boolean isClusterHealthy() {
        // Cluster is healthy if:
        // 1. At least one instance is active
        // 2. Failure rate is below threshold
        // 3. No instances are stuck (all have recent metrics)
        
        if (activeInstanceCount.get() == 0) {
            return false;
        }
        
        double failureRate = getOverallFailureRate();
        if (failureRate > 0.05) { // More than 5% failure rate
            return false;
        }
        
        // Check for stuck instances (metrics older than 5 minutes)
        long currentTime = System.currentTimeMillis();
        long staleThreshold = currentTime - (5 * 60 * 1000); // 5 minutes
        
        try {
            Map<String, InstanceMetrics> instanceMetrics = getPerInstanceMetrics();
            for (InstanceMetrics metrics : instanceMetrics.values()) {
                if (metrics.getTimestamp() < staleThreshold) {
                    logger.warn("Instance {} has stale metrics (last update: {})", 
                               metrics.getInstanceId(), metrics.getTimestamp());
                    return false;
                }
            }
        } catch (Exception e) {
            logger.error("Error checking instance health", e);
            return false;
        }
        
        return true;
    }
    
    /**
     * Generate Prometheus-format metrics for scraping
     */
    public String getPrometheusMetrics() {
        StringBuilder metrics = new StringBuilder();
        Map<String, Object> aggregateMetrics = getAggregateMetrics();
        
        // Aggregate metrics
        metrics.append("# HELP ").append(metricsPrefix).append("_cluster_active_instances Total active streaming instances\n");
        metrics.append("# TYPE ").append(metricsPrefix).append("_cluster_active_instances gauge\n");
        metrics.append(metricsPrefix).append("_cluster_active_instances ").append(aggregateMetrics.get("activeInstances")).append("\n");
        
        metrics.append("# HELP ").append(metricsPrefix).append("_cluster_total_messages Total messages processed across cluster\n");
        metrics.append("# TYPE ").append(metricsPrefix).append("_cluster_total_messages counter\n");
        metrics.append(metricsPrefix).append("_cluster_total_messages ").append(aggregateMetrics.get("totalMessages")).append("\n");
        
        metrics.append("# HELP ").append(metricsPrefix).append("_cluster_success_rate Success rate across cluster\n");
        metrics.append("# TYPE ").append(metricsPrefix).append("_cluster_success_rate gauge\n");
        metrics.append(metricsPrefix).append("_cluster_success_rate ").append(aggregateMetrics.get("successRate")).append("\n");
        
        // Per-instance metrics
        Map<String, InstanceMetrics> instanceMetrics = getPerInstanceMetrics();
        for (Map.Entry<String, InstanceMetrics> entry : instanceMetrics.entrySet()) {
            String instanceId = entry.getKey();
            InstanceMetrics instanceMetric = entry.getValue();
            
            metrics.append("# HELP ").append(metricsPrefix).append("_instance_messages_total Messages processed by instance\n");
            metrics.append("# TYPE ").append(metricsPrefix).append("_instance_messages_total counter\n");
            metrics.append(metricsPrefix).append("_instance_messages_total{instance=\"").append(instanceId).append("\"} ")
                   .append(instanceMetric.getTotalTracked()).append("\n");
                   
            metrics.append("# HELP ").append(metricsPrefix).append("_instance_pending_messages Pending messages on instance\n");
            metrics.append("# TYPE ").append(metricsPrefix).append("_instance_pending_messages gauge\n");
            metrics.append(metricsPrefix).append("_instance_pending_messages{instance=\"").append(instanceId).append("\"} ")
                   .append(instanceMetric.getPendingCount()).append("\n");
        }
        
        return metrics.toString();
    }
}