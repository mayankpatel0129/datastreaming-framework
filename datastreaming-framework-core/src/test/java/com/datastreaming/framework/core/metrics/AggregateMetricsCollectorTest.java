package com.datastreaming.framework.core.metrics;

import com.datastreaming.framework.core.tracking.DistributedMessageTracker;
import com.datastreaming.framework.core.tracking.InstanceMetrics;
import com.datastreaming.framework.core.tracking.ReconciliationStats;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AggregateMetricsCollectorTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;
    
    @Mock
    private ValueOperations<String, String> valueOperations;
    
    @Mock
    private DistributedMessageTracker distributedTracker;
    
    private AggregateMetricsCollector collector;
    private MeterRegistry meterRegistry;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        collector = new AggregateMetricsCollector();
        meterRegistry = new SimpleMeterRegistry();
        objectMapper = new ObjectMapper();
        
        // Set up mocks
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        
        // Inject dependencies
        ReflectionTestUtils.setField(collector, "meterRegistry", meterRegistry);
        ReflectionTestUtils.setField(collector, "redisTemplate", redisTemplate);
        ReflectionTestUtils.setField(collector, "objectMapper", objectMapper);
        ReflectionTestUtils.setField(collector, "distributedTracker", distributedTracker);
        ReflectionTestUtils.setField(collector, "metricsPrefix", "datastreaming");
        
        // Initialize the collector
        collector.initialize();
    }
    
    @Test
    void shouldInitializeMetricsSuccessfully() {
        // When initialized (in setUp)
        
        // Then
        assertNotNull(meterRegistry.find("datastreaming.aggregate.messages.processed.total").counter());
        assertNotNull(meterRegistry.find("datastreaming.aggregate.messages.successful.total").counter());
        assertNotNull(meterRegistry.find("datastreaming.aggregate.messages.failed.total").counter());
        assertNotNull(meterRegistry.find("datastreaming.aggregate.processing.time").timer());
    }
    
    @Test
    void shouldCollectDistributedMetricsWhenTrackerAvailable() {
        // Given
        ReconciliationStats stats = new ReconciliationStats(1000, 50, 900, 50, 900, 50);
        when(distributedTracker.getStats()).thenReturn(stats);
        when(distributedTracker.getActiveInstances()).thenReturn(java.util.List.of("instance-1", "instance-2"));
        
        // When
        collector.collectAggregateMetrics();
        
        // Then
        Map<String, Object> metrics = collector.getAggregateMetrics();
        assertEquals(2L, metrics.get("activeInstances"));
        assertEquals(1000L, metrics.get("totalMessages"));
        assertEquals(900L, metrics.get("successfulDeliveries"));
        assertEquals(50L, metrics.get("failedDeliveries"));
        assertEquals(50L, metrics.get("pendingMessages"));
    }
    
    @Test
    void shouldCollectRedisMetricsWhenTrackerNotAvailable() throws Exception {
        // Given
        ReflectionTestUtils.setField(collector, "distributedTracker", null);
        
        Set<String> metricsKeys = Set.of(
            "streaming:metrics:instance-1",
            "streaming:metrics:instance-2"
        );
        when(redisTemplate.keys("streaming:metrics:*")).thenReturn(metricsKeys);
        
        InstanceMetrics metrics1 = new InstanceMetrics(
            "instance-1", System.currentTimeMillis(),
            500, 475, 25, 20, 5, 2
        );
        InstanceMetrics metrics2 = new InstanceMetrics(
            "instance-2", System.currentTimeMillis(),
            300, 280, 20, 15, 3, 1
        );
        
        when(valueOperations.get("streaming:metrics:instance-1"))
            .thenReturn(objectMapper.writeValueAsString(metrics1));
        when(valueOperations.get("streaming:metrics:instance-2"))
            .thenReturn(objectMapper.writeValueAsString(metrics2));
        
        // When
        collector.collectAggregateMetrics();
        
        // Then
        Map<String, Object> aggregateMetrics = collector.getAggregateMetrics();
        assertEquals(2L, aggregateMetrics.get("activeInstances"));
        assertEquals(800L, aggregateMetrics.get("totalMessages")); // 500 + 300
        assertEquals(755L, aggregateMetrics.get("successfulDeliveries")); // 475 + 280
        assertEquals(45L, aggregateMetrics.get("failedDeliveries")); // 25 + 20
        assertEquals(35L, aggregateMetrics.get("pendingMessages")); // 20 + 15
    }
    
    @Test
    void shouldCalculateSuccessAndFailureRates() {
        // Given - simulate collected metrics
        ReflectionTestUtils.setField(collector, "totalMessagesAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(1000));
        ReflectionTestUtils.setField(collector, "totalSuccessfulAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(950));
        ReflectionTestUtils.setField(collector, "totalFailedAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(50));
        
        // When
        double successRate = collector.getOverallSuccessRate();
        double failureRate = collector.getOverallFailureRate();
        
        // Then
        assertEquals(0.95, successRate, 0.001);
        assertEquals(0.05, failureRate, 0.001);
    }
    
    @Test
    void shouldReturnZeroRatesWhenNoMessages() {
        // Given - no messages processed
        ReflectionTestUtils.setField(collector, "totalMessagesAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(0));
        
        // When
        double successRate = collector.getOverallSuccessRate();
        double failureRate = collector.getOverallFailureRate();
        
        // Then
        assertEquals(0.0, successRate);
        assertEquals(0.0, failureRate);
    }
    
    @Test
    void shouldReturnPerInstanceMetrics() throws Exception {
        // Given
        Set<String> metricsKeys = Set.of(
            "streaming:metrics:instance-1",
            "streaming:metrics:instance-2"
        );
        when(redisTemplate.keys("streaming:metrics:*")).thenReturn(metricsKeys);
        
        InstanceMetrics metrics1 = new InstanceMetrics(
            "instance-1", System.currentTimeMillis(),
            100, 95, 5, 2, 0, 0
        );
        InstanceMetrics metrics2 = new InstanceMetrics(
            "instance-2", System.currentTimeMillis(),
            200, 190, 10, 5, 1, 0
        );
        
        when(valueOperations.get("streaming:metrics:instance-1"))
            .thenReturn(objectMapper.writeValueAsString(metrics1));
        when(valueOperations.get("streaming:metrics:instance-2"))
            .thenReturn(objectMapper.writeValueAsString(metrics2));
        
        // When
        Map<String, InstanceMetrics> instanceMetrics = collector.getPerInstanceMetrics();
        
        // Then
        assertEquals(2, instanceMetrics.size());
        assertTrue(instanceMetrics.containsKey("instance-1"));
        assertTrue(instanceMetrics.containsKey("instance-2"));
        assertEquals(100, instanceMetrics.get("instance-1").getTotalTracked());
        assertEquals(200, instanceMetrics.get("instance-2").getTotalTracked());
    }
    
    @Test
    void shouldReturnHealthyClusterWhenAllConditionsMet() {
        // Given
        ReflectionTestUtils.setField(collector, "activeInstanceCount", 
            new java.util.concurrent.atomic.AtomicLong(2));
        ReflectionTestUtils.setField(collector, "totalMessagesAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(1000));
        ReflectionTestUtils.setField(collector, "totalFailedAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(30)); // 3% failure rate
        
        // Mock recent instance metrics
        when(redisTemplate.keys("streaming:metrics:*")).thenReturn(Set.of("streaming:metrics:instance-1"));
        InstanceMetrics recentMetrics = new InstanceMetrics(
            "instance-1", System.currentTimeMillis(), // Recent timestamp
            100, 97, 3, 1, 0, 0
        );
        
        try {
            when(valueOperations.get("streaming:metrics:instance-1"))
                .thenReturn(objectMapper.writeValueAsString(recentMetrics));
        } catch (Exception e) {
            fail("Failed to mock instance metrics");
        }
        
        // When
        boolean healthy = collector.isClusterHealthy();
        
        // Then
        assertTrue(healthy);
    }
    
    @Test
    void shouldReturnUnhealthyClusterWhenNoActiveInstances() {
        // Given
        ReflectionTestUtils.setField(collector, "activeInstanceCount", 
            new java.util.concurrent.atomic.AtomicLong(0));
        
        // When
        boolean healthy = collector.isClusterHealthy();
        
        // Then
        assertFalse(healthy);
    }
    
    @Test
    void shouldReturnUnhealthyClusterWhenFailureRateHigh() {
        // Given
        ReflectionTestUtils.setField(collector, "activeInstanceCount", 
            new java.util.concurrent.atomic.AtomicLong(2));
        ReflectionTestUtils.setField(collector, "totalMessagesAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(1000));
        ReflectionTestUtils.setField(collector, "totalFailedAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(100)); // 10% failure rate
        
        // When
        boolean healthy = collector.isClusterHealthy();
        
        // Then
        assertFalse(healthy);
    }
    
    @Test
    void shouldReturnUnhealthyClusterWhenInstanceMetricsStale() throws Exception {
        // Given
        ReflectionTestUtils.setField(collector, "activeInstanceCount", 
            new java.util.concurrent.atomic.AtomicLong(1));
        ReflectionTestUtils.setField(collector, "totalMessagesAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(1000));
        ReflectionTestUtils.setField(collector, "totalFailedAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(30));
        
        // Mock stale instance metrics (older than 5 minutes)
        when(redisTemplate.keys("streaming:metrics:*")).thenReturn(Set.of("streaming:metrics:instance-1"));
        long staleTimestamp = System.currentTimeMillis() - (10 * 60 * 1000); // 10 minutes ago
        InstanceMetrics staleMetrics = new InstanceMetrics(
            "instance-1", staleTimestamp,
            100, 97, 3, 1, 0, 0
        );
        when(valueOperations.get("streaming:metrics:instance-1"))
            .thenReturn(objectMapper.writeValueAsString(staleMetrics));
        
        // When
        boolean healthy = collector.isClusterHealthy();
        
        // Then
        assertFalse(healthy);
    }
    
    @Test
    void shouldGeneratePrometheusMetrics() throws Exception {
        // Given
        ReflectionTestUtils.setField(collector, "activeInstanceCount", 
            new java.util.concurrent.atomic.AtomicLong(2));
        ReflectionTestUtils.setField(collector, "totalMessagesAcrossInstances", 
            new java.util.concurrent.atomic.AtomicLong(1000));
        
        when(redisTemplate.keys("streaming:metrics:*")).thenReturn(Set.of("streaming:metrics:instance-1"));
        InstanceMetrics metrics = new InstanceMetrics(
            "instance-1", System.currentTimeMillis(),
            100, 95, 5, 2, 0, 0
        );
        when(valueOperations.get("streaming:metrics:instance-1"))
            .thenReturn(objectMapper.writeValueAsString(metrics));
        
        // When
        String prometheusMetrics = collector.getPrometheusMetrics();
        
        // Then
        assertNotNull(prometheusMetrics);
        assertTrue(prometheusMetrics.contains("datastreaming_cluster_active_instances"));
        assertTrue(prometheusMetrics.contains("datastreaming_cluster_total_messages"));
        assertTrue(prometheusMetrics.contains("datastreaming_instance_messages_total{instance=\"instance-1\"}"));
    }
}