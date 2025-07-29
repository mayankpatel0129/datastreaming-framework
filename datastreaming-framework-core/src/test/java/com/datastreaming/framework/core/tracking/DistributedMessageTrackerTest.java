package com.datastreaming.framework.core.tracking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DistributedMessageTrackerTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;
    
    @Mock
    private ValueOperations<String, String> valueOperations;
    
    private DistributedMessageTracker tracker;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        tracker = new DistributedMessageTracker();
        objectMapper = new ObjectMapper();
        
        // Set up mocks
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        
        // Inject dependencies
        ReflectionTestUtils.setField(tracker, "redisTemplate", redisTemplate);
        ReflectionTestUtils.setField(tracker, "objectMapper", objectMapper);
        ReflectionTestUtils.setField(tracker, "instanceId", "test-instance-1");
        ReflectionTestUtils.setField(tracker, "ttlHours", 24);
        ReflectionTestUtils.setField(tracker, "reconciliationIntervalMs", 30000L);
        ReflectionTestUtils.setField(tracker, "maxRetryAttempts", 3);
        ReflectionTestUtils.setField(tracker, "retryBackoffMs", 5000L);
        
        // Initialize the tracker
        tracker.initialize();
    }
    
    @Test
    void shouldTrackMessageSuccessfully() throws Exception {
        // Given
        String correlationId = "test-correlation-123";
        String sourceQueue = "TEST.QUEUE";
        String targetTopic = "test-topic";
        
        // When
        tracker.trackMessage(correlationId, sourceQueue, targetTopic);
        
        // Then
        verify(valueOperations).set(
            eq("streaming:tracking:" + correlationId), 
            anyString(), 
            eq(Duration.ofHours(24))
        );
    }
    
    @Test
    void shouldRecordSuccessWithValidMetadata() throws Exception {
        // Given
        String correlationId = "test-correlation-123";
        MessageStatus status = new MessageStatus(
            correlationId, "test-instance-1", "TEST.QUEUE", "test-topic",
            System.currentTimeMillis(), DeliveryStatus.PENDING
        );
        String statusJson = objectMapper.writeValueAsString(status);
        
        RecordMetadata metadata = new RecordMetadata(
            new TopicPartition("test-topic", 0), 0, 100L, 
            System.currentTimeMillis(), 0, 0
        );
        
        when(valueOperations.get("streaming:tracking:" + correlationId))
            .thenReturn(statusJson);
        
        // When
        tracker.recordSuccess(correlationId, metadata);
        
        // Then
        verify(valueOperations).set(
            eq("streaming:tracking:" + correlationId),
            anyString(),
            eq(Duration.ofHours(24))
        );
    }
    
    @Test
    void shouldRecordFailureWithErrorMessage() throws Exception {
        // Given
        String correlationId = "test-correlation-123";
        String errorMessage = "Kafka connection failed";
        MessageStatus status = new MessageStatus(
            correlationId, "test-instance-1", "TEST.QUEUE", "test-topic",
            System.currentTimeMillis(), DeliveryStatus.PENDING
        );
        String statusJson = objectMapper.writeValueAsString(status);
        
        when(valueOperations.get("streaming:tracking:" + correlationId))
            .thenReturn(statusJson);
        
        // When
        tracker.recordFailure(correlationId, errorMessage);
        
        // Then
        verify(valueOperations).set(
            eq("streaming:tracking:" + correlationId),
            anyString(),
            eq(Duration.ofHours(24))
        );
    }
    
    @Test
    void shouldReturnEmptyStatsWhenNoMetricsAvailable() {
        // Given
        when(redisTemplate.keys("streaming:metrics:*")).thenReturn(null);
        
        // When
        ReconciliationStats stats = tracker.getStats();
        
        // Then
        assertEquals(0, stats.getTotalTracked());
        assertEquals(0, stats.getPending());
        assertEquals(0, stats.getDelivered());
        assertEquals(0, stats.getFailed());
    }
    
    @Test
    void shouldCalculateAggregateStatsFromMultipleInstances() throws Exception {
        // Given
        Set<String> metricsKeys = Set.of(
            "streaming:metrics:instance-1",
            "streaming:metrics:instance-2"
        );
        when(redisTemplate.keys("streaming:metrics:*")).thenReturn(metricsKeys);
        
        InstanceMetrics metrics1 = new InstanceMetrics(
            "instance-1", System.currentTimeMillis(),
            100, 95, 5, 10, 0, 0
        );
        InstanceMetrics metrics2 = new InstanceMetrics(
            "instance-2", System.currentTimeMillis(),
            200, 180, 20, 15, 2, 1
        );
        
        when(valueOperations.get("streaming:metrics:instance-1"))
            .thenReturn(objectMapper.writeValueAsString(metrics1));
        when(valueOperations.get("streaming:metrics:instance-2"))
            .thenReturn(objectMapper.writeValueAsString(metrics2));
        
        // When
        ReconciliationStats stats = tracker.getStats();
        
        // Then
        assertEquals(300, stats.getTotalTracked()); // 100 + 200
        assertEquals(275, stats.getSuccessfulDeliveries()); // 95 + 180
        assertEquals(25, stats.getFailedDeliveries()); // 5 + 20
    }
    
    @Test
    void shouldReturnActiveInstancesList() throws Exception {
        // Given
        Set<String> instanceKeys = Set.of(
            "streaming:instance:instance-1",
            "streaming:instance:instance-2"
        );
        when(redisTemplate.keys("streaming:instance:*")).thenReturn(instanceKeys);
        
        InstanceInfo info1 = new InstanceInfo("instance-1", System.currentTimeMillis(), "ACTIVE");
        InstanceInfo info2 = new InstanceInfo("instance-2", System.currentTimeMillis(), "ACTIVE");
        
        when(valueOperations.get("streaming:instance:instance-1"))
            .thenReturn(objectMapper.writeValueAsString(info1));
        when(valueOperations.get("streaming:instance:instance-2"))
            .thenReturn(objectMapper.writeValueAsString(info2));
        
        // When
        List<String> activeInstances = tracker.getActiveInstances();
        
        // Then
        assertEquals(2, activeInstances.size());
        assertTrue(activeInstances.contains("instance-1"));
        assertTrue(activeInstances.contains("instance-2"));
    }
    
    @Test
    void shouldReturnHealthyWhenRedisIsAccessible() {
        // Given
        when(valueOperations.get("health-check")).thenReturn(null); // No exception means healthy
        
        // When
        boolean healthy = tracker.isHealthy();
        
        // Then
        assertTrue(healthy);
    }
    
    @Test
    void shouldReturnUnhealthyWhenRedisIsNotAccessible() {
        // Given
        when(valueOperations.get("health-check")).thenThrow(new RuntimeException("Redis connection failed"));
        
        // When
        boolean healthy = tracker.isHealthy();
        
        // Then
        assertFalse(healthy);
    }
    
    @Test
    void shouldPerformReconciliationAndRetryTimedOutMessages() throws Exception {
        // Given
        Set<String> trackingKeys = Set.of("streaming:tracking:msg-1", "streaming:tracking:msg-2");
        when(redisTemplate.keys("streaming:tracking:*")).thenReturn(trackingKeys);
        
        // Create an old pending message that should be retried
        long oldTimestamp = System.currentTimeMillis() - (60 * 1000); // 1 minute ago
        MessageStatus oldStatus = new MessageStatus(
            "msg-1", "test-instance-1", "TEST.QUEUE", "test-topic",
            oldTimestamp, DeliveryStatus.PENDING
        );
        
        // Create a recent pending message that should not be retried
        long recentTimestamp = System.currentTimeMillis() - (5 * 1000); // 5 seconds ago
        MessageStatus recentStatus = new MessageStatus(
            "msg-2", "test-instance-1", "TEST.QUEUE", "test-topic",
            recentTimestamp, DeliveryStatus.PENDING
        );
        
        when(valueOperations.get("streaming:tracking:msg-1"))
            .thenReturn(objectMapper.writeValueAsString(oldStatus));
        when(valueOperations.get("streaming:tracking:msg-2"))
            .thenReturn(objectMapper.writeValueAsString(recentStatus));
        
        // When
        tracker.performReconciliation();
        
        // Then
        // Verify that the old message was updated (retry scheduled)
        verify(valueOperations, times(2)).set(
            eq("streaming:tracking:msg-1"),
            anyString(),
            eq(Duration.ofHours(24))
        );
        
        // Verify metrics were updated
        verify(valueOperations).set(
            eq("streaming:metrics:test-instance-1"),
            anyString(),
            eq(Duration.ofMinutes(5))
        );
    }
    
    @Test
    void shouldCleanupOldCacheEntries() throws InterruptedException {
        // Given
        String correlationId = "test-msg";
        tracker.trackMessage(correlationId, "TEST.QUEUE", "test-topic");
        
        // When
        tracker.heartbeat(); // This triggers cache cleanup
        
        // Then
        // Verify that the tracker continues to work (no exceptions thrown)
        assertDoesNotThrow(() -> tracker.trackMessage("new-msg", "TEST.QUEUE", "test-topic"));
    }
}