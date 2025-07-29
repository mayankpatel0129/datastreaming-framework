package com.datastreaming.framework.core.api;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FrameworkStatsTest {

    @Test
    void shouldCreateFrameworkStatsWithAllFields() {
        // Given
        long totalMessages = 1000L;
        long totalErrors = 50L;
        long pendingMessages = 10L;
        int activeWorkers = 5;
        int totalWorkers = 10;
        Instant startTime = Instant.now();
        Map<String, Object> additionalMetrics = Map.of("version", "1.0.0");

        // When
        FrameworkStats stats = new FrameworkStats(
            totalMessages, totalErrors, pendingMessages,
            activeWorkers, totalWorkers, startTime, additionalMetrics
        );

        // Then
        assertEquals(totalMessages, stats.getTotalMessagesProcessed());
        assertEquals(totalErrors, stats.getTotalErrors());
        assertEquals(pendingMessages, stats.getPendingMessages());
        assertEquals(activeWorkers, stats.getActiveWorkers());
        assertEquals(totalWorkers, stats.getTotalWorkers());
        assertEquals(startTime, stats.getStartTime());
        assertEquals(additionalMetrics, stats.getAdditionalMetrics());
    }

    @Test
    void shouldCalculateErrorRateCorrectly() {
        // Given
        FrameworkStats stats = new FrameworkStats(
            1000L, 50L, 0L, 5, 10, Instant.now(), Map.of()
        );

        // When
        double errorRate = stats.getErrorRate();

        // Then
        assertEquals(0.05, errorRate, 0.001, "Error rate should be 5%");
    }

    @Test
    void shouldCalculateZeroErrorRateWhenNoMessages() {
        // Given
        FrameworkStats stats = new FrameworkStats(
            0L, 0L, 0L, 5, 10, Instant.now(), Map.of()
        );

        // When
        double errorRate = stats.getErrorRate();

        // Then
        assertEquals(0.0, errorRate, "Error rate should be 0% when no messages processed");
    }

    @Test
    void shouldCalculateWorkerHealthRatioCorrectly() {
        // Given
        FrameworkStats stats = new FrameworkStats(
            1000L, 50L, 0L, 8, 10, Instant.now(), Map.of()
        );

        // When
        double healthRatio = stats.getWorkerHealthRatio();

        // Then
        assertEquals(0.8, healthRatio, 0.001, "Worker health ratio should be 80%");
    }

    @Test
    void shouldCalculateZeroWorkerHealthRatioWhenNoWorkers() {
        // Given
        FrameworkStats stats = new FrameworkStats(
            1000L, 50L, 0L, 0, 0, Instant.now(), Map.of()
        );

        // When
        double healthRatio = stats.getWorkerHealthRatio();

        // Then
        assertEquals(0.0, healthRatio, "Worker health ratio should be 0% when no workers");
    }

    @Test
    void shouldHandleAdditionalMetrics() {
        // Given
        Map<String, Object> additionalMetrics = Map.of(
            "version", "1.0.0",
            "environment", "production",
            "customMetric", 42
        );

        FrameworkStats stats = new FrameworkStats(
            1000L, 50L, 0L, 5, 10, Instant.now(), additionalMetrics
        );

        // When & Then
        assertEquals("1.0.0", stats.getAdditionalMetrics().get("version"));
        assertEquals("production", stats.getAdditionalMetrics().get("environment"));
        assertEquals(42, stats.getAdditionalMetrics().get("customMetric"));
    }
}