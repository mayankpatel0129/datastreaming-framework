package com.datastreaming.sample;

import com.datastreaming.framework.core.api.StreamingFramework;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = {
    "datastreaming.enabled=true",
    "datastreaming.sources[0].name=test-source",
    "datastreaming.sources[0].queueManagerName=TEST_QM",
    "datastreaming.sources[0].hostname=localhost",
    "datastreaming.sources[0].queueName=TEST.QUEUE",
    "datastreaming.sources[0].sourceContract=test-contract",
    "datastreaming.sources[0].targetTopic=test-topic",
    "datastreaming.sources[0].targetContract=test-avro",
    "datastreaming.kafka.bootstrapServers=localhost:9092"
})
class SampleApplicationTest {

    @Autowired
    private StreamingFramework streamingFramework;

    @Test
    void contextLoads() {
        // Test that the Spring context loads successfully
        assertNotNull(streamingFramework);
    }

    @Test
    void shouldInitializeFramework() {
        // Given & When
        boolean isHealthy = streamingFramework.isHealthy();

        // Then
        assertTrue(isHealthy, "Framework should be healthy after initialization");
    }

    @Test
    void shouldProvideFrameworkStats() {
        // When
        var stats = streamingFramework.getStats();

        // Then
        assertNotNull(stats);
        assertNotNull(stats.getStartTime());
        assertNotNull(stats.getAdditionalMetrics());
    }

    @Test
    void shouldHaveFrameworkConfigured() {
        // When
        boolean running = streamingFramework.isRunning();
        boolean healthy = streamingFramework.isHealthy();

        // Then
        // Framework might not be running in test context, but should be healthy
        assertTrue(healthy, "Framework should be configured and healthy");
    }
}