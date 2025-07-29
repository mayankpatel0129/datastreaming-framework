package com.datastreaming.framework.core.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class StreamingConfigurationTest {

    private Validator validator;
    private StreamingConfiguration configuration;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
        configuration = new StreamingConfiguration();
    }

    @Test
    void shouldCreateValidConfiguration() {
        // Given
        configuration.setSources(createValidSources());
        configuration.setKafka(createValidKafkaConfig());

        // When
        Set<ConstraintViolation<StreamingConfiguration>> violations = validator.validate(configuration);

        // Then
        assertTrue(violations.isEmpty(), "Configuration should be valid");
    }

    @Test
    void shouldFailValidationWithEmptySources() {
        // Given
        configuration.setSources(new ArrayList<>());
        configuration.setKafka(createValidKafkaConfig());

        // When
        Set<ConstraintViolation<StreamingConfiguration>> violations = validator.validate(configuration);

        // Then
        assertFalse(violations.isEmpty(), "Configuration should be invalid with empty sources");
    }

    @Test
    void shouldFailValidationWithNullKafkaConfig() {
        // Given
        configuration.setSources(createValidSources());
        configuration.setKafka(null);

        // When
        Set<ConstraintViolation<StreamingConfiguration>> violations = validator.validate(configuration);

        // Then
        assertFalse(violations.isEmpty(), "Configuration should be invalid with null Kafka config");
    }

    @Test
    void shouldHaveDefaultReconciliationConfiguration() {
        // Given
        configuration.setSources(createValidSources());
        configuration.setKafka(createValidKafkaConfig());

        // When
        StreamingConfiguration.ReconciliationConfiguration reconciliation = configuration.getReconciliation();

        // Then
        assertNotNull(reconciliation);
        assertTrue(reconciliation.isEnabled());
        assertEquals(30000L, reconciliation.getReconciliationIntervalMs());
        assertEquals(3, reconciliation.getMaxRetryAttempts());
    }

    @Test
    void shouldHaveDefaultPerformanceConfiguration() {
        // Given
        configuration.setSources(createValidSources());
        configuration.setKafka(createValidKafkaConfig());

        // When
        StreamingConfiguration.PerformanceConfiguration performance = configuration.getPerformance();

        // Then
        assertNotNull(performance);
        assertEquals(20, performance.getGlobalThreadPoolSize());
        assertEquals(10000, performance.getQueueCapacity());
        assertTrue(performance.isEnableBackpressure());
    }

    @Test
    void shouldConfigureMQSource() {
        // Given
        StreamingConfiguration.MQSourceConfiguration source = new StreamingConfiguration.MQSourceConfiguration();
        source.setName("test-source");
        source.setQueueManagerName("TEST_QM");
        source.setHostname("localhost");
        source.setPort(1414);
        source.setQueueName("TEST.QUEUE");
        source.setSourceContract("test-contract");
        source.setTargetTopic("test-topic");
        source.setTargetContract("test-avro");

        // When
        List<StreamingConfiguration.MQSourceConfiguration> sources = List.of(source);
        configuration.setSources(sources);

        // Then
        assertEquals(1, configuration.getSources().size());
        StreamingConfiguration.MQSourceConfiguration configuredSource = configuration.getSources().get(0);
        assertEquals("test-source", configuredSource.getName());
        assertEquals("TEST_QM", configuredSource.getQueueManagerName());
        assertEquals("localhost", configuredSource.getHostname());
        assertEquals(1414, configuredSource.getPort());
    }

    @Test
    void shouldConfigureKafkaWithDefaults() {
        // Given
        StreamingConfiguration.KafkaConfiguration kafka = new StreamingConfiguration.KafkaConfiguration();
        kafka.setBootstrapServers("localhost:9092");

        // When
        configuration.setKafka(kafka);

        // Then
        assertEquals("localhost:9092", kafka.getBootstrapServers());
        assertEquals("PLAINTEXT", kafka.getSecurityProtocol());
        assertEquals(65536, kafka.getBatchSize());
        assertEquals(10, kafka.getLingerMs());
        assertEquals("lz4", kafka.getCompressionType());
        assertTrue(kafka.isEnableIdempotence());
    }

    private List<StreamingConfiguration.MQSourceConfiguration> createValidSources() {
        StreamingConfiguration.MQSourceConfiguration source = new StreamingConfiguration.MQSourceConfiguration();
        source.setName("test-source");
        source.setQueueManagerName("TEST_QM");
        source.setHostname("localhost");
        source.setQueueName("TEST.QUEUE");
        source.setSourceContract("test-contract");
        source.setTargetTopic("test-topic");
        source.setTargetContract("test-avro");
        
        return List.of(source);
    }

    private StreamingConfiguration.KafkaConfiguration createValidKafkaConfig() {
        StreamingConfiguration.KafkaConfiguration kafka = new StreamingConfiguration.KafkaConfiguration();
        kafka.setBootstrapServers("localhost:9092");
        return kafka;
    }
}