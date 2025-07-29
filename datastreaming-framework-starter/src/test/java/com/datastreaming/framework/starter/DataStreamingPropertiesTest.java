package com.datastreaming.framework.starter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataStreamingPropertiesTest {

    private DataStreamingProperties properties;

    @BeforeEach
    void setUp() {
        properties = new DataStreamingProperties();
    }

    @Test
    void shouldHaveDefaultValues() {
        // When & Then
        assertTrue(properties.isEnabled());
        assertNotNull(properties.getSources());
        assertTrue(properties.getSources().isEmpty());
        assertNotNull(properties.getKafka());
        assertNotNull(properties.getReconciliation());
        assertNotNull(properties.getPerformance());
        assertNotNull(properties.getMonitoring());
        assertNotNull(properties.getSecurity());
        assertNotNull(properties.getContracts());
    }

    @Test
    void shouldConfigureSourceProperties() {
        // Given
        DataStreamingProperties.SourceProperties source = new DataStreamingProperties.SourceProperties();
        source.setName("test-source");
        source.setQueueManagerName("TEST_QM");
        source.setHostname("localhost");
        source.setPort(1414);
        source.setChannel("TEST.CHANNEL");
        source.setQueueName("TEST.QUEUE");
        source.setUserId("testuser");
        source.setPassword("testpass");
        source.setSourceContract("test-source-contract");
        source.setTargetTopic("test-topic");
        source.setTargetContract("test-target-contract");
        source.setConsumerThreads(10);
        source.setMaxBatchSize(500);
        source.setEnabled(true);

        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("key1", "value1");
        source.setAdditionalProperties(additionalProps);

        List<DataStreamingProperties.SourceProperties> sources = new ArrayList<>();
        sources.add(source);

        // When
        properties.setSources(sources);

        // Then
        assertEquals(1, properties.getSources().size());
        DataStreamingProperties.SourceProperties configuredSource = properties.getSources().get(0);
        assertEquals("test-source", configuredSource.getName());
        assertEquals("TEST_QM", configuredSource.getQueueManagerName());
        assertEquals("localhost", configuredSource.getHostname());
        assertEquals(1414, configuredSource.getPort());
        assertEquals("TEST.CHANNEL", configuredSource.getChannel());
        assertEquals("TEST.QUEUE", configuredSource.getQueueName());
        assertEquals("testuser", configuredSource.getUserId());
        assertEquals("testpass", configuredSource.getPassword());
        assertEquals("test-source-contract", configuredSource.getSourceContract());
        assertEquals("test-topic", configuredSource.getTargetTopic());
        assertEquals("test-target-contract", configuredSource.getTargetContract());
        assertEquals(10, configuredSource.getConsumerThreads());
        assertEquals(500, configuredSource.getMaxBatchSize());
        assertTrue(configuredSource.isEnabled());
        assertEquals("value1", configuredSource.getAdditionalProperties().get("key1"));
    }

    @Test
    void shouldConfigureKafkaProperties() {
        // Given
        DataStreamingProperties.KafkaProperties kafka = properties.getKafka();
        kafka.setBootstrapServers("localhost:9092");
        kafka.setSecurityProtocol("SASL_SSL");
        kafka.setSaslMechanism("PLAIN");
        kafka.setSaslJaasConfig("test-jaas-config");
        kafka.setSchemaRegistryUrl("http://localhost:8081");
        kafka.setSchemaRegistryCredentials("user:pass");
        kafka.setBatchSize(131072);
        kafka.setLingerMs(5);
        kafka.setBufferMemory(268435456L);
        kafka.setCompressionType("lz4");
        kafka.setMaxInFlightRequestsPerConnection(5);
        kafka.setEnableIdempotence(true);

        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("ssl.truststore.location", "/path/to/truststore");
        kafka.setAdditionalProperties(additionalProps);

        // When & Then
        assertEquals("localhost:9092", kafka.getBootstrapServers());
        assertEquals("SASL_SSL", kafka.getSecurityProtocol());
        assertEquals("PLAIN", kafka.getSaslMechanism());
        assertEquals("test-jaas-config", kafka.getSaslJaasConfig());
        assertEquals("http://localhost:8081", kafka.getSchemaRegistryUrl());
        assertEquals("user:pass", kafka.getSchemaRegistryCredentials());
        assertEquals(131072, kafka.getBatchSize());
        assertEquals(5, kafka.getLingerMs());
        assertEquals(268435456L, kafka.getBufferMemory());
        assertEquals("lz4", kafka.getCompressionType());
        assertEquals(5, kafka.getMaxInFlightRequestsPerConnection());
        assertTrue(kafka.isEnableIdempotence());
        assertEquals("/path/to/truststore", kafka.getAdditionalProperties().get("ssl.truststore.location"));
    }

    @Test
    void shouldHaveDefaultKafkaValues() {
        // Given
        DataStreamingProperties.KafkaProperties kafka = properties.getKafka();

        // When & Then
        assertEquals("PLAINTEXT", kafka.getSecurityProtocol());
        assertEquals(65536, kafka.getBatchSize());
        assertEquals(10, kafka.getLingerMs());
        assertEquals(134217728L, kafka.getBufferMemory());
        assertEquals("lz4", kafka.getCompressionType());
        assertEquals(5, kafka.getMaxInFlightRequestsPerConnection());
        assertTrue(kafka.isEnableIdempotence());
    }

    @Test
    void shouldConfigureReconciliationProperties() {
        // Given
        DataStreamingProperties.ReconciliationProperties reconciliation = properties.getReconciliation();
        reconciliation.setEnabled(false);
        reconciliation.setReconciliationIntervalMs(60000L);
        reconciliation.setMaxRetryAttempts(5);
        reconciliation.setRetryBackoffMs(10000L);
        reconciliation.setEnableDeadLetterQueue(false);
        reconciliation.setDeadLetterTopic("custom-dlq");

        // When & Then
        assertFalse(reconciliation.isEnabled());
        assertEquals(60000L, reconciliation.getReconciliationIntervalMs());
        assertEquals(5, reconciliation.getMaxRetryAttempts());
        assertEquals(10000L, reconciliation.getRetryBackoffMs());
        assertFalse(reconciliation.isEnableDeadLetterQueue());
        assertEquals("custom-dlq", reconciliation.getDeadLetterTopic());
    }

    @Test
    void shouldHaveDefaultReconciliationValues() {
        // Given
        DataStreamingProperties.ReconciliationProperties reconciliation = properties.getReconciliation();

        // When & Then
        assertTrue(reconciliation.isEnabled());
        assertEquals(30000L, reconciliation.getReconciliationIntervalMs());
        assertEquals(3, reconciliation.getMaxRetryAttempts());
        assertEquals(5000L, reconciliation.getRetryBackoffMs());
        assertTrue(reconciliation.isEnableDeadLetterQueue());
        assertEquals("streaming-dlq", reconciliation.getDeadLetterTopic());
    }

    @Test
    void shouldConfigurePerformanceProperties() {
        // Given
        DataStreamingProperties.PerformanceProperties performance = properties.getPerformance();
        performance.setGlobalThreadPoolSize(100);
        performance.setQueueCapacity(50000);
        performance.setKeepAliveTimeMs(300000L);
        performance.setEnableBackpressure(false);
        performance.setBackpressureThreshold(40000);

        // When & Then
        assertEquals(100, performance.getGlobalThreadPoolSize());
        assertEquals(50000, performance.getQueueCapacity());
        assertEquals(300000L, performance.getKeepAliveTimeMs());
        assertFalse(performance.isEnableBackpressure());
        assertEquals(40000, performance.getBackpressureThreshold());
    }

    @Test
    void shouldConfigureMonitoringProperties() {
        // Given
        DataStreamingProperties.MonitoringProperties monitoring = properties.getMonitoring();
        monitoring.setMetricsEnabled(false);
        monitoring.setHealthChecksEnabled(false);
        monitoring.setMetricsPrefix("custom_prefix");
        monitoring.setMetricsIntervalMs(60000L);

        // When & Then
        assertFalse(monitoring.isMetricsEnabled());
        assertFalse(monitoring.isHealthChecksEnabled());
        assertEquals("custom_prefix", monitoring.getMetricsPrefix());
        assertEquals(60000L, monitoring.getMetricsIntervalMs());
    }

    @Test
    void shouldConfigureSecurityProperties() {
        // Given
        DataStreamingProperties.SecurityProperties security = properties.getSecurity();
        security.setEnableSsl(true);
        security.setKeystorePath("/path/to/keystore");
        security.setKeystorePassword("keystore-pass");
        security.setTruststorePath("/path/to/truststore");
        security.setTruststorePassword("truststore-pass");

        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("ssl.protocol", "TLSv1.2");
        security.setAdditionalProperties(additionalProps);

        // When & Then
        assertTrue(security.isEnableSsl());
        assertEquals("/path/to/keystore", security.getKeystorePath());
        assertEquals("keystore-pass", security.getKeystorePassword());
        assertEquals("/path/to/truststore", security.getTruststorePath());
        assertEquals("truststore-pass", security.getTruststorePassword());
        assertEquals("TLSv1.2", security.getAdditionalProperties().get("ssl.protocol"));
    }

    @Test
    void shouldConfigureContractsProperties() {
        // Given
        DataStreamingProperties.ContractsProperties contracts = properties.getContracts();
        contracts.setType("schema-registry");
        contracts.setLocation("http://schema-registry:8081");
        contracts.setAutoReload(false);
        contracts.setReloadIntervalMs(600000L);

        // When & Then
        assertEquals("schema-registry", contracts.getType());
        assertEquals("http://schema-registry:8081", contracts.getLocation());
        assertFalse(contracts.isAutoReload());
        assertEquals(600000L, contracts.getReloadIntervalMs());
    }

    @Test
    void shouldHaveDefaultContractsValues() {
        // Given
        DataStreamingProperties.ContractsProperties contracts = properties.getContracts();

        // When & Then
        assertEquals("file", contracts.getType());
        assertEquals("classpath:/contracts/", contracts.getLocation());
        assertTrue(contracts.isAutoReload());
        assertEquals(300000L, contracts.getReloadIntervalMs());
    }
}