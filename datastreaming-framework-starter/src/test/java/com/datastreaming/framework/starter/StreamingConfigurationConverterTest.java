package com.datastreaming.framework.starter;

import com.datastreaming.framework.core.config.StreamingConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StreamingConfigurationConverterTest {

    private StreamingConfigurationConverter converter;
    private DataStreamingProperties properties;

    @BeforeEach
    void setUp() {
        converter = new StreamingConfigurationConverter();
        properties = new DataStreamingProperties();
    }

    @Test
    void shouldConvertMinimalConfiguration() {
        // Given
        DataStreamingProperties.SourceProperties source = new DataStreamingProperties.SourceProperties();
        source.setName("test-source");
        source.setQueueManagerName("TEST_QM");
        source.setHostname("localhost");
        source.setQueueName("TEST.QUEUE");
        source.setSourceContract("test-contract");
        source.setTargetTopic("test-topic");
        source.setTargetContract("test-avro");
        source.setEnabled(true);
        
        properties.setSources(List.of(source));
        properties.getKafka().setBootstrapServers("localhost:9092");

        // When
        StreamingConfiguration config = converter.convert(properties);

        // Then
        assertNotNull(config);
        assertEquals(1, config.getSources().size());
        assertNotNull(config.getKafka());
        assertNotNull(config.getReconciliation());
        assertNotNull(config.getPerformance());
        assertNotNull(config.getMonitoring());
        assertNotNull(config.getSecurity());
    }

    @Test
    void shouldConvertSourceConfiguration() {
        // Given
        DataStreamingProperties.SourceProperties sourceProps = new DataStreamingProperties.SourceProperties();
        sourceProps.setName("payment-processor");
        sourceProps.setQueueManagerName("PROD_QM");
        sourceProps.setHostname("mq-server.company.com");
        sourceProps.setPort(1414);
        sourceProps.setChannel("SYSTEM.DEF.SVRCONN");
        sourceProps.setQueueName("PAYMENT.QUEUE");
        sourceProps.setUserId("mquser");
        sourceProps.setPassword("mqpass");
        sourceProps.setSourceContract("payment-mq-v1");
        sourceProps.setTargetTopic("payment-events");
        sourceProps.setTargetContract("payment-avro-v1");
        sourceProps.setConsumerThreads(8);
        sourceProps.setMaxBatchSize(300);
        sourceProps.setEnabled(true);
        
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("XMSC_WMQ_CLIENT_RECONNECT_OPTIONS", "67108864");
        sourceProps.setAdditionalProperties(additionalProps);
        
        properties.setSources(List.of(sourceProps));
        properties.getKafka().setBootstrapServers("localhost:9092");

        // When
        StreamingConfiguration config = converter.convert(properties);

        // Then
        assertEquals(1, config.getSources().size());
        StreamingConfiguration.MQSourceConfiguration convertedSource = config.getSources().get(0);
        
        assertEquals("payment-processor", convertedSource.getName());
        assertEquals("PROD_QM", convertedSource.getQueueManagerName());
        assertEquals("mq-server.company.com", convertedSource.getHostname());
        assertEquals(1414, convertedSource.getPort());
        assertEquals("SYSTEM.DEF.SVRCONN", convertedSource.getChannel());
        assertEquals("PAYMENT.QUEUE", convertedSource.getQueueName());
        assertEquals("mquser", convertedSource.getUserId());
        assertEquals("mqpass", convertedSource.getPassword());
        assertEquals("payment-mq-v1", convertedSource.getSourceContract());
        assertEquals("payment-events", convertedSource.getTargetTopic());
        assertEquals("payment-avro-v1", convertedSource.getTargetContract());
        assertEquals(8, convertedSource.getConsumerThreads());
        assertEquals(300, convertedSource.getMaxBatchSize());
        assertTrue(convertedSource.isEnabled());
        assertEquals("67108864", convertedSource.getAdditionalProperties().get("XMSC_WMQ_CLIENT_RECONNECT_OPTIONS"));
    }

    @Test
    void shouldFilterDisabledSources() {
        // Given
        DataStreamingProperties.SourceProperties enabledSource = new DataStreamingProperties.SourceProperties();
        enabledSource.setName("enabled-source");
        enabledSource.setQueueManagerName("TEST_QM");
        enabledSource.setHostname("localhost");
        enabledSource.setQueueName("ENABLED.QUEUE");
        enabledSource.setSourceContract("test-contract");
        enabledSource.setTargetTopic("enabled-topic");
        enabledSource.setTargetContract("test-avro");
        enabledSource.setEnabled(true);

        DataStreamingProperties.SourceProperties disabledSource = new DataStreamingProperties.SourceProperties();
        disabledSource.setName("disabled-source");
        disabledSource.setQueueManagerName("TEST_QM");
        disabledSource.setHostname("localhost");
        disabledSource.setQueueName("DISABLED.QUEUE");
        disabledSource.setSourceContract("test-contract");
        disabledSource.setTargetTopic("disabled-topic");
        disabledSource.setTargetContract("test-avro");
        disabledSource.setEnabled(false);

        properties.setSources(List.of(enabledSource, disabledSource));
        properties.getKafka().setBootstrapServers("localhost:9092");

        // When
        StreamingConfiguration config = converter.convert(properties);

        // Then
        assertEquals(1, config.getSources().size());
        assertEquals("enabled-source", config.getSources().get(0).getName());
    }

    @Test
    void shouldConvertKafkaConfiguration() {
        // Given
        DataStreamingProperties.KafkaProperties kafkaProps = properties.getKafka();
        kafkaProps.setBootstrapServers("kafka-01:9092,kafka-02:9092");
        kafkaProps.setSecurityProtocol("SASL_SSL");
        kafkaProps.setSaslMechanism("PLAIN");
        kafkaProps.setSaslJaasConfig("test-jaas-config");
        kafkaProps.setSchemaRegistryUrl("http://schema-registry:8081");
        kafkaProps.setSchemaRegistryCredentials("user:pass");
        kafkaProps.setBatchSize(131072);
        kafkaProps.setLingerMs(5);
        kafkaProps.setBufferMemory(268435456L);
        kafkaProps.setCompressionType("lz4");
        kafkaProps.setMaxInFlightRequestsPerConnection(5);
        kafkaProps.setEnableIdempotence(true);

        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("ssl.truststore.location", "/opt/certs/truststore.jks");
        kafkaProps.setAdditionalProperties(additionalProps);

        // Add at least one source
        DataStreamingProperties.SourceProperties source = new DataStreamingProperties.SourceProperties();
        source.setName("test-source");
        source.setQueueManagerName("TEST_QM");
        source.setHostname("localhost");
        source.setQueueName("TEST.QUEUE");
        source.setSourceContract("test-contract");
        source.setTargetTopic("test-topic");
        source.setTargetContract("test-avro");
        source.setEnabled(true);
        properties.setSources(List.of(source));

        // When
        StreamingConfiguration config = converter.convert(properties);

        // Then
        StreamingConfiguration.KafkaConfiguration convertedKafka = config.getKafka();
        assertEquals("kafka-01:9092,kafka-02:9092", convertedKafka.getBootstrapServers());
        assertEquals("SASL_SSL", convertedKafka.getSecurityProtocol());
        assertEquals("PLAIN", convertedKafka.getSaslMechanism());
        assertEquals("test-jaas-config", convertedKafka.getSaslJaasConfig());
        assertEquals("http://schema-registry:8081", convertedKafka.getSchemaRegistryUrl());
        assertEquals("user:pass", convertedKafka.getSchemaRegistryCredentials());
        assertEquals(131072, convertedKafka.getBatchSize());
        assertEquals(5, convertedKafka.getLingerMs());
        assertEquals(268435456L, convertedKafka.getBufferMemory());
        assertEquals("lz4", convertedKafka.getCompressionType());
        assertEquals(5, convertedKafka.getMaxInFlightRequestsPerConnection());
        assertTrue(convertedKafka.isEnableIdempotence());
        assertEquals("/opt/certs/truststore.jks", convertedKafka.getAdditionalProperties().get("ssl.truststore.location"));
    }

    @Test
    void shouldConvertReconciliationConfiguration() {
        // Given
        DataStreamingProperties.ReconciliationProperties reconciliationProps = properties.getReconciliation();
        reconciliationProps.setEnabled(false);
        reconciliationProps.setReconciliationIntervalMs(60000L);
        reconciliationProps.setMaxRetryAttempts(5);
        reconciliationProps.setRetryBackoffMs(10000L);
        reconciliationProps.setEnableDeadLetterQueue(false);
        reconciliationProps.setDeadLetterTopic("custom-dlq");

        // Add minimal required config
        addMinimalSourceAndKafka();

        // When
        StreamingConfiguration config = converter.convert(properties);

        // Then
        StreamingConfiguration.ReconciliationConfiguration convertedReconciliation = config.getReconciliation();
        assertFalse(convertedReconciliation.isEnabled());
        assertEquals(60000L, convertedReconciliation.getReconciliationIntervalMs());
        assertEquals(5, convertedReconciliation.getMaxRetryAttempts());
        assertEquals(10000L, convertedReconciliation.getRetryBackoffMs());
        assertFalse(convertedReconciliation.isEnableDeadLetterQueue());
        assertEquals("custom-dlq", convertedReconciliation.getDeadLetterTopic());
    }

    @Test
    void shouldConvertPerformanceConfiguration() {
        // Given
        DataStreamingProperties.PerformanceProperties performanceProps = properties.getPerformance();
        performanceProps.setGlobalThreadPoolSize(100);
        performanceProps.setQueueCapacity(50000);
        performanceProps.setKeepAliveTimeMs(300000L);
        performanceProps.setEnableBackpressure(false);
        performanceProps.setBackpressureThreshold(40000);

        // Add minimal required config
        addMinimalSourceAndKafka();

        // When
        StreamingConfiguration config = converter.convert(properties);

        // Then
        StreamingConfiguration.PerformanceConfiguration convertedPerformance = config.getPerformance();
        assertEquals(100, convertedPerformance.getGlobalThreadPoolSize());
        assertEquals(50000, convertedPerformance.getQueueCapacity());
        assertEquals(300000L, convertedPerformance.getKeepAliveTimeMs());
        assertFalse(convertedPerformance.isEnableBackpressure());
        assertEquals(40000, convertedPerformance.getBackpressureThreshold());
    }

    @Test
    void shouldConvertMonitoringConfiguration() {
        // Given
        DataStreamingProperties.MonitoringProperties monitoringProps = properties.getMonitoring();
        monitoringProps.setMetricsEnabled(false);
        monitoringProps.setHealthChecksEnabled(false);
        monitoringProps.setMetricsPrefix("custom_prefix");
        monitoringProps.setMetricsIntervalMs(60000L);

        // Add minimal required config
        addMinimalSourceAndKafka();

        // When
        StreamingConfiguration config = converter.convert(properties);

        // Then
        StreamingConfiguration.MonitoringConfiguration convertedMonitoring = config.getMonitoring();
        assertFalse(convertedMonitoring.isMetricsEnabled());
        assertFalse(convertedMonitoring.isHealthChecksEnabled());
        assertEquals("custom_prefix", convertedMonitoring.getMetricsPrefix());
        assertEquals(60000L, convertedMonitoring.getMetricsIntervalMs());
    }

    @Test
    void shouldConvertSecurityConfiguration() {
        // Given
        DataStreamingProperties.SecurityProperties securityProps = properties.getSecurity();
        securityProps.setEnableSsl(true);
        securityProps.setKeystorePath("/opt/certs/keystore.jks");
        securityProps.setKeystorePassword("keystore-pass");
        securityProps.setTruststorePath("/opt/certs/truststore.jks");
        securityProps.setTruststorePassword("truststore-pass");

        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("ssl.protocol", "TLSv1.2");
        securityProps.setAdditionalProperties(additionalProps);

        // Add minimal required config
        addMinimalSourceAndKafka();

        // When
        StreamingConfiguration config = converter.convert(properties);

        // Then
        StreamingConfiguration.SecurityConfiguration convertedSecurity = config.getSecurity();
        assertTrue(convertedSecurity.isEnableSsl());
        assertEquals("/opt/certs/keystore.jks", convertedSecurity.getKeystorePath());
        assertEquals("keystore-pass", convertedSecurity.getKeystorePassword());
        assertEquals("/opt/certs/truststore.jks", convertedSecurity.getTruststorePath());
        assertEquals("truststore-pass", convertedSecurity.getTruststorePassword());
        assertEquals("TLSv1.2", convertedSecurity.getAdditionalProperties().get("ssl.protocol"));
    }

    private void addMinimalSourceAndKafka() {
        DataStreamingProperties.SourceProperties source = new DataStreamingProperties.SourceProperties();
        source.setName("test-source");
        source.setQueueManagerName("TEST_QM");
        source.setHostname("localhost");
        source.setQueueName("TEST.QUEUE");
        source.setSourceContract("test-contract");
        source.setTargetTopic("test-topic");
        source.setTargetContract("test-avro");
        source.setEnabled(true);
        properties.setSources(List.of(source));
        properties.getKafka().setBootstrapServers("localhost:9092");
    }
}