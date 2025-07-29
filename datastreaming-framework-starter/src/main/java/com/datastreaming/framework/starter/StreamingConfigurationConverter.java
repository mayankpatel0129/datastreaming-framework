package com.datastreaming.framework.starter;

import com.datastreaming.framework.core.config.StreamingConfiguration;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

/**
 * Converts DataStreamingProperties to StreamingConfiguration
 */
@Component
public class StreamingConfigurationConverter {

    public StreamingConfiguration convert(DataStreamingProperties properties) {
        StreamingConfiguration config = new StreamingConfiguration();

        // Convert sources
        if (properties.getSources() != null) {
            config.setSources(
                properties.getSources().stream()
                    .filter(DataStreamingProperties.SourceProperties::isEnabled)
                    .map(this::convertSource)
                    .collect(Collectors.toList())
            );
        }

        // Convert Kafka configuration
        config.setKafka(convertKafka(properties.getKafka()));

        // Convert reconciliation configuration
        config.setReconciliation(convertReconciliation(properties.getReconciliation()));

        // Convert performance configuration
        config.setPerformance(convertPerformance(properties.getPerformance()));

        // Convert monitoring configuration
        config.setMonitoring(convertMonitoring(properties.getMonitoring()));

        // Convert security configuration
        config.setSecurity(convertSecurity(properties.getSecurity()));

        return config;
    }

    private StreamingConfiguration.MQSourceConfiguration convertSource(DataStreamingProperties.SourceProperties source) {
        StreamingConfiguration.MQSourceConfiguration config = new StreamingConfiguration.MQSourceConfiguration();
        
        config.setName(source.getName());
        config.setQueueManagerName(source.getQueueManagerName());
        config.setHostname(source.getHostname());
        config.setPort(source.getPort());
        config.setChannel(source.getChannel());
        config.setQueueName(source.getQueueName());
        config.setUserId(source.getUserId());
        config.setPassword(source.getPassword());
        config.setSourceContract(source.getSourceContract());
        config.setTargetTopic(source.getTargetTopic());
        config.setTargetContract(source.getTargetContract());
        config.setConsumerThreads(source.getConsumerThreads());
        config.setMaxBatchSize(source.getMaxBatchSize());
        config.setEnabled(source.isEnabled());
        config.setAdditionalProperties(source.getAdditionalProperties());
        
        return config;
    }

    private StreamingConfiguration.KafkaConfiguration convertKafka(DataStreamingProperties.KafkaProperties kafka) {
        StreamingConfiguration.KafkaConfiguration config = new StreamingConfiguration.KafkaConfiguration();
        
        config.setBootstrapServers(kafka.getBootstrapServers());
        config.setSecurityProtocol(kafka.getSecurityProtocol());
        config.setSaslMechanism(kafka.getSaslMechanism());
        config.setSaslJaasConfig(kafka.getSaslJaasConfig());
        config.setSchemaRegistryUrl(kafka.getSchemaRegistryUrl());
        config.setSchemaRegistryCredentials(kafka.getSchemaRegistryCredentials());
        config.setBatchSize(kafka.getBatchSize());
        config.setLingerMs(kafka.getLingerMs());
        config.setBufferMemory(kafka.getBufferMemory());
        config.setCompressionType(kafka.getCompressionType());
        config.setMaxInFlightRequestsPerConnection(kafka.getMaxInFlightRequestsPerConnection());
        config.setEnableIdempotence(kafka.isEnableIdempotence());
        config.setAdditionalProperties(kafka.getAdditionalProperties());
        
        return config;
    }

    private StreamingConfiguration.ReconciliationConfiguration convertReconciliation(DataStreamingProperties.ReconciliationProperties reconciliation) {
        StreamingConfiguration.ReconciliationConfiguration config = new StreamingConfiguration.ReconciliationConfiguration();
        
        config.setEnabled(reconciliation.isEnabled());
        config.setReconciliationIntervalMs(reconciliation.getReconciliationIntervalMs());
        config.setMaxRetryAttempts(reconciliation.getMaxRetryAttempts());
        config.setRetryBackoffMs(reconciliation.getRetryBackoffMs());
        config.setEnableDeadLetterQueue(reconciliation.isEnableDeadLetterQueue());
        config.setDeadLetterTopic(reconciliation.getDeadLetterTopic());
        
        return config;
    }

    private StreamingConfiguration.PerformanceConfiguration convertPerformance(DataStreamingProperties.PerformanceProperties performance) {
        StreamingConfiguration.PerformanceConfiguration config = new StreamingConfiguration.PerformanceConfiguration();
        
        config.setGlobalThreadPoolSize(performance.getGlobalThreadPoolSize());
        config.setQueueCapacity(performance.getQueueCapacity());
        config.setKeepAliveTimeMs(performance.getKeepAliveTimeMs());
        config.setEnableBackpressure(performance.isEnableBackpressure());
        config.setBackpressureThreshold(performance.getBackpressureThreshold());
        
        return config;
    }

    private StreamingConfiguration.MonitoringConfiguration convertMonitoring(DataStreamingProperties.MonitoringProperties monitoring) {
        StreamingConfiguration.MonitoringConfiguration config = new StreamingConfiguration.MonitoringConfiguration();
        
        config.setMetricsEnabled(monitoring.isMetricsEnabled());
        config.setHealthChecksEnabled(monitoring.isHealthChecksEnabled());
        config.setMetricsPrefix(monitoring.getMetricsPrefix());
        config.setMetricsIntervalMs(monitoring.getMetricsIntervalMs());
        
        return config;
    }

    private StreamingConfiguration.SecurityConfiguration convertSecurity(DataStreamingProperties.SecurityProperties security) {
        StreamingConfiguration.SecurityConfiguration config = new StreamingConfiguration.SecurityConfiguration();
        
        config.setEnableSsl(security.isEnableSsl());
        config.setKeystorePath(security.getKeystorePath());
        config.setKeystorePassword(security.getKeystorePassword());
        config.setTruststorePath(security.getTruststorePath());
        config.setTruststorePassword(security.getTruststorePassword());
        config.setAdditionalProperties(security.getAdditionalProperties());
        
        return config;
    }
}