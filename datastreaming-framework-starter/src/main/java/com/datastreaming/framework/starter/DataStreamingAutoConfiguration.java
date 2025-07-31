package com.datastreaming.framework.starter;

import com.datastreaming.framework.core.api.ContractRegistry;
import com.datastreaming.framework.core.api.MessageTransformer;
import com.datastreaming.framework.core.api.StreamingFramework;
import com.datastreaming.framework.core.config.StreamingConfiguration;
import com.datastreaming.framework.avro.AvroMessageTransformer;
import com.datastreaming.framework.avro.AvroSchemaManager;
import com.datastreaming.framework.core.alert.AlertService;
import com.datastreaming.framework.core.alert.LogBasedAlertNotifier;
import com.datastreaming.framework.core.alert.TeamsAlertNotifier;
import com.datastreaming.framework.core.health.AlertingHealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

/**
 * Auto-configuration for Data Streaming Framework
 */
@AutoConfiguration
@ConditionalOnClass(StreamingFramework.class)
@EnableConfigurationProperties(DataStreamingProperties.class)
@ComponentScan(basePackages = {
    "com.datastreaming.framework.core",
    "com.datastreaming.framework.avro"
})
@Import({
    DataStreamingCoreConfiguration.class,
    DataStreamingAvroConfiguration.class
})
public class DataStreamingAutoConfiguration {

    /**
     * Main framework bean
     */
    @Bean
    @ConditionalOnMissingBean
    public StreamingFramework streamingFramework() {
        return new DefaultStreamingFramework();
    }

    /**
     * Configuration converter from properties to framework configuration
     */
    @Bean
    @ConditionalOnMissingBean
    public StreamingConfigurationConverter configurationConverter() {
        return new StreamingConfigurationConverter();
    }

    /**
     * File-based contract registry
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "datastreaming.contracts", name = "type", havingValue = "file", matchIfMissing = true)
    public ContractRegistry fileContractRegistry() {
        return new FileBasedContractRegistry();
    }

    /**
     * Schema Registry based contract registry
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "datastreaming.contracts", name = "type", havingValue = "schema-registry")
    public ContractRegistry schemaRegistryContractRegistry() {
        return new SchemaRegistryContractRegistry();
    }
    
    /**
     * Alert service for system notifications
     */
    @Bean
    @ConditionalOnProperty(name = "datastreaming.alerts.enabled", havingValue = "true", matchIfMissing = true)
    public AlertService alertService() {
        return new AlertService();
    }
    
    /**
     * Log-based alert notifier
     */
    @Bean
    @ConditionalOnProperty(name = "datastreaming.alerts.notifiers.log.enabled", havingValue = "true", matchIfMissing = true)
    public LogBasedAlertNotifier logBasedAlertNotifier() {
        return new LogBasedAlertNotifier();
    }
    
    /**
     * Microsoft Teams alert notifier
     */
    @Bean
    @ConditionalOnProperty(name = "datastreaming.alerts.notifiers.teams.enabled", havingValue = "true")
    public TeamsAlertNotifier teamsAlertNotifier() {
        return new TeamsAlertNotifier();
    }
    
    /**
     * Health indicator for alert service
     */
    @Bean
    @ConditionalOnProperty(name = "datastreaming.alerts.enabled", havingValue = "true", matchIfMissing = true)
    public AlertingHealthIndicator alertingHealthIndicator() {
        return new AlertingHealthIndicator();
    }
}