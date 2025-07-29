package com.datastreaming.framework.starter;

import com.datastreaming.framework.core.api.ContractRegistry;
import com.datastreaming.framework.core.api.MessageTransformer;
import com.datastreaming.framework.core.api.StreamingFramework;
import com.datastreaming.framework.core.config.StreamingConfiguration;
import com.datastreaming.framework.avro.AvroMessageTransformer;
import com.datastreaming.framework.avro.AvroSchemaManager;
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
}