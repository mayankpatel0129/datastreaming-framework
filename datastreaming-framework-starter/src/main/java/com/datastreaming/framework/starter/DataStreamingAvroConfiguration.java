package com.datastreaming.framework.starter;

import com.datastreaming.framework.avro.AvroSchemaManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Avro configuration for Data Streaming Framework
 */
@Configuration
@ConditionalOnClass(AvroSchemaManager.class)
public class DataStreamingAvroConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public AvroSchemaManager avroSchemaManager() {
        return new AvroSchemaManager();
    }
}