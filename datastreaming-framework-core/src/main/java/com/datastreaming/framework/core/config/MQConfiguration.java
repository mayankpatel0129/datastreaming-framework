package com.datastreaming.framework.core.config;

import com.datastreaming.framework.core.mq.*;
import com.datastreaming.framework.core.mq.parsers.DelimitedMessageParser;
import com.datastreaming.framework.core.mq.parsers.FixedLengthMessageParser;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for MQ message parsing capabilities
 */
@Configuration
@ConditionalOnProperty(prefix = "datastreaming.mq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MQConfiguration {
    
    /**
     * MQ-specific configuration properties
     */
    @ConfigurationProperties(prefix = "datastreaming.mq")
    public static class MQProperties {
        
        private boolean enabled = true;
        private ContractsConfig contracts = new ContractsConfig();
        private ParsingConfig parsing = new ParsingConfig();
        private Map<String, String> queueToTopicMapping = new HashMap<>();
        private Map<String, String> queueToContractMapping = new HashMap<>();
        
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        
        public ContractsConfig getContracts() { return contracts; }
        public void setContracts(ContractsConfig contracts) { this.contracts = contracts; }
        
        public ParsingConfig getParsing() { return parsing; }
        public void setParsing(ParsingConfig parsing) { this.parsing = parsing; }
        
        public Map<String, String> getQueueToTopicMapping() { return queueToTopicMapping; }
        public void setQueueToTopicMapping(Map<String, String> queueToTopicMapping) { 
            this.queueToTopicMapping = queueToTopicMapping; 
        }
        
        public Map<String, String> getQueueToContractMapping() { return queueToContractMapping; }
        public void setQueueToContractMapping(Map<String, String> queueToContractMapping) { 
            this.queueToContractMapping = queueToContractMapping; 
        }
        
        public static class ContractsConfig {
            private String location = "classpath*:mq/contracts/**/*.{yml,yaml,json}";
            private boolean reloadOnChange = false;
            private boolean validateOnStartup = true;
            
            public String getLocation() { return location; }
            public void setLocation(String location) { this.location = location; }
            
            public boolean isReloadOnChange() { return reloadOnChange; }
            public void setReloadOnChange(boolean reloadOnChange) { this.reloadOnChange = reloadOnChange; }
            
            public boolean isValidateOnStartup() { return validateOnStartup; }
            public void setValidateOnStartup(boolean validateOnStartup) { this.validateOnStartup = validateOnStartup; }
        }
        
        public static class ParsingConfig {
            private boolean strictMode = false;
            private boolean trimFields = true;
            private String defaultEncoding = "UTF-8";
            private int maxMessageSize = 1048576; // 1MB
            private boolean enableMetrics = true;
            
            public boolean isStrictMode() { return strictMode; }
            public void setStrictMode(boolean strictMode) { this.strictMode = strictMode; }
            
            public boolean isTrimFields() { return trimFields; }
            public void setTrimFields(boolean trimFields) { this.trimFields = trimFields; }
            
            public String getDefaultEncoding() { return defaultEncoding; }
            public void setDefaultEncoding(String defaultEncoding) { this.defaultEncoding = defaultEncoding; }
            
            public int getMaxMessageSize() { return maxMessageSize; }
            public void setMaxMessageSize(int maxMessageSize) { this.maxMessageSize = maxMessageSize; }
            
            public boolean isEnableMetrics() { return enableMetrics; }
            public void setEnableMetrics(boolean enableMetrics) { this.enableMetrics = enableMetrics; }
        }
    }
    
    @Bean
    @ConfigurationProperties(prefix = "datastreaming.mq")
    public MQProperties mqProperties() {
        return new MQProperties();
    }
    
    @Bean
    public FixedLengthMessageParser fixedLengthMessageParser() {
        return new FixedLengthMessageParser();
    }
    
    @Bean
    public DelimitedMessageParser delimitedMessageParser() {
        return new DelimitedMessageParser();
    }
    
    @Bean
    public MQContractRegistry mqContractRegistry(MQProperties mqProperties) {
        MQContractRegistry registry = new MQContractRegistry();
        // The registry will be initialized via @PostConstruct
        return registry;
    }
    
    @Bean
    public MQMessageParsingService mqMessageParsingService(
            java.util.List<MQMessageParser> parsers, 
            MQContractRegistry contractRegistry) {
        return new MQMessageParsingService(parsers, contractRegistry);
    }
    
    @Bean
    public MQToKafkaTransformer mqToKafkaTransformer(MQMessageParsingService parsingService) {
        return new MQToKafkaTransformer(parsingService);
    }
}