package com.datastreaming.framework.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration properties for Data Streaming Framework
 * Maps to YAML/properties configuration
 */
@ConfigurationProperties(prefix = "datastreaming")
public class DataStreamingProperties {

    private boolean enabled = true;
    
    @NestedConfigurationProperty
    private List<SourceProperties> sources = new ArrayList<>();
    
    @NestedConfigurationProperty
    private KafkaProperties kafka = new KafkaProperties();
    
    @NestedConfigurationProperty
    private ReconciliationProperties reconciliation = new ReconciliationProperties();
    
    @NestedConfigurationProperty
    private PerformanceProperties performance = new PerformanceProperties();
    
    @NestedConfigurationProperty
    private MonitoringProperties monitoring = new MonitoringProperties();
    
    @NestedConfigurationProperty
    private SecurityProperties security = new SecurityProperties();
    
    @NestedConfigurationProperty
    private ContractsProperties contracts = new ContractsProperties();

    // Getters and setters
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public List<SourceProperties> getSources() { return sources; }
    public void setSources(List<SourceProperties> sources) { this.sources = sources; }
    public KafkaProperties getKafka() { return kafka; }
    public void setKafka(KafkaProperties kafka) { this.kafka = kafka; }
    public ReconciliationProperties getReconciliation() { return reconciliation; }
    public void setReconciliation(ReconciliationProperties reconciliation) { this.reconciliation = reconciliation; }
    public PerformanceProperties getPerformance() { return performance; }
    public void setPerformance(PerformanceProperties performance) { this.performance = performance; }
    public MonitoringProperties getMonitoring() { return monitoring; }
    public void setMonitoring(MonitoringProperties monitoring) { this.monitoring = monitoring; }
    public SecurityProperties getSecurity() { return security; }
    public void setSecurity(SecurityProperties security) { this.security = security; }
    public ContractsProperties getContracts() { return contracts; }
    public void setContracts(ContractsProperties contracts) { this.contracts = contracts; }

    public static class SourceProperties {
        private String name;
        private String queueManagerName;
        private String hostname;
        private int port = 1414;
        private String channel = "SYSTEM.DEF.SVRCONN";
        private String queueName;
        private String userId;
        private String password;
        private String sourceContract;
        private String targetTopic;
        private String targetContract;
        private int consumerThreads = 5;
        private int maxBatchSize = 100;
        private boolean enabled = true;
        private Map<String, String> additionalProperties = new HashMap<>();

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getQueueManagerName() { return queueManagerName; }
        public void setQueueManagerName(String queueManagerName) { this.queueManagerName = queueManagerName; }
        public String getHostname() { return hostname; }
        public void setHostname(String hostname) { this.hostname = hostname; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getChannel() { return channel; }
        public void setChannel(String channel) { this.channel = channel; }
        public String getQueueName() { return queueName; }
        public void setQueueName(String queueName) { this.queueName = queueName; }
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getSourceContract() { return sourceContract; }
        public void setSourceContract(String sourceContract) { this.sourceContract = sourceContract; }
        public String getTargetTopic() { return targetTopic; }
        public void setTargetTopic(String targetTopic) { this.targetTopic = targetTopic; }
        public String getTargetContract() { return targetContract; }
        public void setTargetContract(String targetContract) { this.targetContract = targetContract; }
        public int getConsumerThreads() { return consumerThreads; }
        public void setConsumerThreads(int consumerThreads) { this.consumerThreads = consumerThreads; }
        public int getMaxBatchSize() { return maxBatchSize; }
        public void setMaxBatchSize(int maxBatchSize) { this.maxBatchSize = maxBatchSize; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public Map<String, String> getAdditionalProperties() { return additionalProperties; }
        public void setAdditionalProperties(Map<String, String> additionalProperties) { this.additionalProperties = additionalProperties; }
    }

    public static class KafkaProperties {
        private String bootstrapServers;
        private String securityProtocol = "PLAINTEXT";
        private String saslMechanism;
        private String saslJaasConfig;
        private String schemaRegistryUrl;
        private String schemaRegistryCredentials;
        private int batchSize = 65536;
        private int lingerMs = 10;
        private long bufferMemory = 134217728L;
        private String compressionType = "lz4";
        private int maxInFlightRequestsPerConnection = 5;
        private boolean enableIdempotence = true;
        private Map<String, String> additionalProperties = new HashMap<>();

        // Getters and setters - similar pattern as above
        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public String getSecurityProtocol() { return securityProtocol; }
        public void setSecurityProtocol(String securityProtocol) { this.securityProtocol = securityProtocol; }
        public String getSaslMechanism() { return saslMechanism; }
        public void setSaslMechanism(String saslMechanism) { this.saslMechanism = saslMechanism; }
        public String getSaslJaasConfig() { return saslJaasConfig; }
        public void setSaslJaasConfig(String saslJaasConfig) { this.saslJaasConfig = saslJaasConfig; }
        public String getSchemaRegistryUrl() { return schemaRegistryUrl; }
        public void setSchemaRegistryUrl(String schemaRegistryUrl) { this.schemaRegistryUrl = schemaRegistryUrl; }
        public String getSchemaRegistryCredentials() { return schemaRegistryCredentials; }
        public void setSchemaRegistryCredentials(String schemaRegistryCredentials) { this.schemaRegistryCredentials = schemaRegistryCredentials; }
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        public int getLingerMs() { return lingerMs; }
        public void setLingerMs(int lingerMs) { this.lingerMs = lingerMs; }
        public long getBufferMemory() { return bufferMemory; }
        public void setBufferMemory(long bufferMemory) { this.bufferMemory = bufferMemory; }
        public String getCompressionType() { return compressionType; }
        public void setCompressionType(String compressionType) { this.compressionType = compressionType; }
        public int getMaxInFlightRequestsPerConnection() { return maxInFlightRequestsPerConnection; }
        public void setMaxInFlightRequestsPerConnection(int maxInFlightRequestsPerConnection) { this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection; }
        public boolean isEnableIdempotence() { return enableIdempotence; }
        public void setEnableIdempotence(boolean enableIdempotence) { this.enableIdempotence = enableIdempotence; }
        public Map<String, String> getAdditionalProperties() { return additionalProperties; }
        public void setAdditionalProperties(Map<String, String> additionalProperties) { this.additionalProperties = additionalProperties; }
    }

    public static class ReconciliationProperties {
        private boolean enabled = true;
        private long reconciliationIntervalMs = 30000L;
        private int maxRetryAttempts = 3;
        private long retryBackoffMs = 5000L;
        private boolean enableDeadLetterQueue = true;
        private String deadLetterTopic = "streaming-dlq";

        // Getters and setters
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getReconciliationIntervalMs() { return reconciliationIntervalMs; }
        public void setReconciliationIntervalMs(long reconciliationIntervalMs) { this.reconciliationIntervalMs = reconciliationIntervalMs; }
        public int getMaxRetryAttempts() { return maxRetryAttempts; }
        public void setMaxRetryAttempts(int maxRetryAttempts) { this.maxRetryAttempts = maxRetryAttempts; }
        public long getRetryBackoffMs() { return retryBackoffMs; }
        public void setRetryBackoffMs(long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; }
        public boolean isEnableDeadLetterQueue() { return enableDeadLetterQueue; }
        public void setEnableDeadLetterQueue(boolean enableDeadLetterQueue) { this.enableDeadLetterQueue = enableDeadLetterQueue; }
        public String getDeadLetterTopic() { return deadLetterTopic; }
        public void setDeadLetterTopic(String deadLetterTopic) { this.deadLetterTopic = deadLetterTopic; }
    }

    public static class PerformanceProperties {
        private int globalThreadPoolSize = 20;
        private int queueCapacity = 10000;
        private long keepAliveTimeMs = 60000L;
        private boolean enableBackpressure = true;
        private int backpressureThreshold = 8000;

        // Getters and setters
        public int getGlobalThreadPoolSize() { return globalThreadPoolSize; }
        public void setGlobalThreadPoolSize(int globalThreadPoolSize) { this.globalThreadPoolSize = globalThreadPoolSize; }
        public int getQueueCapacity() { return queueCapacity; }
        public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
        public long getKeepAliveTimeMs() { return keepAliveTimeMs; }
        public void setKeepAliveTimeMs(long keepAliveTimeMs) { this.keepAliveTimeMs = keepAliveTimeMs; }
        public boolean isEnableBackpressure() { return enableBackpressure; }
        public void setEnableBackpressure(boolean enableBackpressure) { this.enableBackpressure = enableBackpressure; }
        public int getBackpressureThreshold() { return backpressureThreshold; }
        public void setBackpressureThreshold(int backpressureThreshold) { this.backpressureThreshold = backpressureThreshold; }
    }

    public static class MonitoringProperties {
        private boolean metricsEnabled = true;
        private boolean healthChecksEnabled = true;
        private String metricsPrefix = "datastreaming";
        private long metricsIntervalMs = 30000L;

        // Getters and setters
        public boolean isMetricsEnabled() { return metricsEnabled; }
        public void setMetricsEnabled(boolean metricsEnabled) { this.metricsEnabled = metricsEnabled; }
        public boolean isHealthChecksEnabled() { return healthChecksEnabled; }
        public void setHealthChecksEnabled(boolean healthChecksEnabled) { this.healthChecksEnabled = healthChecksEnabled; }
        public String getMetricsPrefix() { return metricsPrefix; }
        public void setMetricsPrefix(String metricsPrefix) { this.metricsPrefix = metricsPrefix; }
        public long getMetricsIntervalMs() { return metricsIntervalMs; }
        public void setMetricsIntervalMs(long metricsIntervalMs) { this.metricsIntervalMs = metricsIntervalMs; }
    }

    public static class SecurityProperties {
        private boolean enableSsl = false;
        private String keystorePath;
        private String keystorePassword;
        private String truststorePath;
        private String truststorePassword;
        private Map<String, String> additionalProperties = new HashMap<>();

        // Getters and setters
        public boolean isEnableSsl() { return enableSsl; }
        public void setEnableSsl(boolean enableSsl) { this.enableSsl = enableSsl; }
        public String getKeystorePath() { return keystorePath; }
        public void setKeystorePath(String keystorePath) { this.keystorePath = keystorePath; }
        public String getKeystorePassword() { return keystorePassword; }
        public void setKeystorePassword(String keystorePassword) { this.keystorePassword = keystorePassword; }
        public String getTruststorePath() { return truststorePath; }
        public void setTruststorePath(String truststorePath) { this.truststorePath = truststorePath; }
        public String getTruststorePassword() { return truststorePassword; }
        public void setTruststorePassword(String truststorePassword) { this.truststorePassword = truststorePassword; }
        public Map<String, String> getAdditionalProperties() { return additionalProperties; }
        public void setAdditionalProperties(Map<String, String> additionalProperties) { this.additionalProperties = additionalProperties; }
    }

    public static class ContractsProperties {
        private String type = "file"; // file, schema-registry
        private String location = "classpath:/contracts/";
        private boolean autoReload = true;
        private long reloadIntervalMs = 300000L; // 5 minutes

        // Getters and setters
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getLocation() { return location; }
        public void setLocation(String location) { this.location = location; }
        public boolean isAutoReload() { return autoReload; }
        public void setAutoReload(boolean autoReload) { this.autoReload = autoReload; }
        public long getReloadIntervalMs() { return reloadIntervalMs; }
        public void setReloadIntervalMs(long reloadIntervalMs) { this.reloadIntervalMs = reloadIntervalMs; }
    }
}