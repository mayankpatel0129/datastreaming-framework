package com.datastreaming.framework.core.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

/**
 * Main configuration class for the streaming framework
 * This is externalized and can be provided via YAML, properties, or programmatically
 */
public class StreamingConfiguration {

    @NotEmpty
    @Valid
    private List<MQSourceConfiguration> sources;

    @NotNull
    @Valid
    private KafkaConfiguration kafka;
    
    @Valid
    private InstanceConfiguration instance = new InstanceConfiguration();

    @Valid
    private ReconciliationConfiguration reconciliation = new ReconciliationConfiguration();

    @Valid
    private PerformanceConfiguration performance = new PerformanceConfiguration();

    @Valid
    private MonitoringConfiguration monitoring = new MonitoringConfiguration();

    @Valid
    private SecurityConfiguration security = new SecurityConfiguration();

    // Getters and setters
    public List<MQSourceConfiguration> getSources() { return sources; }
    public void setSources(List<MQSourceConfiguration> sources) { this.sources = sources; }
    public KafkaConfiguration getKafka() { return kafka; }
    public void setKafka(KafkaConfiguration kafka) { this.kafka = kafka; }
    public InstanceConfiguration getInstance() { return instance; }
    public void setInstance(InstanceConfiguration instance) { this.instance = instance; }
    public ReconciliationConfiguration getReconciliation() { return reconciliation; }
    public void setReconciliation(ReconciliationConfiguration reconciliation) { this.reconciliation = reconciliation; }
    public PerformanceConfiguration getPerformance() { return performance; }
    public void setPerformance(PerformanceConfiguration performance) { this.performance = performance; }
    public MonitoringConfiguration getMonitoring() { return monitoring; }
    public void setMonitoring(MonitoringConfiguration monitoring) { this.monitoring = monitoring; }
    public SecurityConfiguration getSecurity() { return security; }
    public void setSecurity(SecurityConfiguration security) { this.security = security; }

    /**
     * MQ Source Configuration - Each source represents a queue to consume from
     */
    public static class MQSourceConfiguration {
        private String name; // Unique identifier for this source
        private String queueManagerName;
        private String hostname;
        private int port = 1414;
        private String channel = "SYSTEM.DEF.SVRCONN";
        private String queueName;
        private String userId;
        private String password;
        private String sourceContract; // Contract ID for incoming messages
        private String targetTopic; // Kafka topic to publish to
        private String targetContract; // Contract ID for outgoing messages
        private int consumerThreads = 5;
        private int maxBatchSize = 100;
        private boolean enabled = true;
        private Map<String, String> additionalProperties;

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

    /**
     * Kafka Configuration
     */
    public static class KafkaConfiguration {
        private String bootstrapServers;
        private String securityProtocol = "PLAINTEXT";
        private String saslMechanism;
        private String saslJaasConfig;
        private String schemaRegistryUrl; // For Avro support
        private String schemaRegistryCredentials;
        private int batchSize = 65536;
        private int lingerMs = 10;
        private long bufferMemory = 134217728L;
        private String compressionType = "lz4";
        private int maxInFlightRequestsPerConnection = 5;
        private boolean enableIdempotence = true;
        private Map<String, String> additionalProperties;

        // Getters and setters
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

    /**
     * Reconciliation Configuration
     */
    public static class ReconciliationConfiguration {
        private boolean enabled = true;
        private String mode = "local"; // local, distributed, hybrid
        private long reconciliationIntervalMs = 30000L;
        private int maxRetryAttempts = 3;
        private long retryBackoffMs = 5000L;
        private boolean enableDeadLetterQueue = true;
        private String deadLetterTopic = "streaming-dlq";
        private int ttlHours = 24;
        
        // Distributed mode specific settings
        private DistributedConfiguration distributed = new DistributedConfiguration();

        // Getters and setters
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
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
        public int getTtlHours() { return ttlHours; }
        public void setTtlHours(int ttlHours) { this.ttlHours = ttlHours; }
        public DistributedConfiguration getDistributed() { return distributed; }
        public void setDistributed(DistributedConfiguration distributed) { this.distributed = distributed; }
    }

    /**
     * Performance Configuration
     */
    public static class PerformanceConfiguration {
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

    /**
     * Monitoring Configuration
     */
    public static class MonitoringConfiguration {
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

    /**
     * Security Configuration
     */
    public static class SecurityConfiguration {
        private boolean enableSsl = false;
        private String keystorePath;
        private String keystorePassword;
        private String truststorePath;
        private String truststorePassword;
        private Map<String, String> additionalProperties;

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
}