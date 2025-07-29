package com.datastreaming.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.List;
import java.util.Map;

public class ProcessorProfile {

    @NotBlank
    @JsonProperty("profileName")
    private String profileName;

    @NotEmpty
    @Valid
    @JsonProperty("mqConfigurations")
    private List<MQConfiguration> mqConfigurations;

    @NotNull
    @Valid
    @JsonProperty("kafkaConfiguration")
    private KafkaConfiguration kafkaConfiguration;

    @NotNull
    @Valid
    @JsonProperty("reconciliationConfiguration")
    private ReconciliationConfiguration reconciliationConfiguration;

    @NotNull
    @Valid
    @JsonProperty("performanceConfiguration")
    private PerformanceConfiguration performanceConfiguration;

    public static class MQConfiguration {
        @NotBlank
        @JsonProperty("queueManagerName")
        private String queueManagerName;

        @NotBlank
        @JsonProperty("hostname")
        private String hostname;

        @Positive
        @JsonProperty("port")
        private int port;

        @NotBlank
        @JsonProperty("channel")
        private String channel;

        @NotBlank
        @JsonProperty("queueName")
        private String queueName;

        @NotBlank
        @JsonProperty("userId")
        private String userId;

        @JsonProperty("password")
        private String password;

        @NotBlank
        @JsonProperty("messageContract")
        private String messageContract;

        @NotBlank
        @JsonProperty("targetKafkaTopic")
        private String targetKafkaTopic;

        @Positive
        @JsonProperty("consumerThreads")
        private int consumerThreads = 5;

        @Positive
        @JsonProperty("maxBatchSize")
        private int maxBatchSize = 100;

        @JsonProperty("additionalProperties")
        private Map<String, String> additionalProperties;

        // Getters and setters
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
        public String getMessageContract() { return messageContract; }
        public void setMessageContract(String messageContract) { this.messageContract = messageContract; }
        public String getTargetKafkaTopic() { return targetKafkaTopic; }
        public void setTargetKafkaTopic(String targetKafkaTopic) { this.targetKafkaTopic = targetKafkaTopic; }
        public int getConsumerThreads() { return consumerThreads; }
        public void setConsumerThreads(int consumerThreads) { this.consumerThreads = consumerThreads; }
        public int getMaxBatchSize() { return maxBatchSize; }
        public void setMaxBatchSize(int maxBatchSize) { this.maxBatchSize = maxBatchSize; }
        public Map<String, String> getAdditionalProperties() { return additionalProperties; }
        public void setAdditionalProperties(Map<String, String> additionalProperties) { this.additionalProperties = additionalProperties; }
    }

    public static class KafkaConfiguration {
        @NotBlank
        @JsonProperty("bootstrapServers")
        private String bootstrapServers;

        @NotBlank
        @JsonProperty("securityProtocol")
        private String securityProtocol = "PLAINTEXT";

        @JsonProperty("saslMechanism")
        private String saslMechanism;

        @JsonProperty("saslJaasConfig")
        private String saslJaasConfig;

        @Positive
        @JsonProperty("batchSize")
        private int batchSize = 65536;

        @Positive
        @JsonProperty("lingerMs")
        private int lingerMs = 10;

        @Positive
        @JsonProperty("bufferMemory")
        private long bufferMemory = 134217728L;

        @JsonProperty("compressionType")
        private String compressionType = "lz4";

        @Positive
        @JsonProperty("maxInFlightRequestsPerConnection")
        private int maxInFlightRequestsPerConnection = 5;

        @JsonProperty("additionalProperties")
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
        public Map<String, String> getAdditionalProperties() { return additionalProperties; }
        public void setAdditionalProperties(Map<String, String> additionalProperties) { this.additionalProperties = additionalProperties; }
    }

    public static class ReconciliationConfiguration {
        @Positive
        @JsonProperty("reconciliationIntervalMs")
        private long reconciliationIntervalMs = 30000L;

        @Positive
        @JsonProperty("maxRetryAttempts")
        private int maxRetryAttempts = 3;

        @Positive
        @JsonProperty("retryBackoffMs")
        private long retryBackoffMs = 5000L;

        @JsonProperty("enableDeadLetterQueue")
        private boolean enableDeadLetterQueue = true;

        @JsonProperty("deadLetterTopic")
        private String deadLetterTopic = "transaction-dlq";

        // Getters and setters
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

    public static class PerformanceConfiguration {
        @Positive
        @JsonProperty("globalThreadPoolSize")
        private int globalThreadPoolSize = 20;

        @Positive
        @JsonProperty("queueCapacity")
        private int queueCapacity = 10000;

        @Positive
        @JsonProperty("keepAliveTimeMs")
        private long keepAliveTimeMs = 60000L;

        @JsonProperty("enableBackpressure")
        private boolean enableBackpressure = true;

        @Positive
        @JsonProperty("backpressureThreshold")
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

    // Main class getters and setters
    public String getProfileName() { return profileName; }
    public void setProfileName(String profileName) { this.profileName = profileName; }
    public List<MQConfiguration> getMqConfigurations() { return mqConfigurations; }
    public void setMqConfigurations(List<MQConfiguration> mqConfigurations) { this.mqConfigurations = mqConfigurations; }
    public KafkaConfiguration getKafkaConfiguration() { return kafkaConfiguration; }
    public void setKafkaConfiguration(KafkaConfiguration kafkaConfiguration) { this.kafkaConfiguration = kafkaConfiguration; }
    public ReconciliationConfiguration getReconciliationConfiguration() { return reconciliationConfiguration; }
    public void setReconciliationConfiguration(ReconciliationConfiguration reconciliationConfiguration) { this.reconciliationConfiguration = reconciliationConfiguration; }
    public PerformanceConfiguration getPerformanceConfiguration() { return performanceConfiguration; }
    public void setPerformanceConfiguration(PerformanceConfiguration performanceConfiguration) { this.performanceConfiguration = performanceConfiguration; }
}