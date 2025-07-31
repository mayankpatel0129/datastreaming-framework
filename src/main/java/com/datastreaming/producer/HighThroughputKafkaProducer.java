package com.datastreaming.producer;

import com.datastreaming.config.ProcessorProfile;
import com.datastreaming.monitoring.MetricsCollector;
import com.datastreaming.reconciliation.MessageTracker;
import com.datastreaming.framework.core.alert.AlertService;
import com.datastreaming.framework.core.resilience.CircuitBreaker;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class HighThroughputKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(HighThroughputKafkaProducer.class);

    @Autowired
    private MetricsCollector metricsCollector;

    @Autowired
    private MessageTracker messageTracker;
    
    @Autowired(required = false)
    private AlertService alertService;
    
    private CircuitBreaker kafkaCircuitBreaker;

    private KafkaProducer<String, String> producer;
    private ExecutorService callbackExecutor;
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final AtomicLong totalMessagesSent = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong pendingMessages = new AtomicLong(0);

    private ProcessorProfile.KafkaConfiguration kafkaConfig;

    public void initialize(ProcessorProfile.KafkaConfiguration kafkaConfig) {
        if (isInitialized.compareAndSet(false, true)) {
            this.kafkaConfig = kafkaConfig;
            createProducer();
            createCallbackExecutor();
            createCircuitBreaker();
            logger.info("High throughput Kafka producer initialized");
        }
    }
    
    private void createCircuitBreaker() {
        this.kafkaCircuitBreaker = new CircuitBreaker(
            "kafka-producer",
            5, // failure threshold
            java.time.Duration.ofSeconds(30), // timeout
            java.time.Duration.ofMinutes(1) // retry timeout
        );
    }

    private void createProducer() {
        Properties props = new Properties();
        
        // Basic configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // High throughput optimizations
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaConfig.getBatchSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaConfig.getLingerMs());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaConfig.getBufferMemory());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaConfig.getCompressionType());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, kafkaConfig.getMaxInFlightRequestsPerConnection());
        
        // Reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        // Idempotence for exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Security configuration
        if (!"PLAINTEXT".equals(kafkaConfig.getSecurityProtocol())) {
            props.put("security.protocol", kafkaConfig.getSecurityProtocol());
            if (kafkaConfig.getSaslMechanism() != null) {
                props.put("sasl.mechanism", kafkaConfig.getSaslMechanism());
                props.put("sasl.jaas.config", kafkaConfig.getSaslJaasConfig());
            }
        }
        
        // Additional custom properties
        if (kafkaConfig.getAdditionalProperties() != null) {
            props.putAll(kafkaConfig.getAdditionalProperties());
        }

        this.producer = new KafkaProducer<>(props);
        logger.info("Kafka producer created with high throughput configuration");
    }

    private void createCallbackExecutor() {
        this.callbackExecutor = new ThreadPoolExecutor(
                5, 20, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10000),
                r -> {
                    Thread t = new Thread(r, "kafka-callback-thread");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public CompletableFuture<SendResult> sendAsync(String topic, String key, String message, String correlationId) {
        if (!isInitialized.get()) {
            throw new IllegalStateException("Producer not initialized");
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        
        // Add headers for tracing and correlation
        record.headers().add("correlationId", correlationId.getBytes());
        record.headers().add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
        
        CompletableFuture<SendResult> future = new CompletableFuture<>();
        pendingMessages.incrementAndGet();

        try {
            producer.send(record, new ProducerCallback(topic, correlationId, future));
            metricsCollector.recordKafkaMessageSent(topic);
            
        } catch (Exception e) {
            pendingMessages.decrementAndGet();
            totalErrors.incrementAndGet();
            future.completeExceptionally(e);
            
            handleKafkaError("Failed to send message to topic: " + topic, e, Map.of(
                "topic", topic,
                "correlationId", correlationId,
                "messageSize", message.length()
            ));
        }

        return future;
    }

    public void sendSync(String topic, String key, String message, String correlationId) 
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<SendResult> future = sendAsync(topic, key, message, correlationId);
        future.get(30, TimeUnit.SECONDS); // Configurable timeout
    }

    public CompletableFuture<Void> sendBatchAsync(String topic, java.util.List<MessageBatch.Message> messages) {
        if (!isInitialized.get()) {
            throw new IllegalStateException("Producer not initialized");
        }

        CompletableFuture<Void> batchFuture = new CompletableFuture<>();
        AtomicLong remainingMessages = new AtomicLong(messages.size());
        AtomicBoolean hasError = new AtomicBoolean(false);

        for (MessageBatch.Message msg : messages) {
            sendAsync(topic, msg.getKey(), msg.getValue(), msg.getCorrelationId())
                    .whenComplete((result, throwable) -> {
                        if (throwable != null && hasError.compareAndSet(false, true)) {
                            batchFuture.completeExceptionally(throwable);
                        } else if (remainingMessages.decrementAndGet() == 0 && !hasError.get()) {
                            batchFuture.complete(null);
                        }
                    });
        }

        return batchFuture;
    }

    public void flush() {
        if (producer != null) {
            producer.flush();
        }
    }

    public ProducerStats getStats() {
        return new ProducerStats(
                totalMessagesSent.get(),
                totalErrors.get(),
                pendingMessages.get(),
                isInitialized.get()
        );
    }

    public boolean isHealthy() {
        if (!isInitialized.get() || producer == null) {
            return false;
        }
        
        // Check pending messages threshold
        long pending = pendingMessages.get();
        if (pending > 50000) { // Configurable threshold
            return false;
        }
        
        // Check circuit breaker state
        if (kafkaCircuitBreaker != null && kafkaCircuitBreaker.isOpen()) {
            return false;
        }
        
        // Check error rate
        long total = totalMessagesSent.get() + totalErrors.get();
        if (total > 100) { // Only check after significant traffic
            double errorRate = (double) totalErrors.get() / total;
            if (errorRate > 0.05) { // More than 5% error rate
                return false;
            }
        }
        
        return true;
    }

    @PreDestroy
    public void shutdown() {
        if (isInitialized.compareAndSet(true, false)) {
            logger.info("Shutting down Kafka producer");
            
            try {
                if (producer != null) {
                    producer.flush();
                    producer.close(30, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                logger.error("Error closing Kafka producer", e);
            }

            if (callbackExecutor != null && !callbackExecutor.isShutdown()) {
                callbackExecutor.shutdown();
                try {
                    if (!callbackExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                        callbackExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    callbackExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            
            logger.info("Kafka producer shutdown completed");
        }
    }
    
    private void handleKafkaError(String message, Throwable cause, Map<String, Object> context) {
        logger.error(message, cause);
        
        if (alertService != null) {
            // Determine alert level based on error characteristics
            boolean isCritical = isCriticalError(cause);
            
            if (isCritical) {
                alertService.sendCriticalAlert("KafkaProducer", "Critical Kafka Error", message, context, cause);
            } else {
                alertService.sendHighAlert("KafkaProducer", "Kafka Error", message, context, cause);
            }
        }
    }
    
    private boolean isCriticalError(Throwable cause) {
        if (cause == null) return false;
        
        String errorMessage = cause.getMessage();
        String errorClass = cause.getClass().getSimpleName();
        
        // Critical errors that indicate serious system issues
        return errorMessage != null && (
            errorMessage.contains("Authentication failed") ||
            errorMessage.contains("SSL") ||
            errorMessage.contains("Connection refused") ||
            errorMessage.contains("Timeout") ||
            errorClass.contains("SecurityException") ||
            errorClass.contains("AuthenticationException")
        );
    }

    private class ProducerCallback implements Callback {
        private final String topic;
        private final String correlationId;
        private final CompletableFuture<SendResult> future;

        public ProducerCallback(String topic, String correlationId, CompletableFuture<SendResult> future) {
            this.topic = topic;
            this.correlationId = correlationId;
            this.future = future;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            callbackExecutor.submit(() -> {
                try {
                    pendingMessages.decrementAndGet();

                    if (exception != null) {
                        totalErrors.incrementAndGet();
                        metricsCollector.recordKafkaError(topic);
                        messageTracker.recordFailure(correlationId, exception.getMessage());
                        future.completeExceptionally(exception);
                        
                        handleKafkaError("Kafka callback failure for topic: " + topic, exception, Map.of(
                            "topic", topic,
                            "correlationId", correlationId,
                            "callbackError", true
                        ));
                    } else {
                        totalMessagesSent.incrementAndGet();
                        metricsCollector.recordKafkaSuccess(topic, metadata.partition(), metadata.offset());
                        messageTracker.recordSuccess(correlationId, metadata);
                        
                        SendResult result = new SendResult(metadata, correlationId);
                        future.complete(result);
                        
                        logger.debug("Message sent successfully to topic: {}, partition: {}, offset: {}, correlationId: {}", 
                                   topic, metadata.partition(), metadata.offset(), correlationId);
                    }
                } catch (Exception e) {
                    handleKafkaError("Critical error in producer callback", e, Map.of(
                        "topic", topic,
                        "correlationId", correlationId,
                        "criticalError", true
                    ));
                }
            });
        }
    }

    public static class SendResult {
        private final RecordMetadata metadata;
        private final String correlationId;

        public SendResult(RecordMetadata metadata, String correlationId) {
            this.metadata = metadata;
            this.correlationId = correlationId;
        }

        public RecordMetadata getMetadata() { return metadata; }
        public String getCorrelationId() { return correlationId; }
    }

    public static class ProducerStats {
        private final long totalMessagesSent;
        private final long totalErrors;
        private final long pendingMessages;
        private final boolean isHealthy;

        public ProducerStats(long totalMessagesSent, long totalErrors, long pendingMessages, boolean isHealthy) {
            this.totalMessagesSent = totalMessagesSent;
            this.totalErrors = totalErrors;
            this.pendingMessages = pendingMessages;
            this.isHealthy = isHealthy;
        }

        public long getTotalMessagesSent() { return totalMessagesSent; }
        public long getTotalErrors() { return totalErrors; }
        public long getPendingMessages() { return pendingMessages; }
        public boolean isHealthy() { return isHealthy; }
    }
}