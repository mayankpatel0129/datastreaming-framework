package com.datastreaming.framework.core.consumer;

import com.datastreaming.framework.core.alert.AlertService;
import com.datastreaming.framework.core.resilience.CircuitBreaker;
import com.datastreaming.framework.core.resilience.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.jms.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enhanced MQ Consumer Worker with comprehensive error handling, circuit breakers, and retry policies
 */
public class EnhancedMQConsumerWorker {
    
    private static final Logger logger = LoggerFactory.getLogger(EnhancedMQConsumerWorker.class);
    
    private final String workerId;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicInteger consecutiveErrors = new AtomicInteger(0);
    private final AtomicLong lastSuccessfulMessage = new AtomicLong(System.currentTimeMillis());
    
    // Connection management
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    
    // Resilience components
    private CircuitBreaker mqCircuitBreaker;
    private CircuitBreaker processingCircuitBreaker;
    private RetryPolicy connectionRetryPolicy;
    private RetryPolicy messageProcessingRetryPolicy;
    
    @Autowired(required = false)
    private AlertService alertService;
    
    // Configuration
    private final MQWorkerConfiguration configuration;
    
    public EnhancedMQConsumerWorker(MQWorkerConfiguration configuration) {
        this.workerId = UUID.randomUUID().toString();
        this.configuration = configuration;
        initializeResilienceComponents();
    }
    
    private void initializeResilienceComponents() {
        // Circuit breaker for MQ connection issues
        this.mqCircuitBreaker = new CircuitBreaker(
            "mq-connection-" + workerId,
            configuration.getMqCircuitBreakerFailureThreshold(),
            Duration.ofSeconds(30),
            Duration.ofMinutes(2)
        );
        
        // Circuit breaker for message processing issues
        this.processingCircuitBreaker = new CircuitBreaker(
            "message-processing-" + workerId,
            configuration.getProcessingCircuitBreakerFailureThreshold(),
            Duration.ofSeconds(15),
            Duration.ofMinutes(1)
        );
        
        // Retry policy for connection establishment
        this.connectionRetryPolicy = RetryPolicy.builder("mq-connection-" + workerId)
            .maxAttempts(5)
            .initialDelay(Duration.ofSeconds(1))
            .maxDelay(Duration.ofSeconds(30))
            .backoffMultiplier(2.0)
            .retryCondition(this::isRetryableConnectionException)
            .build();
        
        // Retry policy for message processing
        this.messageProcessingRetryPolicy = RetryPolicy.builder("message-processing-" + workerId)
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(500))
            .maxDelay(Duration.ofSeconds(5))
            .backoffMultiplier(1.5)
            .retryCondition(this::isRetryableProcessingException)
            .build();
    }
    
    public void start() throws Exception {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("Worker already running: {}", workerId);
            return;
        }
        
        logger.info("Starting enhanced MQ consumer worker: {} for queue: {}", 
                   workerId, configuration.getQueueName());
        
        try {
            establishConnection();
            startMessageConsumption();
            
        } catch (Exception e) {
            handleCriticalError("Failed to start MQ consumer worker", e, Map.of(
                "workerId", workerId,
                "queueName", configuration.getQueueName()
            ));
            stop();
            throw e;
        }
    }
    
    private void establishConnection() throws Exception {
        connectionRetryPolicy.execute(() -> {
            return mqCircuitBreaker.execute(() -> {
                setupJMSConnection();
                return null;
            });
        });
    }
    
    private void setupJMSConnection() throws JMSException {
        try {
            // Close existing connections if any
            closeConnections();
            
            // Create new connection
            ConnectionFactory factory = configuration.createConnectionFactory();
            connection = factory.createConnection(
                configuration.getUserId(), 
                configuration.getPassword()
            );
            
            connection.setExceptionListener(this::onConnectionException);
            connection.start();
            
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(configuration.getQueueName());
            consumer = session.createConsumer(queue);
            
            logger.info("MQ connection established successfully for worker: {}", workerId);
            
            // Reset error counters on successful connection
            consecutiveErrors.set(0);
            
        } catch (JMSException e) {
            logger.error("Failed to establish MQ connection for worker: {}", workerId, e);
            throw e;
        }
    }
    
    private void startMessageConsumption() {
        Thread consumptionThread = new Thread(this::consumeMessages, "mq-consumer-" + workerId);
        consumptionThread.setDaemon(true);
        consumptionThread.start();
    }
    
    private void consumeMessages() {
        logger.info("Message consumption started for worker: {}", workerId);
        
        while (isRunning.get()) {
            try {
                // Check if we should continue processing
                if (!shouldContinueProcessing()) {
                    Thread.sleep(5000); // Wait before retrying
                    continue;
                }
                
                // Consume message with timeout
                Message message = consumer.receive(configuration.getReceiveTimeoutMs());
                
                if (message != null) {
                    processMessageWithResilience(message);
                } else {
                    // No message received, continue polling
                    Thread.sleep(100);
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Message consumption interrupted for worker: {}", workerId);
                break;
                
            } catch (JMSException e) {
                handleConnectionError("JMS error during message consumption", e);
                
            } catch (Exception e) {
                handleUnexpectedError("Unexpected error during message consumption", e);
            }
        }
        
        logger.info("Message consumption stopped for worker: {}", workerId);
    }
    
    private void processMessageWithResilience(Message message) {
        String correlationId = null;
        
        try {
            correlationId = extractCorrelationId(message);
            final String finalCorrelationId = correlationId;
            
            // Process message with circuit breaker and retry policy
            messageProcessingRetryPolicy.execute(() -> {
                return processingCircuitBreaker.execute(() -> {
                    processMessage(message, finalCorrelationId);
                    return null;
                });
            });
            
            // Acknowledge message only after successful processing
            message.acknowledge();
            
            // Update success metrics
            messagesProcessed.incrementAndGet();
            lastSuccessfulMessage.set(System.currentTimeMillis());
            consecutiveErrors.set(0);
            
            logger.debug("Message processed successfully: {}", correlationId);
            
        } catch (Exception e) {
            int errorCount = consecutiveErrors.incrementAndGet();
            
            logger.error("Failed to process message: {} (consecutive errors: {})", 
                        correlationId, errorCount, e);
            
            // Send alert for processing errors
            if (alertService != null) {
                Map<String, Object> context = Map.of(
                    "workerId", workerId,
                    "correlationId", correlationId != null ? correlationId : "unknown",
                    "consecutiveErrors", errorCount,
                    "queueName", configuration.getQueueName()
                );
                
                if (errorCount >= configuration.getCriticalErrorThreshold()) {
                    alertService.sendCriticalAlert("MQConsumerWorker", 
                        "Critical Message Processing Errors", 
                        "Worker has " + errorCount + " consecutive processing errors", 
                        context, e);
                } else {
                    alertService.sendHighAlert("MQConsumerWorker", 
                        "Message Processing Error", 
                        "Failed to process message: " + e.getMessage(), 
                        context, e);
                }
            }
            
            // Decide whether to acknowledge the message based on error type
            handleMessageProcessingError(message, e);
        }
    }
    
    private void processMessage(Message message, String correlationId) throws Exception {
        // This would be implemented by the concrete consumer implementation
        // For now, this is a placeholder for the actual message processing logic
        throw new UnsupportedOperationException("processMessage must be implemented by subclass");
    }
    
    private void handleMessageProcessingError(Message message, Exception error) {
        try {
            if (isPoisonMessage(error)) {
                // Acknowledge poison messages to prevent infinite reprocessing
                logger.warn("Acknowledging poison message to prevent reprocessing: {}", error.getMessage());
                message.acknowledge();
            } else {
                // For retryable errors, don't acknowledge so message can be redelivered
                logger.info("Not acknowledging message due to retryable error, will be redelivered");
                // Could implement dead letter queue logic here
            }
        } catch (JMSException e) {
            logger.error("Failed to handle message processing error", e);
        }
    }
    
    private boolean isPoisonMessage(Exception error) {
        // Logic to determine if a message is poison (non-retryable)
        String errorMessage = error.getMessage();
        return errorMessage != null && (
            errorMessage.contains("parsing") ||
            errorMessage.contains("validation") ||
            errorMessage.contains("format") ||
            error instanceof IllegalArgumentException
        );
    }
    
    private boolean shouldContinueProcessing() {
        // Check circuit breaker states
        if (mqCircuitBreaker.isOpen() || processingCircuitBreaker.isOpen()) {
            logger.warn("Circuit breakers open for worker: {} (MQ: {}, Processing: {})", 
                       workerId, mqCircuitBreaker.getState(), processingCircuitBreaker.getState());
            return false;
        }
        
        // Check if too many consecutive errors
        if (consecutiveErrors.get() >= configuration.getMaxConsecutiveErrors()) {
            logger.warn("Too many consecutive errors for worker: {} ({})", 
                       workerId, consecutiveErrors.get());
            return false;
        }
        
        // Check if worker has been unresponsive for too long
        long timeSinceLastSuccess = System.currentTimeMillis() - lastSuccessfulMessage.get();
        if (timeSinceLastSuccess > configuration.getMaxUnresponsiveTimeMs()) {
            logger.warn("Worker has been unresponsive for {}ms: {}", timeSinceLastSuccess, workerId);
            return false;
        }
        
        return true;
    }
    
    private void onConnectionException(JMSException exception) {
        logger.error("JMS connection exception for worker: {}", workerId, exception);
        
        if (alertService != null) {
            alertService.sendHighAlert("MQConsumerWorker", 
                "MQ Connection Exception", 
                "JMS connection lost: " + exception.getMessage(), 
                Map.of("workerId", workerId), exception);
        }
        
        // Try to reconnect
        scheduleReconnection();
    }
    
    private void scheduleReconnection() {
        Thread reconnectThread = new Thread(() -> {
            try {
                Thread.sleep(5000); // Wait before reconnecting
                if (isRunning.get()) {
                    logger.info("Attempting to reconnect worker: {}", workerId);
                    establishConnection();
                }
            } catch (Exception e) {
                logger.error("Failed to reconnect worker: {}", workerId, e);
            }
        }, "reconnect-" + workerId);
        
        reconnectThread.setDaemon(true);
        reconnectThread.start();
    }
    
    private void handleConnectionError(String message, Exception cause) {
        logger.error("{} for worker: {}", message, workerId, cause);
        
        if (alertService != null) {
            alertService.sendHighAlert("MQConsumerWorker", 
                "MQ Connection Error", 
                message + ": " + cause.getMessage(), 
                Map.of("workerId", workerId), cause);
        }
        
        // Increment error counter and potentially trigger circuit breaker
        consecutiveErrors.incrementAndGet();
    }
    
    private void handleCriticalError(String message, Exception cause, Map<String, Object> context) {
        logger.error("{} for worker: {}", message, workerId, cause);
        
        if (alertService != null) {
            alertService.sendCriticalAlert("MQConsumerWorker", 
                "Critical MQ Consumer Error", 
                message + ": " + cause.getMessage(), 
                context, cause);
        }
    }
    
    private void handleUnexpectedError(String message, Exception cause) {
        logger.error("{} for worker: {}", message, workerId, cause);
        
        if (alertService != null) {
            alertService.sendHighAlert("MQConsumerWorker", 
                "Unexpected Error", 
                message + ": " + cause.getMessage(), 
                Map.of("workerId", workerId), cause);
        }
        
        // For unexpected errors, add a longer delay before retrying
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private String extractCorrelationId(Message message) throws JMSException {
        String messageId = message.getJMSMessageID();
        if (messageId != null) {
            return messageId;
        }
        
        String correlationId = message.getJMSCorrelationID();
        if (correlationId != null) {
            return correlationId;
        }
        
        return workerId + "-" + System.currentTimeMillis() + "-" + messagesProcessed.get();
    }
    
    private boolean isRetryableConnectionException(Throwable throwable) {
        if (throwable instanceof JMSException) {
            String message = throwable.getMessage();
            return message != null && (
                message.contains("Connection refused") ||
                message.contains("timeout") ||
                message.contains("network") ||
                message.contains("temporarily unavailable")
            );
        }
        return false;
    }
    
    private boolean isRetryableProcessingException(Throwable throwable) {
        // Don't retry parsing or validation errors
        if (throwable instanceof IllegalArgumentException ||
            throwable.getMessage() != null && (
                throwable.getMessage().contains("parsing") ||
                throwable.getMessage().contains("validation")
            )) {
            return false;
        }
        
        // Retry network and temporary errors
        return true;
    }
    
    public void stop() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }
        
        logger.info("Stopping enhanced MQ consumer worker: {}", workerId);
        
        try {
            closeConnections();
        } catch (Exception e) {
            logger.error("Error stopping worker: {}", workerId, e);
        }
        
        logger.info("Enhanced MQ consumer worker stopped: {}", workerId);
    }
    
    private void closeConnections() {
        try {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        } catch (Exception e) {
            logger.warn("Error closing consumer for worker: {}", workerId, e);
        }
        
        try {
            if (session != null) {
                session.close();
                session = null;
            }
        } catch (Exception e) {
            logger.warn("Error closing session for worker: {}", workerId, e);
        }
        
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (Exception e) {
            logger.warn("Error closing connection for worker: {}", workerId, e);
        }
    }
    
    // Getters for monitoring
    public String getWorkerId() {
        return workerId;
    }
    
    public long getMessagesProcessed() {
        return messagesProcessed.get();
    }
    
    public boolean isRunning() {
        return isRunning.get();
    }
    
    public boolean isHealthy() {
        if (!isRunning.get()) {
            return false;
        }
        
        // Check circuit breaker states
        if (mqCircuitBreaker.isOpen() || processingCircuitBreaker.isOpen()) {
            return false;
        }
        
        // Check consecutive errors
        if (consecutiveErrors.get() >= configuration.getCriticalErrorThreshold()) {
            return false;
        }
        
        // Check responsiveness
        long timeSinceLastSuccess = System.currentTimeMillis() - lastSuccessfulMessage.get();
        if (timeSinceLastSuccess > configuration.getMaxUnresponsiveTimeMs()) {
            return false;
        }
        
        return true;
    }
    
    public WorkerStats getStats() {
        return new WorkerStats(
            workerId,
            messagesProcessed.get(),
            consecutiveErrors.get(),
            mqCircuitBreaker.getState().toString(),
            processingCircuitBreaker.getState().toString(),
            isHealthy(),
            Instant.ofEpochMilli(lastSuccessfulMessage.get())
        );
    }
    
    /**
     * Configuration class for MQ Worker
     */
    public static class MQWorkerConfiguration {
        private String queueName;
        private String userId;
        private String password;
        private long receiveTimeoutMs = 5000;
        private int mqCircuitBreakerFailureThreshold = 5;
        private int processingCircuitBreakerFailureThreshold = 10;
        private int maxConsecutiveErrors = 20;
        private int criticalErrorThreshold = 50;
        private long maxUnresponsiveTimeMs = 300000; // 5 minutes
        
        // Constructor and getters/setters
        public String getQueueName() { 
            return queueName; 
        }
        
        public void setQueueName(String queueName) { 
            this.queueName = queueName; 
        }
        
        public String getUserId() { 
            return userId; 
        }
        
        public void setUserId(String userId) { 
            this.userId = userId; 
        }
        
        public String getPassword() { 
            return password; 
        }
        
        public void setPassword(String password) { 
            this.password = password; 
        }
        
        public long getReceiveTimeoutMs() { 
            return receiveTimeoutMs; 
        }
        
        public void setReceiveTimeoutMs(long receiveTimeoutMs) { 
            this.receiveTimeoutMs = receiveTimeoutMs; 
        }
        
        public int getMqCircuitBreakerFailureThreshold() { 
            return mqCircuitBreakerFailureThreshold; 
        }
        
        public void setMqCircuitBreakerFailureThreshold(int threshold) { 
            this.mqCircuitBreakerFailureThreshold = threshold; 
        }
        
        public int getProcessingCircuitBreakerFailureThreshold() { 
            return processingCircuitBreakerFailureThreshold; 
        }
        
        public void setProcessingCircuitBreakerFailureThreshold(int threshold) { 
            this.processingCircuitBreakerFailureThreshold = threshold; 
        }
        
        public int getMaxConsecutiveErrors() { 
            return maxConsecutiveErrors; 
        }
        
        public void setMaxConsecutiveErrors(int maxConsecutiveErrors) { 
            this.maxConsecutiveErrors = maxConsecutiveErrors; 
        }
        
        public int getCriticalErrorThreshold() { 
            return criticalErrorThreshold; 
        }
        
        public void setCriticalErrorThreshold(int criticalErrorThreshold) { 
            this.criticalErrorThreshold = criticalErrorThreshold; 
        }
        
        public long getMaxUnresponsiveTimeMs() { 
            return maxUnresponsiveTimeMs; 
        }
        
        public void setMaxUnresponsiveTimeMs(long maxUnresponsiveTimeMs) { 
            this.maxUnresponsiveTimeMs = maxUnresponsiveTimeMs; 
        }
        
        public ConnectionFactory createConnectionFactory() throws JMSException {
            // This would be implemented based on the specific MQ provider
            throw new UnsupportedOperationException("createConnectionFactory must be implemented");
        }
    }
    
    /**
     * Worker statistics for monitoring
     */
    public static class WorkerStats {
        private final String workerId;
        private final long messagesProcessed;
        private final int consecutiveErrors;
        private final String mqCircuitBreakerState;
        private final String processingCircuitBreakerState;
        private final boolean healthy;
        private final Instant lastSuccessfulMessage;
        
        public WorkerStats(String workerId, long messagesProcessed, int consecutiveErrors,
                          String mqCircuitBreakerState, String processingCircuitBreakerState,
                          boolean healthy, Instant lastSuccessfulMessage) {
            this.workerId = workerId;
            this.messagesProcessed = messagesProcessed;
            this.consecutiveErrors = consecutiveErrors;
            this.mqCircuitBreakerState = mqCircuitBreakerState;
            this.processingCircuitBreakerState = processingCircuitBreakerState;
            this.healthy = healthy;
            this.lastSuccessfulMessage = lastSuccessfulMessage;
        }
        
        // Getters
        public String getWorkerId() { 
            return workerId; 
        }
        
        public long getMessagesProcessed() { 
            return messagesProcessed; 
        }
        
        public int getConsecutiveErrors() { 
            return consecutiveErrors; 
        }
        
        public String getMqCircuitBreakerState() { 
            return mqCircuitBreakerState; 
        }
        
        public String getProcessingCircuitBreakerState() { 
            return processingCircuitBreakerState; 
        }
        
        public boolean isHealthy() { 
            return healthy; 
        }
        
        public Instant getLastSuccessfulMessage() { 
            return lastSuccessfulMessage; 
        }
    }
}