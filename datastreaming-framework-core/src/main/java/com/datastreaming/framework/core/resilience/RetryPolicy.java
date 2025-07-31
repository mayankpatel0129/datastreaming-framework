package com.datastreaming.framework.core.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Predicate;

/**
 * Retry policy implementation with exponential backoff
 */
public class RetryPolicy {
    
    private static final Logger logger = LoggerFactory.getLogger(RetryPolicy.class);
    
    private final String name;
    private final int maxAttempts;
    private final Duration initialDelay;
    private final Duration maxDelay;
    private final double backoffMultiplier;
    private final Predicate<Throwable> retryCondition;
    
    private RetryPolicy(Builder builder) {
        this.name = builder.name;
        this.maxAttempts = builder.maxAttempts;
        this.initialDelay = builder.initialDelay;
        this.maxDelay = builder.maxDelay;
        this.backoffMultiplier = builder.backoffMultiplier;
        this.retryCondition = builder.retryCondition;
    }
    
    /**
     * Execute operation with retry policy
     */
    public <T> T execute(RetryableOperation<T> operation) throws Exception {
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                T result = operation.execute();
                if (attempt > 1) {
                    logger.info("Operation '{}' succeeded on attempt {}/{}", name, attempt, maxAttempts);
                }
                return result;
                
            } catch (Exception e) {
                lastException = e;
                
                if (attempt == maxAttempts) {
                    logger.error("Operation '{}' failed after {} attempts", name, maxAttempts, e);
                    break;
                }
                
                if (!retryCondition.test(e)) {
                    logger.warn("Operation '{}' failed with non-retryable exception on attempt {}", name, attempt, e);
                    break;
                }
                
                Duration delay = calculateDelay(attempt);
                logger.warn("Operation '{}' failed on attempt {}/{}, retrying in {}ms", 
                           name, attempt, maxAttempts, delay.toMillis(), e);
                
                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }
            }
        }
        
        throw new RetryExhaustedException("Retry exhausted for operation '" + name + "'", lastException);
    }
    
    private Duration calculateDelay(int attempt) {
        long delayMs = (long) (initialDelay.toMillis() * Math.pow(backoffMultiplier, attempt - 1));
        return Duration.ofMillis(Math.min(delayMs, maxDelay.toMillis()));
    }
    
    public static Builder builder(String name) {
        return new Builder(name);
    }
    
    public static class Builder {
        private final String name;
        private int maxAttempts = 3;
        private Duration initialDelay = Duration.ofMillis(1000);
        private Duration maxDelay = Duration.ofSeconds(30);
        private double backoffMultiplier = 2.0;
        private Predicate<Throwable> retryCondition = throwable -> true;
        
        public Builder(String name) {
            this.name = name;
        }
        
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }
        
        public Builder initialDelay(Duration initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }
        
        public Builder maxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
            return this;
        }
        
        public Builder backoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
            return this;
        }
        
        public Builder retryCondition(Predicate<Throwable> retryCondition) {
            this.retryCondition = retryCondition;
            return this;
        }
        
        public RetryPolicy build() {
            return new RetryPolicy(this);
        }
    }
    
    @FunctionalInterface
    public interface RetryableOperation<T> {
        T execute() throws Exception;
    }
    
    public static class RetryExhaustedException extends RuntimeException {
        public RetryExhaustedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}