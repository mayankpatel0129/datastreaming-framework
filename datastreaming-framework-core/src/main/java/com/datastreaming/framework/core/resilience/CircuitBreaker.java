package com.datastreaming.framework.core.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Circuit breaker implementation for resilient error handling
 */
public class CircuitBreaker {
    
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreaker.class);
    
    public enum State {
        CLOSED,    // Normal operation
        OPEN,      // Circuit is open, rejecting calls
        HALF_OPEN  // Testing if service has recovered
    }
    
    private final String name;
    private final int failureThreshold;
    private final Duration timeout;
    private final Duration retryTimeout;
    
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicReference<Instant> lastFailureTime = new AtomicReference<>();
    
    public CircuitBreaker(String name, int failureThreshold, Duration timeout, Duration retryTimeout) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.timeout = timeout;
        this.retryTimeout = retryTimeout;
        
        logger.debug("Created circuit breaker '{}' with failure threshold: {}, timeout: {}, retry timeout: {}", 
                    name, failureThreshold, timeout, retryTimeout);
    }
    
    /**
     * Execute operation with circuit breaker protection
     */
    public <T> T execute(CircuitBreakerOperation<T> operation) throws CircuitBreakerException {
        if (!allowRequest()) {
            throw new CircuitBreakerException("Circuit breaker '" + name + "' is OPEN");
        }
        
        try {
            T result = operation.execute();
            onSuccess();
            return result;
            
        } catch (Exception e) {
            onFailure();
            throw new CircuitBreakerException("Operation failed in circuit breaker '" + name + "'", e);
        }
    }
    
    /**
     * Execute operation with fallback
     */
    public <T> T executeWithFallback(CircuitBreakerOperation<T> operation, CircuitBreakerOperation<T> fallback) {
        try {
            return execute(operation);
        } catch (CircuitBreakerException e) {
            logger.warn("Circuit breaker '{}' operation failed, executing fallback", name);
            try {
                return fallback.execute();
            } catch (Exception fallbackException) {
                logger.error("Fallback operation also failed for circuit breaker '{}'", name, fallbackException);
                throw new RuntimeException("Both primary and fallback operations failed", fallbackException);
            }
        }
    }
    
    private boolean allowRequest() {
        State currentState = state.get();
        
        switch (currentState) {
            case CLOSED:
                return true;
                
            case OPEN:
                Instant lastFailure = lastFailureTime.get();
                if (lastFailure != null && Duration.between(lastFailure, Instant.now()).compareTo(retryTimeout) >= 0) {
                    // Transition to HALF_OPEN to test the service
                    if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                        logger.info("Circuit breaker '{}' transitioning from OPEN to HALF_OPEN", name);
                        return true;
                    }
                }
                return false;
                
            case HALF_OPEN:
                // Allow one request to test if service has recovered
                return true;
                
            default:
                return false;
        }
    }
    
    private void onSuccess() {
        State currentState = state.get();
        successCount.incrementAndGet();
        
        if (currentState == State.HALF_OPEN) {
            // Service has recovered, close the circuit
            if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                logger.info("Circuit breaker '{}' transitioning from HALF_OPEN to CLOSED - service recovered", name);
                failureCount.set(0);
            }
        } else if (currentState == State.CLOSED) {
            // Reset failure count on successful operation
            failureCount.set(0);
        }
    }
    
    private void onFailure() {
        State currentState = state.get();
        int failures = failureCount.incrementAndGet();
        lastFailureTime.set(Instant.now());
        
        if (currentState == State.HALF_OPEN) {
            // Test failed, reopen the circuit
            if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                logger.warn("Circuit breaker '{}' transitioning from HALF_OPEN to OPEN - service still failing", name);
            }
        } else if (currentState == State.CLOSED && failures >= failureThreshold) {
            // Too many failures, open the circuit
            if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                logger.warn("Circuit breaker '{}' transitioning from CLOSED to OPEN - failure threshold reached: {}", 
                           name, failures);
            }
        }
    }
    
    public State getState() {
        return state.get();
    }
    
    public int getFailureCount() {
        return failureCount.get();
    }
    
    public int getSuccessCount() {
        return successCount.get();
    }
    
    public String getName() {
        return name;
    }
    
    public boolean isOpen() {
        return state.get() == State.OPEN;
    }
    
    public boolean isClosed() {
        return state.get() == State.CLOSED;
    }
    
    public boolean isHalfOpen() {
        return state.get() == State.HALF_OPEN;
    }
    
    /**
     * Functional interface for operations protected by circuit breaker
     */
    @FunctionalInterface
    public interface CircuitBreakerOperation<T> {
        T execute() throws Exception;
    }
    
    /**
     * Exception thrown when circuit breaker prevents operation execution
     */
    public static class CircuitBreakerException extends RuntimeException {
        public CircuitBreakerException(String message) {
            super(message);
        }
        
        public CircuitBreakerException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}