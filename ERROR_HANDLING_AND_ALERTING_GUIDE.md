# Error Handling and Alerting Guide

This guide covers the comprehensive error handling and alerting system implemented in the Data Streaming Framework, providing enterprise-grade reliability and operational visibility.

## 🚨 Overview

The framework includes a multi-layered error handling and alerting system designed to:

- **Detect and categorize errors** at different levels (CRITICAL, HIGH, WARNING, INFO)
- **Implement resilience patterns** including circuit breakers and retry policies
- **Send intelligent alerts** through multiple channels (logs, Teams, email, PagerDuty)
- **Provide operational visibility** through comprehensive health checks and metrics
- **Enable graceful degradation** and automatic recovery

## 📋 Key Components

### 1. Alert System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Application Components                      │
├─────────────────────────────────────────────────────────────────┤
│  MQ Consumer  │  Kafka Producer  │  Message Parser  │  Tracker  │
│   Workers     │     Service      │    Service       │ Service   │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Alert Service                               │
├─────────────────────────────────────────────────────────────────┤
│  • Centralized alert processing                                │
│  • Rate limiting and deduplication                             │
│  • Context enrichment                                          │
│  • Alert level management                                      │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Alert Notifiers                              │
├─────────────────────────────────────────────────────────────────┤
│ Log Notifier │ Teams Notifier │ Email Notifier │ PagerDuty     │
│ (Always On)  │ (HIGH+)        │ (CRITICAL)     │ (CRITICAL)    │
└─────────────────────────────────────────────────────────────────┘
```

### 2. Resilience Patterns

- **Circuit Breakers**: Prevent cascading failures by opening circuits when error thresholds are exceeded
- **Retry Policies**: Intelligent retry with exponential backoff for transient failures
- **Timeout Handling**: Configurable timeouts for all external service calls
- **Graceful Degradation**: Fallback mechanisms when primary services are unavailable

## ⚙️ Configuration

### Basic Alert Configuration

```yaml
datastreaming:
  alerts:
    enabled: true
    rateLimitMinutes: 5          # Prevent spam alerts
    maxAlertsPerHour: 100        # Rate limiting
    
    notifiers:
      log:
        enabled: true            # Always enabled for audit trail
      
      teams:
        enabled: true
        webhookUrl: "${TEAMS_WEBHOOK_URL}"
        title: "Data Streaming Framework Alert"
        minLevel: "HIGH"         # Only HIGH and CRITICAL to Teams
```

### Circuit Breaker Configuration

```yaml
datastreaming:
  resilience:
    circuitBreaker:
      enabled: true
      failureThreshold: 5        # Open after 5 failures
      timeout: 30s               # Circuit timeout
      retryTimeout: 60s          # Wait before retry
      
    retry:
      enabled: true
      maxAttempts: 3             # Maximum retry attempts
      initialDelay: 1s           # Initial retry delay
      maxDelay: 30s              # Maximum retry delay
      backoffMultiplier: 2.0     # Exponential backoff
```

### Environment-Specific Configuration

**Production (application-prod.yml):**
```yaml
datastreaming:
  alerts:
    notifiers:
      teams:
        enabled: true
        webhookUrl: "${TEAMS_PROD_WEBHOOK}"
        minLevel: "HIGH"
      
      email:
        enabled: true
        to: ["ops-team@company.com", "on-call@company.com"]
        minLevel: "CRITICAL"
      
      pagerduty:
        enabled: true
        integrationKey: "${PAGERDUTY_KEY}"
        minLevel: "CRITICAL"
```

**Development (application-dev.yml):**
```yaml
datastreaming:
  alerts:
    notifiers:
      teams:
        enabled: true
        title: "Dev Environment Alert"
        minLevel: "WARNING"        # More verbose in dev
```

## 🚨 Alert Levels and Triggers

### CRITICAL Alerts
**When Triggered:**
- System-wide failures (all workers down)
- Authentication/Security failures
- Circuit breakers open for critical services
- Data corruption or parsing failures
- Resource exhaustion (memory, disk)

**Example:**
```java
alertService.sendCriticalAlert(
    "KafkaProducer",
    "Kafka Authentication Failed", 
    "Unable to authenticate with Kafka cluster",
    Map.of("cluster", "prod-kafka", "errorCode", "SASL_AUTH_FAILED"),
    authException
);
```

### HIGH Priority Alerts
**When Triggered:**
- Service degradation (partial failures)
- High error rates (>5%)
- Connection failures with retries
- Performance issues (high latency)
- Failed message processing

**Example:**
```java
alertService.sendHighAlert(
    "MQConsumerWorker",
    "High Error Rate Detected",
    "Error rate exceeded 5% threshold: " + errorRate,
    Map.of("workerId", workerId, "errorRate", errorRate),
    null
);
```

### WARNING Alerts
**When Triggered:**
- Recoverable errors
- Temporary service unavailability
- Configuration issues
- Performance degradation warnings

### INFO Alerts
**When Triggered:**
- System startup/shutdown
- Configuration changes
- Successful recovery from errors
- Maintenance operations

## 🔧 Implementation Examples

### 1. Enhanced Error Handling in MQ Consumer

```java
public class EnhancedMQConsumerWorker {
    
    @Autowired
    private AlertService alertService;
    
    private CircuitBreaker mqCircuitBreaker;
    private RetryPolicy connectionRetryPolicy;
    
    private void processMessageWithResilience(Message message) {
        String correlationId = extractCorrelationId(message);
        
        try {
            // Process with circuit breaker and retry
            messageProcessingRetryPolicy.execute(() -> {
                return processingCircuitBreaker.execute(() -> {
                    processMessage(message, correlationId);
                    return null;
                });
            });
            
            // Success metrics
            consecutiveErrors.set(0);
            lastSuccessfulMessage.set(System.currentTimeMillis());
            
        } catch (Exception e) {
            int errorCount = consecutiveErrors.incrementAndGet();
            
            // Send contextual alerts
            Map<String, Object> context = Map.of(
                "workerId", workerId,
                "correlationId", correlationId,
                "consecutiveErrors", errorCount,
                "queueName", configuration.getQueueName()
            );
            
            if (errorCount >= CRITICAL_ERROR_THRESHOLD) {
                alertService.sendCriticalAlert("MQConsumerWorker", 
                    "Critical Message Processing Errors", 
                    "Worker has " + errorCount + " consecutive errors", 
                    context, e);
            } else {
                alertService.sendHighAlert("MQConsumerWorker", 
                    "Message Processing Error", 
                    "Failed to process message: " + e.getMessage(), 
                    context, e);
            }
        }
    }
}
```

### 2. Kafka Producer with Enhanced Error Handling

```java
public class HighThroughputKafkaProducer {
    
    @Autowired
    private AlertService alertService;
    
    private CircuitBreaker kafkaCircuitBreaker;
    
    private void handleKafkaError(String message, Throwable cause, 
                                 Map<String, Object> context) {
        logger.error(message, cause);
        
        boolean isCritical = isCriticalError(cause);
        
        if (isCritical) {
            alertService.sendCriticalAlert("KafkaProducer", 
                "Critical Kafka Error", message, context, cause);
        } else {
            alertService.sendHighAlert("KafkaProducer", 
                "Kafka Error", message, context, cause);
        }
    }
    
    private boolean isCriticalError(Throwable cause) {
        if (cause == null) return false;
        
        String errorMessage = cause.getMessage();
        return errorMessage != null && (
            errorMessage.contains("Authentication failed") ||
            errorMessage.contains("SSL") ||
            errorMessage.contains("Connection refused")
        );
    }
}
```

### 3. Circuit Breaker Usage

```java
// Create circuit breaker
CircuitBreaker circuitBreaker = new CircuitBreaker(
    "external-service",
    5,                                    // failure threshold
    Duration.ofSeconds(30),               // timeout
    Duration.ofMinutes(1)                 // retry timeout
);

// Use with fallback
String result = circuitBreaker.executeWithFallback(
    () -> externalService.call(),         // primary operation
    () -> getCachedValue()                // fallback operation
);
```

### 4. Retry Policy Usage

```java
// Create retry policy
RetryPolicy retryPolicy = RetryPolicy.builder("database-operation")
    .maxAttempts(3)
    .initialDelay(Duration.ofMillis(500))
    .maxDelay(Duration.ofSeconds(5))
    .backoffMultiplier(2.0)
    .retryCondition(this::isRetryableException)
    .build();

// Execute with retry
Result result = retryPolicy.execute(() -> {
    return databaseService.executeQuery(sql);
});
```

## 📊 Monitoring and Health Checks

### Health Endpoint Response

```json
{
  "status": "UP",
  "components": {
    "alertingHealth": {
      "status": "UP",
      "details": {
        "totalAlerts": 1247,
        "criticalAlerts": 23,
        "failedNotifications": 5,
        "recentAlertsCount": 12,
        "totalNotifiers": 3,
        "healthyNotifiers": 3,
        "failureRate": "0.40%"
      }
    },
    "mqConsumerWorkers": {
      "status": "UP",
      "details": {
        "activeWorkers": 8,
        "healthyWorkers": 8,
        "totalProcessed": 1234567,
        "circuitBreakerStates": {
          "worker-1": "CLOSED",
          "worker-2": "CLOSED"
        }
      }
    }
  }
}
```

### Prometheus Metrics

```prometheus
# Alert metrics
datastreaming_alerts_total{level="CRITICAL"} 23
datastreaming_alerts_total{level="HIGH"} 156
datastreaming_alerts_failed_notifications_total 5

# Circuit breaker metrics
datastreaming_circuit_breaker_state{name="kafka-producer"} 0  # 0=CLOSED, 1=OPEN, 2=HALF_OPEN
datastreaming_circuit_breaker_failures{name="kafka-producer"} 2

# Error rate metrics
datastreaming_error_rate{component="mq-consumer"} 0.02
datastreaming_error_rate{component="kafka-producer"} 0.01
```

## 🔍 Troubleshooting

### Common Alert Scenarios

**1. High Error Rate Alert**
```
[CRITICAL] MQConsumerWorker - High Error Rate: Error rate exceeded 10% threshold
Context: {workerId=worker-123, errorRate=0.15, queueName=TRANSACTIONS}
```
**Action:** Check MQ connectivity, validate message formats, review logs for specific errors.

**2. Circuit Breaker Open**
```
[HIGH] KafkaProducer - Circuit Breaker Open: Kafka producer circuit breaker opened
Context: {circuitBreaker=kafka-producer, failureCount=7, state=OPEN}
```
**Action:** Check Kafka cluster health, network connectivity, authentication credentials.

**3. Authentication Failure**
```
[CRITICAL] System - Authentication Failed: SASL authentication failed for Kafka
Context: {service=kafka, mechanism=PLAIN, broker=kafka-prod:9092}
```
**Action:** Verify credentials, check security configuration, validate SASL setup.

### Alert Debugging

Enable debug logging for alerts:
```yaml
logging:
  level:
    com.datastreaming.framework.core.alert: DEBUG
    ALERTS: DEBUG
```

Check alert service health:
```bash
curl http://localhost:8080/actuator/health/alertingHealth
```

View recent alerts in logs:
```bash
grep "ALERT_ID=" application.log | tail -20
```

## 🚀 Best Practices

### 1. Alert Design
- **Use meaningful titles** that clearly describe the issue
- **Include relevant context** (IDs, metrics, configuration)
- **Categorize appropriately** - don't send too many CRITICAL alerts
- **Make alerts actionable** - include enough information for resolution

### 2. Rate Limiting
- **Configure appropriate rate limits** to prevent alert storms
- **Use different limits for different alert levels**
- **Consider time-based grouping** for similar alerts

### 3. Testing
```java
@Test
void shouldSendCriticalAlertOnSystemFailure() {
    // Simulate system failure
    systemService.simulateFailure();
    
    // Verify alert was sent
    verify(alertService).sendCriticalAlert(
        eq("SystemService"),
        eq("System Failure"),
        contains("simulated failure"),
        any(Map.class),
        any(Exception.class)
    );
}
```

### 4. Monitoring
- **Monitor alert volume** and patterns
- **Track notification success rates**
- **Review and tune alert thresholds** based on operational experience
- **Set up alerts for the alerting system itself**

## 📝 Configuration Reference

### Complete Alert Configuration

```yaml
datastreaming:
  alerts:
    enabled: true
    rateLimitMinutes: 5
    maxAlertsPerHour: 100
    
    notifiers:
      log:
        enabled: true
      
      teams:
        enabled: "${TEAMS_ALERTS_ENABLED:false}"
        webhookUrl: "${TEAMS_WEBHOOK_URL}"
        title: "${TEAMS_CARD_TITLE:Data Streaming Framework Alert}"
        minLevel: "${TEAMS_MIN_LEVEL:HIGH}"
      
      email:
        enabled: "${EMAIL_ALERTS_ENABLED:false}"
        smtpHost: "${EMAIL_SMTP_HOST}"
        smtpPort: "${EMAIL_SMTP_PORT:587}"
        username: "${EMAIL_USERNAME}"
        password: "${EMAIL_PASSWORD}"
        from: "${EMAIL_FROM:alerts@company.com}"
        to: "${EMAIL_TO:ops@company.com}"
        minLevel: "${EMAIL_MIN_LEVEL:CRITICAL}"

  resilience:
    circuitBreaker:
      failureThreshold: 5
      timeout: 30s
      retryTimeout: 60s
    
    retry:
      maxAttempts: 3
      initialDelay: 1s
      maxDelay: 30s
      backoffMultiplier: 2.0

  # Worker-specific error handling
  consumer:
    maxConsecutiveErrors: 20
    criticalErrorThreshold: 50
    maxUnresponsiveTimeMs: 300000  # 5 minutes
    
  producer:
    maxPendingMessages: 50000
    errorRateThreshold: 0.05       # 5%
    criticalErrorPatterns: 
      - "Authentication failed"
      - "SSL"
      - "Connection refused"
```

This comprehensive error handling and alerting system ensures that your Data Streaming Framework operates with enterprise-grade reliability and provides the operational visibility needed for production environments.