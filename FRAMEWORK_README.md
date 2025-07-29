# Data Streaming Framework

A high-performance, enterprise-grade framework for streaming data from MQ queue managers to Kafka topics with support for message transformation, reconciliation, and monitoring.

## 🚀 Quick Start Guide

### 1. Add Framework Dependency

Add the framework starter to your `pom.xml`:

```xml
<dependency>
    <groupId>com.datastreaming.framework</groupId>
    <artifactId>datastreaming-framework-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. Configure Your Application

Create `application.yml` with your MQ and Kafka settings:

```yaml
datastreaming:
  enabled: true
  sources:
    - name: "my-transactions"
      queueManagerName: "MY_QM"
      hostname: "mq-server.company.com"
      port: 1414
      queueName: "TRANSACTION.QUEUE"
      sourceContract: "transaction-mq-v1"
      targetTopic: "transactions"
      targetContract: "transaction-avro-v1"
      consumerThreads: 5
  
  kafka:
    bootstrapServers: "kafka:9092"
    schemaRegistryUrl: "http://schema-registry:8081"
```

### 3. Define Message Contracts

Place your Avro schemas in `src/main/resources/contracts/`:

**transaction-avro-v1.avsc:**
```json
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.company.events",
  "fields": [
    {"name": "transactionId", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "timestamp", "type": "long"}
  ]
}
```

### 4. Run Your Application

```java
@SpringBootApplication
public class MyStreamingApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyStreamingApplication.class, args);
    }
}
```

That's it! The framework will automatically:
- Connect to your MQ queue managers
- Consume messages with high throughput
- Transform messages using your contracts
- Publish to Kafka with optimized batching
- Provide monitoring and health checks
- Handle errors and retries

## 📋 Framework Features

### Core Capabilities
- ✅ **Multiple MQ Support** - Connect to multiple queue managers simultaneously
- ✅ **High Throughput** - Optimized for 1000+ TPS processing
- ✅ **Avro Schema Support** - Native support for Avro message contracts
- ✅ **Message Transformation** - Flexible contract-based transformation
- ✅ **Reconciliation** - Built-in message delivery tracking
- ✅ **Auto-Configuration** - Spring Boot auto-configuration
- ✅ **Externalized Config** - Environment-specific configurations
- ✅ **Health Monitoring** - Comprehensive health checks and metrics

### Enterprise Features
- 🔒 **Security First** - SSL/TLS, SASL authentication
- 🎯 **High Availability** - Circuit breakers, automatic recovery
- 📊 **Observability** - Prometheus metrics, structured logging
- 🔄 **Graceful Shutdown** - Clean resource management
- 🎛️ **Backpressure Control** - Configurable flow control
- 🏗️ **Thread Safe** - Concurrent processing with safety guarantees

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐
│   MQ Queue  │───▶│  Framework      │───▶│  Contract       │───▶│   Kafka     │
│   Manager   │    │  Consumer       │    │  Transformer    │    │   Topic     │
└─────────────┘    └─────────────────┘    └─────────────────┘    └─────────────┘
                            │                       │
                            ▼                       ▼
                   ┌─────────────────┐    ┌─────────────────┐
                   │  Reconciliation │    │   Monitoring    │
                   │     Service     │    │   & Metrics     │
                   └─────────────────┘    └─────────────────┘
```

## 📖 Configuration Guide

### Complete Configuration Example

```yaml
datastreaming:
  enabled: true
  
  # Multiple MQ Sources
  sources:
    - name: "financial-transactions"
      queueManagerName: "PROD_QM_FINANCE"
      hostname: "mq-finance.company.com"
      port: 1414
      channel: "SYSTEM.DEF.SVRCONN"
      queueName: "FINANCE.TRANSACTION.QUEUE"
      userId: "${MQ_USER}"
      password: "${MQ_PASSWORD}"
      sourceContract: "transaction-mq-v1"
      targetTopic: "financial-transactions"
      targetContract: "transaction-avro-v1"
      consumerThreads: 10
      maxBatchSize: 500
      enabled: true
      additionalProperties:
        XMSC_WMQ_CLIENT_RECONNECT_OPTIONS: "67108864"
    
    - name: "payment-events"
      queueManagerName: "PROD_QM_PAYMENTS"
      hostname: "mq-payments.company.com"
      port: 1414
      queueName: "PAYMENTS.QUEUE"
      sourceContract: "payment-mq-v1"
      targetTopic: "payment-events"
      targetContract: "payment-avro-v1"
      consumerThreads: 8
      enabled: true

  # Kafka Configuration
  kafka:
    bootstrapServers: "kafka-prod:9092"
    securityProtocol: "SASL_SSL"
    saslMechanism: "PLAIN"
    saslJaasConfig: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USERNAME}\" password=\"${KAFKA_PASSWORD}\";"
    schemaRegistryUrl: "https://schema-registry.company.com"
    batchSize: 131072
    lingerMs: 5
    bufferMemory: 268435456
    compressionType: "lz4"
    enableIdempotence: true

  # Reconciliation
  reconciliation:
    enabled: true
    reconciliationIntervalMs: 15000
    maxRetryAttempts: 5
    retryBackoffMs: 3000
    enableDeadLetterQueue: true
    deadLetterTopic: "streaming-dlq"

  # Performance Tuning
  performance:
    globalThreadPoolSize: 50
    queueCapacity: 20000
    enableBackpressure: true
    backpressureThreshold: 15000

  # Monitoring
  monitoring:
    metricsEnabled: true
    healthChecksEnabled: true
    metricsPrefix: "my_streaming_app"

  # Contract Management
  contracts:
    type: "file"  # or "schema-registry"
    location: "classpath:/contracts/"
    autoReload: true
```

### Environment-Specific Profiles

The framework supports Spring profiles for different environments:

```yaml
---
# Production Profile
spring:
  config:
    activate:
      on-profile: production

datastreaming:
  kafka:
    bootstrapServers: "kafka-prod-01:9092,kafka-prod-02:9092"
    securityProtocol: "SASL_SSL"
  
  security:
    enableSsl: true
    keystorePath: "/opt/certs/keystore.jks"

---
# Development Profile  
spring:
  config:
    activate:
      on-profile: development

datastreaming:
  kafka:
    bootstrapServers: "localhost:9092"
    securityProtocol: "PLAINTEXT"
  
  security:
    enableSsl: false
```

## 🔧 Contract Management

### Avro Schema Contracts

Place Avro schemas in your contracts directory:

```
src/main/resources/contracts/
├── transaction-mq-v1.avsc       # Source MQ format
├── transaction-avro-v1.avsc     # Target Kafka format
├── payment-mq-v1.avsc
└── payment-avro-v1.avsc
```

### Schema Registry Integration

For production environments, use Confluent Schema Registry:

```yaml
datastreaming:
  contracts:
    type: "schema-registry"
  kafka:
    schemaRegistryUrl: "http://schema-registry:8081"
    schemaRegistryCredentials: "username:password"
```

### Custom Transformers

Implement custom transformation logic:

```java
@Component
public class CustomTransactionTransformer implements MessageTransformer {

    @Override
    public String transform(Message sourceMessage, 
                           String sourceContract, 
                           String targetContract, 
                           String correlationId) throws TransformationException {
        // Your custom transformation logic
        return transformedMessage;
    }

    @Override
    public boolean supports(String sourceContract, String targetContract) {
        return "custom-format".equals(sourceContract);
    }

    @Override
    public String getName() {
        return "CustomTransactionTransformer";
    }
}
```

## 📊 Monitoring & Observability

### Health Checks

Built-in health indicators:

```bash
curl http://localhost:8080/actuator/health
```

```json
{
  "status": "UP",
  "components": {
    "transactionProcessor": {
      "status": "UP",
      "details": {
        "mqConsumers": "UP",
        "kafkaProducer": "UP", 
        "messageTracker": "UP",
        "pendingMessages": 0,
        "activeWorkers": 18
      }
    }
  }
}
```

### Metrics

Prometheus metrics endpoint:

```bash
curl http://localhost:8080/actuator/prometheus
```

Key metrics:
- `datastreaming_messages_consumed_total` - Total messages consumed
- `datastreaming_messages_produced_total` - Total messages produced
- `datastreaming_message_processing_duration` - Processing time
- `datastreaming_error_rate` - Error rate percentage
- `datastreaming_workers_active` - Active worker threads

### Logging

Structured logging with correlation IDs:

```
2024-01-15 10:30:45.123 [worker-1] INFO [correlationId:tx-12345] c.d.f.c.consumer.MQConsumerWorker - Message processed successfully
```

## 🔐 Security

### SSL/TLS Configuration

```yaml
datastreaming:
  security:
    enableSsl: true
    keystorePath: "/opt/certs/keystore.jks"
    keystorePassword: "${KEYSTORE_PASSWORD}"
    truststorePath: "/opt/certs/truststore.jks"
    truststorePassword: "${TRUSTSTORE_PASSWORD}"
  
  kafka:
    securityProtocol: "SASL_SSL"
    saslMechanism: "PLAIN"
    saslJaasConfig: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USERNAME}\" password=\"${KAFKA_PASSWORD}\";"
```

### Credential Management

Use environment variables or external secret management:

```bash
export MQ_USER_ID="service-account"
export MQ_PASSWORD="$(vault kv get -field=password secret/mq/prod)"
export KAFKA_USERNAME="streaming-app"
export KAFKA_PASSWORD="$(vault kv get -field=password secret/kafka/prod)"
```

## 🚀 Deployment

### Docker Deployment

```dockerfile
FROM openjdk:17-jre-slim

COPY target/my-streaming-app-*.jar app.jar
COPY contracts/ /opt/contracts/

ENV DATASTREAMING_CONTRACTS_LOCATION=file:/opt/contracts/

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streaming-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: streaming-app
  template:
    metadata:
      labels:
        app: streaming-app
    spec:
      containers:
      - name: streaming-app
        image: my-streaming-app:latest
        ports:
        - containerPort: 8080
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "production"
        - name: MQ_USER_ID
          valueFrom:
            secretKeyRef:
              name: mq-credentials
              key: username
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /actuator/health
            port: 8080
          initialDelaySeconds: 60
        readinessProbe:
          httpGet:
            path: /actuator/health
            port: 8080
          initialDelaySeconds: 30
```

## 🎯 Performance Tuning

### High Throughput Configuration

For maximum throughput:

```yaml
datastreaming:
  performance:
    globalThreadPoolSize: 100
    queueCapacity: 50000
    enableBackpressure: true
    backpressureThreshold: 40000

  sources:
    - name: "high-volume-source"
      consumerThreads: 20
      maxBatchSize: 1000

  kafka:
    batchSize: 262144      # 256KB
    lingerMs: 1            # Aggressive batching
    bufferMemory: 536870912 # 512MB
    compressionType: "lz4"
    maxInFlightRequestsPerConnection: 5
```

### JVM Tuning

```bash
export JAVA_OPTS="-Xms2g -Xmx4g \
                  -XX:+UseG1GC \
                  -XX:MaxGCPauseMillis=200 \
                  -XX:+UseStringDeduplication"
```

## 🧪 Testing

### Integration Testing

```java
@SpringBootTest
@TestPropertySource(properties = {
    "datastreaming.enabled=true",
    "datastreaming.sources[0].queueManagerName=TEST_QM"
})
class StreamingFrameworkIntegrationTest {

    @Autowired
    private StreamingFramework streamingFramework;

    @Test
    void shouldProcessMessages() {
        assertTrue(streamingFramework.isHealthy());
        // Test message processing
    }
}
```

### Contract Testing

Validate schema compatibility:

```java
@Test
void shouldValidateSchemaCompatibility() {
    AvroSchemaManager schemaManager = new AvroSchemaManager();
    Schema readerSchema = loadSchema("transaction-v2.avsc");
    Schema writerSchema = loadSchema("transaction-v1.avsc");
    
    assertTrue(schemaManager.isCompatible(readerSchema, writerSchema));
}
```

## 🆘 Troubleshooting

### Common Issues

1. **Connection Failures**
   ```
   Check MQ connectivity: telnet mq-host 1414
   Verify credentials and permissions
   Check firewall rules
   ```

2. **Schema Registry Issues**
   ```
   Verify schema-registry URL accessibility
   Check subject naming conventions
   Validate Avro schema syntax
   ```

3. **Performance Issues**
   ```
   Monitor thread pool utilization
   Check Kafka producer metrics
   Analyze GC patterns
   Review network latency
   ```

### Debugging

Enable debug logging:

```yaml
logging:
  level:
    com.datastreaming.framework: DEBUG
    org.apache.kafka.clients: DEBUG
```

Monitor key metrics:
- Message processing rate
- Error rate trends  
- Worker thread health
- Memory usage patterns

## 🤝 Contributing

The framework is designed to be extensible. You can:

1. **Implement Custom Transformers**
2. **Add New Contract Registry Types**
3. **Create Custom Monitoring Components**
4. **Extend Security Features**

### Extension Points

```java
// Custom Contract Registry
public class DatabaseContractRegistry implements ContractRegistry {
    // Implementation
}

// Custom Message Transformer  
public class ProtobufTransformer implements MessageTransformer {
    // Implementation  
}

// Custom Health Indicator
@Component
public class CustomHealthIndicator implements HealthIndicator {
    // Implementation
}
```

## 📞 Support

- **Documentation**: [Framework Wiki](wiki-url)
- **Issues**: [GitHub Issues](issues-url)  
- **Discussions**: [GitHub Discussions](discussions-url)
- **Enterprise Support**: Contact your framework team

## 📄 License

This framework is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.