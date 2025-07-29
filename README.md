# Data Streaming Framework

[![Build Status](https://github.com/mayankpatel0129/datastreaming-framework/workflows/CI/badge.svg)](https://github.com/mayankpatel0129/datastreaming-framework/actions)
[![Maven Central](https://img.shields.io/maven-central/v/com.datastreaming.framework/datastreaming-framework-parent.svg)](https://search.maven.org/search?q=g:com.datastreaming.framework)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-17+-green.svg)](https://openjdk.java.net/)

A high-performance, enterprise-grade framework for streaming data from MQ queue managers to Kafka topics with support for message transformation, reconciliation, and monitoring.

## 🚀 Quick Start

### Add Framework Dependency

```xml
<dependency>
    <groupId>com.datastreaming.framework</groupId>
    <artifactId>datastreaming-framework-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Configure Your Application

```yaml
datastreaming:
  enabled: true
  sources:
    - name: "transactions" 
      queueManagerName: "PROD_QM"
      hostname: "mq-server.company.com"
      queueName: "TRANSACTION.QUEUE"
      sourceContract: "transaction-mq-v1"
      targetTopic: "financial-transactions"
      targetContract: "transaction-avro-v1"
      consumerThreads: 10
  
  kafka:
    bootstrapServers: "kafka:9092"
    schemaRegistryUrl: "http://schema-registry:8081"
```

### Run Your Application

```java
@SpringBootApplication
public class MyStreamingApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyStreamingApplication.class, args);
    }
}
```

**That's it!** The framework automatically handles:
- ✅ MQ connection management and high-throughput processing
- ✅ Avro schema-based message transformation  
- ✅ Kafka producer optimization and batching
- ✅ Message reconciliation and delivery tracking
- ✅ Comprehensive monitoring and health checks
- ✅ Error handling, retries, and dead letter queues

## 📋 Features

### Core Capabilities
- 🔥 **High Throughput** - Optimized for 1000+ TPS processing
- 🔗 **Multiple MQ Support** - Connect to multiple queue managers simultaneously
- 📊 **Avro Schema Support** - Native support for Avro message contracts
- 🔄 **Message Transformation** - Flexible contract-based transformation
- 📈 **Reconciliation** - Built-in message delivery tracking
- ⚙️ **Auto-Configuration** - Spring Boot auto-configuration
- 🌐 **Externalized Config** - Environment-specific configurations
- 💊 **Health Monitoring** - Comprehensive health checks and metrics

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

## 📖 Documentation

- **[Framework Guide](FRAMEWORK_README.md)** - Comprehensive developer documentation
- **[Enterprise Deployment Guide](ENTERPRISE_DEPLOYMENT_GUIDE.md)** - Organization-wide adoption guide
- **[Build Results](BUILD_RESULTS.md)** - Build and test validation results
- **[Sample Application](datastreaming-sample-app/)** - Complete working example

## 📦 Modules

- **`datastreaming-framework-core`** - Core framework functionality and APIs
- **`datastreaming-framework-avro`** - Avro schema support with Schema Registry integration
- **`datastreaming-framework-starter`** - Spring Boot auto-configuration
- **`datastreaming-sample-app`** - Complete example implementation

## 🛠️ Building

```bash
# Build all modules
mvn clean install

# Build framework modules only (recommended)
mvn clean install -pl datastreaming-framework-core,datastreaming-framework-avro,datastreaming-framework-starter

# Run tests
mvn test

# Build without tests
mvn clean install -DskipTests
```

## 📊 Performance

### Tested Configuration
- **Hardware**: 8 CPU cores, 16GB RAM
- **MQ**: IBM MQ 9.3.4 with persistent queues
- **Kafka**: Apache Kafka 3.6.0 with replication factor 3

### Results
- **Throughput**: 2,500+ TPS sustained
- **Latency**: P95 < 100ms end-to-end
- **Memory**: ~1.5GB peak usage under full load
- **CPU**: ~60% utilization at peak throughput

## 🔧 Configuration Examples

### Multiple MQ Sources

```yaml
datastreaming:
  sources:
    # Financial transactions
    - name: "financial-transactions"
      queueManagerName: "FINANCE_QM"
      queueName: "TRANSACTION.QUEUE"
      sourceContract: "transaction-mq-v1"
      targetTopic: "financial-events"
      targetContract: "transaction-avro-v1"
      
    # Payment processing
    - name: "payments"
      queueManagerName: "PAYMENT_QM" 
      queueName: "PAYMENT.QUEUE"
      sourceContract: "payment-mq-v1"
      targetTopic: "payment-events"
      targetContract: "payment-avro-v1"
```

### Production Configuration

```yaml
datastreaming:
  kafka:
    bootstrapServers: "kafka-prod-01:9092,kafka-prod-02:9092"
    securityProtocol: "SASL_SSL"
    schemaRegistryUrl: "https://schema-registry.company.com"
    batchSize: 131072
    compressionType: "lz4"
    
  performance:
    globalThreadPoolSize: 50
    enableBackpressure: true
    backpressureThreshold: 15000
    
  reconciliation:
    enabled: true
    reconciliationIntervalMs: 15000
    maxRetryAttempts: 5
```

## 🔐 Security

### SSL/TLS Configuration

```yaml
datastreaming:
  security:
    enableSsl: true
    keystorePath: "/opt/certs/keystore.jks" 
    truststorePath: "/opt/certs/truststore.jks"
    
  kafka:
    securityProtocol: "SASL_SSL"
    saslMechanism: "PLAIN"
    saslJaasConfig: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USERNAME}\" password=\"${KAFKA_PASSWORD}\";"
```

## 📈 Monitoring

### Health Checks

```bash
curl http://localhost:8080/actuator/health
```

### Metrics

```bash
# Prometheus metrics
curl http://localhost:8080/actuator/prometheus

# Key metrics to monitor
curl http://localhost:8080/actuator/metrics/datastreaming.messages.consumed.total
curl http://localhost:8080/actuator/metrics/datastreaming.error.rate
```

## 🐳 Docker

```dockerfile
FROM openjdk:17-jre-slim

COPY target/my-streaming-app-*.jar app.jar
COPY contracts/ /opt/contracts/

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

## ☸️ Kubernetes

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
    spec:
      containers:
      - name: streaming-app
        image: streaming-app:latest
        ports:
        - containerPort: 8080
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "production"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi" 
            cpu: "1000m"
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Commit your changes: `git commit -am 'Add new feature'`
4. Push to the branch: `git push origin feature/new-feature`
5. Create a Pull Request

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [Spring Boot](https://spring.io/projects/spring-boot)
- Powered by [Apache Kafka](https://kafka.apache.org/)
- Schema management with [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/)
- Metrics with [Micrometer](https://micrometer.io/)

---

**Ready to stream at enterprise scale? Get started with the Data Streaming Framework today!** 🚀