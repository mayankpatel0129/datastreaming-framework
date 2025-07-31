# Data Streaming Framework

A high-performance, enterprise-grade framework for streaming data from MQ queue managers to Kafka topics with comprehensive support for MQ message parsing, transformation, reconciliation, and monitoring.

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

Place your Avro schemas in `src/main/resources/contracts/` and MQ contracts in `src/main/resources/mq/contracts/`:

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

**transaction-mq-v1.json:** (MQ message contract)
```json
{
  "name": "transaction-mq-v1",
  "version": "1.0",
  "format": "FIXED_LENGTH",
  "fields": [
    {"name": "transactionId", "type": "STRING", "position": 0, "length": 10},
    {"name": "amount", "type": "DECIMAL", "position": 10, "length": 15},
    {"name": "timestamp", "type": "DATETIME", "position": 25, "length": 14, "dateFormat": "yyyyMMddHHmmss"}
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
- ✅ **MQ Message Parsing** - Fixed-length, delimited, and custom format support
- ✅ **Externalized Contracts** - JSON-based message contract definitions
- ✅ **Avro Schema Support** - Native support for Avro message contracts
- ✅ **Message Transformation** - Flexible contract-based transformation
- ✅ **Type-Safe Processing** - Automatic type conversion with validation
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
┌─────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐
│   MQ Queue  │───▶│  Framework      │───▶│  MQ Message     │───▶│  Contract       │───▶│   Kafka     │
│   Manager   │    │  Consumer       │    │  Parser         │    │  Transformer    │    │   Topic     │
└─────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────┘
                            │                       │                       │
                            ▼                       ▼                       ▼
                   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
                   │  Reconciliation │    │  MQ Contract    │    │   Monitoring    │
                   │     Service     │    │   Registry      │    │   & Metrics     │
                   └─────────────────┘    └─────────────────┘    └─────────────────┘
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

## 🔄 MQ Message Parsing

The framework provides comprehensive support for parsing various MQ message formats using externalized contracts, enabling organizations to handle fixed-length, delimited, and custom message formats with type-safe processing.

### Supported Message Formats

- **Fixed-Length Messages** - Position-based field extraction with configurable padding
- **Delimited Messages** - CSV, TSV, pipe-separated with quote handling
- **JSON Messages** - Native JSON parsing and validation
- **XML Messages** - XML parsing with schema validation
- **Custom Formats** - Extensible parser interface for proprietary formats

### MQ Contract Configuration

Enable MQ message parsing in your `application.yml`:

```yaml
datastreaming:
  # Enable MQ message parsing
  mq:
    enabled: true
    contracts:
      location: "classpath*:mq/contracts/**/*.json"
      reloadOnChange: false
      validateOnStartup: true
    parsing:
      strictMode: false
      trimFields: true
      defaultEncoding: "UTF-8"
      maxMessageSize: 1048576
      enableMetrics: true
    
    # Queue to topic mapping
    queueToTopicMapping:
      "FINANCE.TRANSACTION.QUEUE": "financial-transactions"
      "PAYMENTS.INPUT.QUEUE": "payment-events"
      "CUSTOMER.EVENTS.QUEUE": "customer-events"
    
    # Queue to contract mapping
    queueToContractMapping:
      "FINANCE.TRANSACTION.QUEUE": "transaction-fixed"
      "PAYMENTS.INPUT.QUEUE": "customer-csv"
      "CUSTOMER.EVENTS.QUEUE": "order-pipe"
```

### Fixed-Length Message Contracts

Define fixed-length message structure with position-based fields:

**transaction-fixed-1.0.json:**
```json
{
  "name": "transaction-fixed",
  "version": "1.0",
  "description": "Fixed-length transaction message format",
  "format": "FIXED_LENGTH",
  "properties": {
    "trimFields": "true",
    "strictMode": "false",
    "encoding": "UTF-8"
  },
  "fields": [
    {
      "name": "transactionId",
      "type": "STRING",
      "position": 0,
      "length": 10,
      "required": true,
      "description": "Unique transaction identifier",
      "pattern": "^[A-Z0-9]{10}$"
    },
    {
      "name": "accountNumber", 
      "type": "STRING",
      "position": 10,
      "length": 12,
      "required": true,
      "description": "Customer account number"
    },
    {
      "name": "amount",
      "type": "DECIMAL",
      "position": 22,
      "length": 15,
      "required": true,
      "description": "Transaction amount"
    },
    {
      "name": "currency",
      "type": "STRING", 
      "position": 37,
      "length": 3,
      "required": true,
      "description": "Currency code",
      "pattern": "^[A-Z]{3}$"
    },
    {
      "name": "transactionDate",
      "type": "DATE",
      "position": 40,
      "length": 8,
      "required": true,
      "description": "Transaction date",
      "dateFormat": "yyyyMMdd"
    },
    {
      "name": "transactionTime",
      "type": "TIME", 
      "position": 48,
      "length": 6,
      "required": true,
      "description": "Transaction time",
      "dateFormat": "HHmmss"
    },
    {
      "name": "status",
      "type": "STRING",
      "position": 54, 
      "length": 1,
      "required": false,
      "description": "Transaction status",
      "defaultValue": "P"
    },
    {
      "name": "isVip",
      "type": "BOOLEAN",
      "position": 55,
      "length": 1, 
      "required": false,
      "description": "VIP customer flag",
      "defaultValue": "0"
    }
  ]
}
```

**Example Fixed-Length Message:**
```
TX12345678ACC123456789000000001234.56USD20240731143005P1
```

### Delimited Message Contracts

Define CSV, TSV, or custom-delimited message formats:

**customer-csv-1.0.json:**
```json
{
  "name": "customer-csv",
  "version": "1.0",
  "description": "CSV format customer data",
  "format": "DELIMITED",
  "properties": {
    "delimiter": ",",
    "hasQuotes": "true",
    "quoteChar": "\"",
    "trimFields": "true",
    "strictMode": "false",
    "encoding": "UTF-8"
  },
  "fields": [
    {
      "name": "customerId",
      "type": "STRING",
      "required": true,
      "description": "Customer ID",
      "pattern": "^CUST\\d{6}$"
    },
    {
      "name": "firstName",
      "type": "STRING",
      "required": true,
      "description": "Customer first name"
    },
    {
      "name": "lastName", 
      "type": "STRING",
      "required": true,
      "description": "Customer last name"
    },
    {
      "name": "email",
      "type": "STRING",
      "required": true,
      "description": "Customer email address",
      "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"
    },
    {
      "name": "birthDate",
      "type": "DATE",
      "required": false,
      "description": "Customer birth date",
      "dateFormat": "yyyy-MM-dd"
    },
    {
      "name": "registrationDate",
      "type": "DATETIME", 
      "required": true,
      "description": "Customer registration date and time",
      "dateFormat": "yyyy-MM-dd HH:mm:ss"
    },
    {
      "name": "isActive",
      "type": "BOOLEAN",
      "required": false,
      "description": "Customer active status",
      "defaultValue": "true"
    },
    {
      "name": "creditLimit",
      "type": "DECIMAL",
      "required": false,
      "description": "Customer credit limit", 
      "defaultValue": "0.00"
    }
  ]
}
```

**Example CSV Message:**
```
"CUST123456","John","Doe","john.doe@example.com","1990-05-15","2024-01-15 10:30:00","true","5000.00"
```

### Supported Data Types

The framework supports comprehensive data types with automatic conversion:

| Type | Java Type | Description | Example Values |
|------|-----------|-------------|----------------|
| `STRING` | `String` | Text data | `"Hello World"` |
| `INTEGER` | `Integer` | 32-bit integers | `12345` |
| `LONG` | `Long` | 64-bit integers | `1234567890L` |
| `DECIMAL` | `BigDecimal` | Decimal numbers | `1234.56` |
| `BOOLEAN` | `Boolean` | Boolean values | `true`, `false`, `Y`, `N`, `1`, `0` |
| `DATE` | `LocalDate` | Date without time | `2024-07-31` |
| `DATETIME` | `LocalDateTime` | Date with time | `2024-07-31 14:30:05` |
| `TIME` | `LocalTime` | Time without date | `14:30:05` |
| `BINARY` | `byte[]` | Binary data | Raw bytes |
| `CUSTOM` | `String` | Custom types | Application-specific |

### Field Validation Features

- **Required Fields** - Enforce mandatory field presence
- **Pattern Matching** - Regex validation for field values  
- **Default Values** - Automatic default value assignment
- **Length Validation** - Fixed-length field size checking
- **Date Formatting** - Custom date/time format parsing
- **Encoding Support** - Configurable character encoding

### Programmatic Usage

Use the MQ parsing services directly in your code:

```java
@Service
public class MessageProcessingService {
    
    @Autowired
    private MQMessageParsingService parsingService;
    
    @Autowired
    private MQToKafkaTransformer transformer;
    
    public void processMessage(String mqMessage, String contractName) {
        try {
            // Parse MQ message using contract
            Map<String, Object> parsedMessage = parsingService.parseMessage(mqMessage, contractName);
            
            // Transform for Kafka publishing
            Object kafkaMessage = transformer.transform(mqMessage, contractName);
            
            // Process the structured data
            processStructuredData(parsedMessage);
            
        } catch (MQMessageParsingException e) {
            logger.error("Failed to parse MQ message: {}", e.getMessage());
            // Handle parsing error
        }
    }
    
    public boolean validateMessage(String mqMessage, String contractName) {
        return parsingService.validateMessageFormat(mqMessage, contractName);
    }
}
```

### Contract Registry Management

Monitor and manage contracts at runtime:

```java
@Component
public class ContractManager {
    
    @Autowired
    private MQContractRegistry contractRegistry;
    
    public void showContractInfo() {
        // Get all loaded contracts
        Map<String, MQContract> contracts = contractRegistry.getAllContracts();
        
        // Get registry statistics
        var stats = contractRegistry.getStats();
        logger.info("Loaded {} contracts: {}", stats.getTotalContracts(), stats.getContractsByFormat());
        
        // Reload contracts from disk
        contractRegistry.reloadContracts();
    }
    
    public void registerNewContract(MQContract contract) {
        contractRegistry.registerContract(contract);
    }
}
```

### Performance Considerations

- **Parser Selection** - Automatic parser selection based on message format
- **Field Caching** - Contract field definitions are cached for performance
- **Type Conversion** - Optimized type conversion with minimal object creation
- **Memory Usage** - Efficient string processing with minimal allocations
- **Validation** - Configurable strict/lenient modes for performance tuning

### Error Handling

The framework provides detailed error information for parsing failures:

```java
try {
    Map<String, Object> result = parsingService.parseMessage(message, "contract-name");
} catch (MQMessageParsingException e) {
    // Detailed error information
    String contractName = e.getContractName();
    String messageContent = e.getMessageContent(); 
    int fieldIndex = e.getFieldIndex();
    
    logger.error("Parsing failed for contract '{}' at field index {}: {}", 
                contractName, fieldIndex, e.getMessage());
}
```

### Custom Parser Development

Extend the framework with custom message parsers:

```java
@Component
public class XMLMessageParser implements MQMessageParser {
    
    @Override
    public Map<String, Object> parse(String messageContent, MQContract contract) 
            throws MQMessageParsingException {
        // Custom XML parsing logic
        return parseXMLMessage(messageContent, contract);
    }
    
    @Override
    public boolean supports(MQMessageFormat format) {
        return format == MQMessageFormat.XML;
    }
    
    @Override
    public MQMessageFormat getSupportedFormat() {
        return MQMessageFormat.XML;
    }
}
```

The framework automatically discovers and registers custom parsers through Spring's component scanning.

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
- **MQ Parsing Guide**: [MQ_PARSING_USER_GUIDE.md](MQ_PARSING_USER_GUIDE.md)
- **Issues**: [GitHub Issues](issues-url)  
- **Discussions**: [GitHub Discussions](discussions-url)
- **Enterprise Support**: Contact your framework team

## 📄 License

This framework is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.