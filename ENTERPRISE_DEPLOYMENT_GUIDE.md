# Enterprise Deployment Guide
## Data Streaming Framework for Organizations

This guide explains how to deploy and use the Data Streaming Framework across your organization as a reusable JAR dependency.

## 🏢 Framework as Organizational Standard

### Benefits for Your Organization

1. **Standardization** - Common patterns across all streaming applications
2. **Reduced Time-to-Market** - New streaming apps in hours, not weeks
3. **Centralized Maintenance** - Framework updates benefit all applications
4. **Compliance & Security** - Built-in security and audit capabilities
5. **Cost Efficiency** - Shared infrastructure and expertise

### Framework Adoption Strategy

```
Phase 1: Pilot Project (1-2 applications)
Phase 2: Department Rollout (5-10 applications) 
Phase 3: Enterprise Adoption (Organization-wide)
```

## 📦 Framework Distribution

### Internal Maven Repository Setup

1. **Publish Framework to Internal Repository**

```bash
# Build and publish framework
mvn clean deploy -P enterprise-release

# Framework artifacts available:
# com.datastreaming.framework:datastreaming-framework-starter:1.0.0
# com.datastreaming.framework:datastreaming-framework-core:1.0.0
# com.datastreaming.framework:datastreaming-framework-avro:1.0.0
```

2. **Corporate POM Configuration**

```xml
<!-- Corporate Parent POM -->
<project>
    <groupId>com.yourcompany</groupId>
    <artifactId>corporate-parent</artifactId>
    <version>1.0.0</version>
    <packaging>pom</packaging>
    
    <dependencyManagement>
        <dependencies>
            <!-- Data Streaming Framework BOM -->
            <dependency>
                <groupId>com.datastreaming.framework</groupId>
                <artifactId>datastreaming-framework-parent</artifactId>
                <version>1.0.0</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

## 🚀 Quick Start for Development Teams

### Step 1: Create New Streaming Application

```bash
# Using Maven archetype (optional)
mvn archetype:generate \
  -DarchetypeGroupId=com.datastreaming.framework \
  -DarchetypeArtifactId=datastreaming-archetype \
  -DgroupId=com.yourcompany.streaming \
  -DartifactId=customer-events-stream \
  -Dversion=1.0.0-SNAPSHOT
```

### Step 2: Minimal Project Structure

```
customer-events-stream/
├── pom.xml
├── src/main/
│   ├── java/
│   │   └── com/yourcompany/streaming/CustomerEventsApplication.java
│   └── resources/
│       ├── application.yml
│       └── contracts/
│           ├── customer-mq-v1.json
│           └── customer-avro-v1.avsc
└── docker/
    └── Dockerfile
```

### Step 3: Application Dependencies

```xml
<project>
    <parent>
        <groupId>com.yourcompany</groupId>
        <artifactId>corporate-parent</artifactId>
        <version>1.0.0</version>
    </parent>
    
    <artifactId>customer-events-stream</artifactId>
    
    <dependencies>
        <!-- Single framework dependency -->
        <dependency>
            <groupId>com.datastreaming.framework</groupId>
            <artifactId>datastreaming-framework-starter</artifactId>
        </dependency>
        
        <!-- Optional: Web endpoints for management -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
    </dependencies>
</project>
```

### Step 4: Application Configuration

```java
@SpringBootApplication
public class CustomerEventsApplication {
    public static void main(String[] args) {
        SpringApplication.run(CustomerEventsApplication.class, args);
    }
}
```

### Step 5: Environment Configuration

```yaml
# application.yml
datastreaming:
  enabled: true
  sources:
    - name: "customer-events"
      queueManagerName: "${MQ_QUEUE_MANAGER}"
      hostname: "${MQ_HOSTNAME}"
      queueName: "${MQ_QUEUE_NAME}"
      sourceContract: "customer-mq-v1"
      targetTopic: "${KAFKA_TOPIC}"
      targetContract: "customer-avro-v1"
      consumerThreads: ${CONSUMER_THREADS:5}
  
  kafka:
    bootstrapServers: "${KAFKA_BROKERS}"
    schemaRegistryUrl: "${SCHEMA_REGISTRY_URL}"
```

## 🏗️ Enterprise Architecture Patterns

### Pattern 1: Domain-Specific Applications

```
Financial Services Domain:
├── transaction-processor/     (Framework + Financial contracts)
├── payment-processor/         (Framework + Payment contracts)  
├── fraud-detector/           (Framework + Fraud contracts)
└── risk-calculator/          (Framework + Risk contracts)

Customer Domain:
├── customer-onboarding/      (Framework + Customer contracts)
├── profile-updater/          (Framework + Profile contracts)
└── notification-sender/      (Framework + Notification contracts)
```

### Pattern 2: Multi-Tenant Applications

```yaml
# Multi-tenant configuration
datastreaming:
  sources:
    # Tenant A
    - name: "tenant-a-orders"
      queueManagerName: "TENANT_A_QM"
      queueName: "ORDERS.QUEUE"
      targetTopic: "tenant-a-orders"
      
    # Tenant B  
    - name: "tenant-b-orders"
      queueManagerName: "TENANT_B_QM"
      queueName: "ORDERS.QUEUE"
      targetTopic: "tenant-b-orders"
```

### Pattern 3: Event-Driven Microservices

```
Order Processing Flow:
MQ Queue → [Framework App] → Kafka Topic → [Order Service]
                                       → [Inventory Service]
                                       → [Payment Service]
                                       → [Notification Service]
```

## 🔧 Configuration Management

### Environment-Specific Configurations

#### Development Environment
```yaml
# application-dev.yml
datastreaming:
  sources:
    - hostname: "dev-mq.company.com"
      consumerThreads: 2
  kafka:
    bootstrapServers: "dev-kafka:9092"
    securityProtocol: "PLAINTEXT"
```

#### Production Environment
```yaml
# application-prod.yml  
datastreaming:
  sources:
    - hostname: "prod-mq.company.com"
      consumerThreads: 20
  kafka:
    bootstrapServers: "prod-kafka-01:9092,prod-kafka-02:9092,prod-kafka-03:9092"
    securityProtocol: "SASL_SSL"
  security:
    enableSsl: true
```

### Configuration Templates

Provide standardized configuration templates:

```bash
# Configuration templates repository
enterprise-config-templates/
├── environments/
│   ├── dev-template.yml
│   ├── test-template.yml
│   ├── prod-template.yml
├── contracts/
│   ├── common/
│   │   ├── generic-message.avsc
│   │   └── error-event.avsc
│   └── domain-specific/
│       ├── financial/
│       ├── customer/
│       └── inventory/
└── deployment/
    ├── kubernetes/
    ├── docker-compose/
    └── helm-charts/
```

## 🚀 Deployment Strategies

### Strategy 1: Kubernetes Deployment

**Namespace Organization:**
```
streaming-dev/          # Development namespace
streaming-test/         # Testing namespace  
streaming-prod/         # Production namespace
```

**Resource Templates:**
```yaml
# k8s/deployment-template.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${APP_NAME}
  namespace: ${NAMESPACE}
spec:
  replicas: ${REPLICA_COUNT}
  selector:
    matchLabels:
      app: ${APP_NAME}
  template:
    spec:
      containers:
      - name: ${APP_NAME}
        image: ${IMAGE_REPOSITORY}/${APP_NAME}:${VERSION}
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: ${ENVIRONMENT}
        - name: MQ_HOSTNAME
          valueFrom:
            configMapKeyRef:
              name: mq-config
              key: hostname
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits: 
            memory: "2Gi"
            cpu: "1000m"
```

### Strategy 2: Docker Compose for Development

```yaml
# docker-compose.template.yml
version: '3.8'
services:
  ${APP_NAME}:
    build: .
    environment:
      - SPRING_PROFILES_ACTIVE=development
      - MQ_HOSTNAME=ibm-mq
      - KAFKA_BROKERS=kafka:9092
    depends_on:
      - kafka
      - ibm-mq
    ports:
      - "${PORT}:8080"
      
  # Shared infrastructure
  kafka:
    image: confluentinc/cp-kafka:latest
    # ... kafka configuration
    
  ibm-mq:
    image: ibmcom/mq:latest
    # ... MQ configuration
```

## 🔒 Security & Compliance

### Security Standards

1. **Credential Management**
```bash
# Use external secret management
export MQ_PASSWORD="$(vault kv get -field=password secret/mq/${ENVIRONMENT})"
export KAFKA_PASSWORD="$(kubectl get secret kafka-creds -o jsonpath='{.data.password}' | base64 -d)"
```

2. **Network Security**
```yaml
# Network policies
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: streaming-app-netpol
spec:
  podSelector:
    matchLabels:
      type: streaming-app
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: monitoring
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: kafka
  - to:
    - namespaceSelector:
        matchLabels:
          name: mq
```

3. **Audit Logging**
```yaml
datastreaming:
  monitoring:
    auditLoggingEnabled: true
    auditLevel: "DETAILED"
    auditOutputFormat: "JSON"
```

### Compliance Features

- **Data Lineage Tracking** - Built-in correlation ID tracking
- **Message Retention** - Configurable retention policies
- **Error Handling** - Dead letter queues for failed messages
- **Metrics & Monitoring** - Full observability stack

## 📊 Monitoring & Operations

### Centralized Monitoring Stack

```yaml
# monitoring-stack.yml
version: '3.8'
services:
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"
      
  grafana:
    image: grafana/grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./dashboards:/var/lib/grafana/dashboards
    ports:
      - "3000:3000"
      
  alertmanager:
    image: prom/alertmanager
    volumes:
      - ./alertmanager.yml:/etc/alertmanager/alertmanager.yml
    ports:
      - "9093:9093"
```

### Standard Dashboards

Create organization-wide Grafana dashboards:

1. **Framework Overview Dashboard**
   - Total applications using framework
   - Framework version distribution
   - Overall health status

2. **Application Performance Dashboard**
   - Message throughput per application
   - Processing latency percentiles
   - Error rates and trends

3. **Infrastructure Dashboard**
   - MQ queue depths
   - Kafka topic metrics
   - Resource utilization

### Alerting Rules

```yaml
# alerts.yml
groups:
- name: datastreaming.rules
  rules:
  - alert: HighErrorRate
    expr: datastreaming_error_rate > 0.05
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High error rate detected"
      
  - alert: ConsumerLag
    expr: datastreaming_consumer_lag > 1000
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "Consumer lag is high"
```

## 🧪 Testing Strategy

### Integration Testing Framework

```java
@SpringBootTest
@DataStreamingTest  // Custom test slice
class StreamingApplicationIT {
    
    @Autowired
    private TestMQSender mqSender;
    
    @Autowired  
    private TestKafkaConsumer kafkaConsumer;
    
    @Test
    void shouldProcessTransactionMessage() {
        // Send test message to MQ
        mqSender.send("TRANSACTION.QUEUE", createTestTransaction());
        
        // Verify message processed and sent to Kafka
        ConsumerRecord<String, String> record = kafkaConsumer.poll("transactions", 10000);
        assertThat(record).isNotNull();
        
        // Verify transformation
        Transaction transaction = parseAvroRecord(record.value());
        assertThat(transaction.getTransactionId()).isEqualTo("TEST-123");
    }
}
```

### Contract Testing

```java
@ContractTest
class MessageContractTest {
    
    @Test
    void shouldValidateSchemaEvolution() {
        SchemaCompatibilityTest.builder()
            .readerSchema("transaction-v2.avsc")
            .writerSchema("transaction-v1.avsc") 
            .expectCompatible()
            .run();
    }
}
```

## 📈 Performance Optimization

### Capacity Planning

**Throughput Calculations:**
```
Messages/sec = (Kafka batch size * Kafka batches/sec) / Message size
Worker threads = (Target throughput * Processing time) / 1000
Memory = (Buffer size + Queue capacity) * Average message size
```

**Example Configuration for 10K TPS:**
```yaml
datastreaming:
  sources:
    - consumerThreads: 50
      maxBatchSize: 1000
  kafka:
    batchSize: 262144    # 256KB
    lingerMs: 1
    bufferMemory: 1073741824  # 1GB
  performance:
    globalThreadPoolSize: 100
    queueCapacity: 100000
```

### JVM Tuning Guidelines

```bash
# Production JVM settings
export JAVA_OPTS="
  -Xms4g -Xmx8g
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=200
  -XX:+UseStringDeduplication
  -XX:+OptimizeStringConcat
  -Djava.net.preferIPv4Stack=true
  -Dcom.sun.management.jmxremote
  -Dcom.sun.management.jmxremote.port=9999
  -Dcom.sun.management.jmxremote.authenticate=false"
```

## 🔄 Lifecycle Management

### Framework Updates

1. **Version Management**
```xml
<!-- Applications specify framework version -->
<dependency>
    <groupId>com.datastreaming.framework</groupId>
    <artifactId>datastreaming-framework-starter</artifactId>
    <version>${datastreaming.framework.version}</version>
</dependency>
```

2. **Compatibility Matrix**
```
Framework Version | Spring Boot | Java | Kafka | MQ Client
1.0.0            | 3.2.x       | 17+  | 3.6.x | 9.3.x
1.1.0            | 3.2.x       | 17+  | 3.7.x | 9.3.x
```

3. **Migration Guide**
```markdown
# Migration from 1.0.0 to 1.1.0

## Breaking Changes
- None

## New Features  
- Enhanced Avro schema validation
- Improved error handling

## Configuration Changes
- New optional property: `datastreaming.validation.strict=true`
```

### Application Lifecycle

```bash
# Development workflow
git clone template-repo
./configure-app.sh customer-events
mvn spring-boot:run

# CI/CD Pipeline
mvn clean test
mvn spring-boot:build-image
docker push registry/customer-events:${BUILD_NUMBER}
kubectl apply -f k8s/

# Production deployment
helm upgrade customer-events ./helm-chart \
  --set image.tag=${BUILD_NUMBER} \
  --set environment=production
```

## 💡 Best Practices

### Application Design

1. **Keep Applications Focused**
   - One application per business domain
   - Single responsibility principle
   - Minimal custom code

2. **Configuration Management**
   - Use environment variables for secrets
   - Externalize all environment-specific settings
   - Version your configuration templates

3. **Error Handling**
   - Always configure dead letter queues
   - Implement proper retry strategies  
   - Monitor error rates and patterns

4. **Performance**
   - Start with conservative thread counts
   - Monitor and tune based on actual load
   - Use connection pooling appropriately

### Operational Excellence

1. **Monitoring**
   - Monitor ALL framework applications centrally
   - Set up proactive alerting
   - Implement log aggregation

2. **Security**
   - Regular security scans
   - Rotate credentials periodically
   - Follow principle of least privilege

3. **Disaster Recovery**
   - Document recovery procedures  
   - Test failover scenarios
   - Maintain configuration backups

## 📞 Support & Governance

### Support Tiers

**Tier 1: Self-Service**
- Framework documentation
- Sample applications
- FAQ and troubleshooting guides

**Tier 2: Community Support**  
- Internal forums/chat channels
- Knowledge sharing sessions
- Peer support network

**Tier 3: Expert Support**
- Framework team direct support
- Architecture reviews
- Performance optimization consulting

### Governance Model

**Framework Council:**
- Architecture representatives
- Security team member  
- Operations team member
- Framework maintainers

**Responsibilities:**
- Framework roadmap planning
- Breaking change approvals
- Security and compliance oversight
- Resource allocation decisions

### Success Metrics

**Technical Metrics:**
- Framework adoption rate
- Application deployment frequency
- Mean time to production
- Error rates and availability

**Business Metrics:**
- Development cost reduction
- Time to market improvement  
- Operational efficiency gains
- Compliance audit results

---

## 🎯 Getting Started Checklist

For new teams adopting the framework:

- [ ] Access to internal Maven repository
- [ ] Framework training completed
- [ ] Development environment setup
- [ ] Sample application deployed
- [ ] Production readiness review
- [ ] Monitoring and alerting configured
- [ ] Team added to support channels

**Welcome to the Data Streaming Framework ecosystem!**

Your organization now has a powerful, standardized foundation for all MQ-to-Kafka streaming needs. Focus on your business logic while the framework handles the infrastructure complexity.