# Distributed Tracking Implementation

## 🚀 Overview

This document describes the **distributed tracking and aggregate metrics** implementation for the Data Streaming Framework, enabling **cross-instance message reconciliation** and **cluster-wide monitoring** in multi-instance deployments.

## 📋 Key Features

### ✅ **Distributed Message Tracking**
- **Redis-based storage** for cross-instance message visibility
- **Real-time status updates** from any instance
- **Automatic retry mechanisms** with exponential backoff
- **Dead letter queue** support for failed messages

### ✅ **Aggregate Metrics Collection**  
- **Cluster-wide statistics** aggregated from all instances
- **Per-instance metrics** for detailed monitoring
- **Prometheus integration** for metrics scraping
- **Real-time health monitoring** across the cluster

### ✅ **Multi-Instance Health Monitoring**
- **Distributed health checks** with Redis connectivity tests
- **Instance staleness detection** (identifies stuck instances)
- **Cluster health assessment** with failure rate monitoring
- **Comprehensive health reports** for operational visibility

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Instance-1    │    │   Instance-2    │    │   Instance-3    │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │   Local     │ │    │ │   Local     │ │    │ │   Local     │ │
│ │ Processing  │ │    │ │ Processing  │ │    │ │ Processing  │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│        │        │    │        │        │    │        │        │
└────────┼────────┘    └────────┼────────┘    └────────┼────────┘
         │                      │                      │
         └──────────────────────┼──────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │                       │
                    │    Redis Cluster      │
                    │                       │
                    │ ┌─────────────────┐   │
                    │ │ Message Tracking│   │
                    │ │   streaming:    │   │
                    │ │   tracking:*    │   │
                    │ └─────────────────┘   │
                    │                       │
                    │ ┌─────────────────┐   │
                    │ │ Instance Metrics│   │
                    │ │   streaming:    │   │
                    │ │   metrics:*     │   │
                    │ └─────────────────┘   │
                    │                       │
                    │ ┌─────────────────┐   │
                    │ │Instance Registry│   │
                    │ │   streaming:    │   │
                    │ │   instance:*    │   │
                    │ └─────────────────┘   │
                    └───────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │                       │
                    │ Aggregate Monitoring  │
                    │                       │
                    │ • Prometheus Metrics  │
                    │ • Health Dashboards   │
                    │ • Alerting Rules      │
                    │ • Cluster Overview    │
                    └───────────────────────┘
```

## ⚙️ Configuration

### **Enable Distributed Mode**

```yaml
datastreaming:
  # Instance identification
  instance:
    id: "${HOSTNAME:-streaming-app}" # Unique instance identifier
    zone: "us-west-2a"               # Deployment zone
    environment: "production"        # Environment name
    labels:
      team: "data-platform"
      version: "1.0.0"
  
  # Reconciliation configuration
  reconciliation:
    enabled: true
    mode: "distributed"              # Enable distributed tracking
    reconciliationIntervalMs: 30000  # Check interval
    maxRetryAttempts: 5              # Max retry attempts
    retryBackoffMs: 5000             # Retry delay
    ttlHours: 24                     # Message retention in Redis
    
    # Distributed storage configuration
    distributed:
      storageType: "redis"
      connectionString: "redis://redis-cluster:6379"
      keyPrefix: "streaming:"
      enableLeaderElection: true
      heartbeatIntervalMs: 30000
      
      # Redis-specific settings
      redis:
        host: "redis-cluster.company.com"
        port: 6379
        password: "${REDIS_PASSWORD}"
        database: 0
        maxConnections: 20
        maxIdleConnections: 5
        maxWaitMs: 2000
        enableSsl: true

  # Monitoring configuration
  monitoring:
    metricsEnabled: true
    healthChecksEnabled: true
    metricsPrefix: "datastreaming"
    metricsIntervalMs: 30000
```

### **Production Redis Configuration**

```yaml
# Redis Cluster for High Availability
datastreaming:
  reconciliation:
    distributed:
      connectionString: "redis://redis-cluster-01:6379,redis-cluster-02:6379,redis-cluster-03:6379"
      redis:
        enableSsl: true
        password: "${REDIS_CLUSTER_PASSWORD}"
        maxConnections: 50
        maxIdleConnections: 10
```

## 🔧 Implementation Details

### **1. Distributed Message Tracker**

#### **Key Components:**
- **`DistributedMessageTracker`** - Main tracking implementation
- **Redis storage** with automatic TTL management
- **JSON serialization** for message status objects
- **Concurrent processing** with thread-safe operations

#### **Redis Data Structure:**
```redis
# Message tracking
streaming:tracking:correlation-123 → MessageStatus JSON
streaming:tracking:correlation-456 → MessageStatus JSON

# Instance registration  
streaming:instance:app-001 → InstanceInfo JSON
streaming:instance:app-002 → InstanceInfo JSON

# Instance metrics
streaming:metrics:app-001 → InstanceMetrics JSON
streaming:metrics:app-002 → InstanceMetrics JSON
```

#### **Message Lifecycle:**
1. **Track Message** → Store in Redis with PENDING status
2. **Process Message** → Update status to DELIVERED/FAILED
3. **Reconciliation** → Check timeouts and retry failed messages
4. **Cleanup** → Remove old entries after TTL expiration

### **2. Aggregate Metrics Collector**

#### **Functionality:**
- **Cross-instance aggregation** of metrics from Redis
- **Real-time calculation** of success/failure rates
- **Prometheus exposition** format support
- **Instance health monitoring** with staleness detection

#### **Collected Metrics:**
```prometheus
# Cluster-level metrics
datastreaming_cluster_active_instances 3
datastreaming_cluster_total_messages 10500
datastreaming_cluster_success_rate 0.98
datastreaming_cluster_failure_rate 0.02

# Per-instance metrics  
datastreaming_instance_messages_total{instance="app-001"} 3500
datastreaming_instance_messages_total{instance="app-002"} 3000
datastreaming_instance_messages_total{instance="app-003"} 4000

datastreaming_instance_pending_messages{instance="app-001"} 25
datastreaming_instance_pending_messages{instance="app-002"} 30
datastreaming_instance_pending_messages{instance="app-003"} 15
```

### **3. Multi-Instance Health Monitoring**

#### **Health Check Components:**
- **Redis connectivity** test
- **Distributed tracker** health verification  
- **Cluster-wide health** assessment
- **Instance staleness** detection (>5 minutes without updates)

#### **Health Endpoint Response:**
```json
{
  "status": "UP",
  "components": {
    "distributedStreamingHealth": {
      "status": "UP",
      "details": {
        "redis": "UP",
        "tracker": "UP", 
        "cluster": "HEALTHY",
        "activeInstances": 3,
        "instances": ["app-001", "app-002", "app-003"],
        "metrics": {
          "activeInstances": 3,
          "totalMessages": 10500,
          "successfulDeliveries": 10290,
          "failedDeliveries": 210,
          "pendingMessages": 70,
          "successRate": 0.98,
          "failureRate": 0.02
        },
        "instanceHealth": {
          "app-001": "HEALTHY",
          "app-002": "HEALTHY", 
          "app-003": "HEALTHY"
        },
        "overallStatus": "HEALTHY"
      }
    }
  }
}
```

## 🚀 Deployment Examples

### **Kubernetes Deployment**

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streaming-app
  namespace: data-platform
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
        image: streaming-app:1.0.0
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "distributed"
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: redis-credentials
              key: password
        - name: HOSTNAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - name: http
          containerPort: 8080
        - name: metrics
          containerPort: 8080
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
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10

---
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: streaming-app-service
  labels:
    app: streaming-app
spec:
  selector:
    app: streaming-app
  ports:
  - name: http
    port: 8080
    targetPort: 8080
  - name: metrics
    port: 8080
    targetPort: 8080

---
# servicemonitor.yaml  
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: streaming-app-metrics
spec:
  selector:
    matchLabels:
      app: streaming-app
  endpoints:
  - port: metrics
    path: /actuator/prometheus
    interval: 30s
```

### **Docker Compose (Development)**

```yaml
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --requirepass password123
    
  streaming-app-1:
    image: streaming-app:latest
    ports:
      - "8081:8080"
    environment:
      - SPRING_PROFILES_ACTIVE=distributed
      - REDIS_PASSWORD=password123
      - HOSTNAME=streaming-app-1
    depends_on:
      - redis
      
  streaming-app-2:
    image: streaming-app:latest  
    ports:
      - "8082:8080"
    environment:
      - SPRING_PROFILES_ACTIVE=distributed
      - REDIS_PASSWORD=password123
      - HOSTNAME=streaming-app-2
    depends_on:
      - redis
      
  streaming-app-3:
    image: streaming-app:latest
    ports:
      - "8083:8080" 
    environment:
      - SPRING_PROFILES_ACTIVE=distributed
      - REDIS_PASSWORD=password123
      - HOSTNAME=streaming-app-3
    depends_on:
      - redis

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
```

## 📊 Monitoring & Alerting

### **Prometheus Configuration**

```yaml
# prometheus.yml
global:
  scrape_interval: 30s

scrape_configs:
  - job_name: 'streaming-framework'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: 
        - 'streaming-app-1:8080'
        - 'streaming-app-2:8080' 
        - 'streaming-app-3:8080'
    scrape_interval: 30s
```

### **Grafana Dashboard Queries**

```promql
# Total messages processed across cluster
sum(datastreaming_cluster_total_messages)

# Cluster success rate
datastreaming_cluster_success_rate

# Active instances count
datastreaming_cluster_active_instances

# Messages per instance
sum by (instance) (datastreaming_instance_messages_total)

# Pending messages by instance
datastreaming_instance_pending_messages

# Instance health status
up{job="streaming-framework"}
```

### **Alerting Rules**

```yaml
# alerting-rules.yml
groups:
- name: streaming-framework
  rules:
  - alert: StreamingClusterDown
    expr: datastreaming_cluster_active_instances == 0
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "No streaming instances active"
      
  - alert: StreamingHighFailureRate
    expr: datastreaming_cluster_failure_rate > 0.05
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High failure rate: {{ $value }}"
      
  - alert: StreamingInstanceDown  
    expr: up{job="streaming-framework"} == 0
    for: 1m
    labels:
      severity: warning
    annotations:
      summary: "Streaming instance {{ $labels.instance }} is down"
      
  - alert: StreamingHighPendingMessages
    expr: sum(datastreaming_instance_pending_messages) > 1000
    for: 3m
    labels:
      severity: warning
    annotations:
      summary: "High pending messages: {{ $value }}"
```

## 🧪 Testing

### **Unit Tests**
- **`DistributedMessageTrackerTest`** - Redis tracking functionality
- **`AggregateMetricsCollectorTest`** - Metrics aggregation logic
- **`DistributedHealthIndicatorTest`** - Health monitoring components

### **Integration Testing**

```bash
# Build and test distributed components
mvn clean test -Dtest="*Distributed*Test"

# Start test environment
docker-compose -f docker-compose-test.yml up -d

# Run integration tests
mvn clean verify -Pintegration-tests
```

## 🚀 Benefits

### **✅ Cross-Instance Visibility**
- **Complete message tracking** across all instances
- **No blind spots** in reconciliation process
- **Centralized monitoring** of entire cluster

### **✅ High Availability** 
- **Redis cluster support** for storage redundancy
- **Automatic failover** with leader election
- **Instance failure detection** and recovery

### **✅ Operational Excellence**
- **Comprehensive monitoring** with Prometheus/Grafana
- **Proactive alerting** for system issues
- **Detailed health reporting** for troubleshooting

### **✅ Scalability**
- **Horizontal scaling** with automatic instance discovery  
- **Efficient Redis operations** with connection pooling
- **Optimized memory usage** with TTL cleanup

## 🔧 Migration Guide

### **From Local to Distributed Mode**

1. **Update Configuration:**
   ```yaml
   datastreaming:
     reconciliation:
       mode: "distributed"  # Change from "local"
   ```

2. **Deploy Redis:**
   ```bash
   helm install redis bitnami/redis-cluster
   ```

3. **Update Application:**
   ```bash
   # Build with Redis dependencies
   mvn clean install
   
   # Deploy with distributed configuration
   kubectl apply -f k8s/distributed-deployment.yml
   ```

4. **Verify Operation:**
   ```bash
   # Check health endpoints
   curl http://streaming-app:8080/actuator/health
   
   # Verify metrics
   curl http://streaming-app:8080/actuator/prometheus | grep datastreaming_cluster
   ```

---

**The distributed tracking implementation provides enterprise-grade reliability and monitoring capabilities for multi-instance Data Streaming Framework deployments, ensuring complete message visibility and operational excellence at scale.**