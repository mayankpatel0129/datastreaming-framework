package com.datastreaming.monitoring;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class MetricsCollector {

    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);

    @Autowired
    private MeterRegistry meterRegistry;

    // Message processing metrics
    private Counter messagesConsumedCounter;
    private Counter messagesProducedCounter;
    private Counter messagesFailedCounter;
    private Timer messageProcessingTimer;
    private Gauge pendingMessagesGauge;

    // Kafka specific metrics
    private Counter kafkaSuccessCounter;
    private Counter kafkaErrorCounter;
    private Timer kafkaProduceTimer;
    private final ConcurrentHashMap<String, Counter> topicCounters = new ConcurrentHashMap<>();

    // MQ specific metrics
    private final ConcurrentHashMap<String, Counter> queueCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> queueTimers = new ConcurrentHashMap<>();

    // Worker health metrics
    private final ConcurrentHashMap<String, Gauge> workerHealthGauges = new ConcurrentHashMap<>();

    // System metrics
    private final AtomicLong pendingMessageCount = new AtomicLong(0);
    private final AtomicLong activeWorkerCount = new AtomicLong(0);

    @PostConstruct
    public void initialize() {
        logger.info("Initializing Metrics Collector");
        
        initializeCounters();
        initializeTimers();
        initializeGauges();
        registerJvmMetrics();
        
        logger.info("Metrics Collector initialized with {} meters", meterRegistry.getMeters().size());
    }

    private void initializeCounters() {
        // Message processing counters
        messagesConsumedCounter = Counter.builder("messages.consumed.total")
                .description("Total number of messages consumed from MQ")
                .register(meterRegistry);

        messagesProducedCounter = Counter.builder("messages.produced.total")
                .description("Total number of messages produced to Kafka")
                .register(meterRegistry);

        messagesFailedCounter = Counter.builder("messages.failed.total")
                .description("Total number of message processing failures")
                .register(meterRegistry);

        // Kafka counters
        kafkaSuccessCounter = Counter.builder("kafka.messages.success")
                .description("Successful Kafka message deliveries")
                .register(meterRegistry);

        kafkaErrorCounter = Counter.builder("kafka.messages.error")
                .description("Failed Kafka message deliveries")
                .register(meterRegistry);
    }

    private void initializeTimers() {
        // Processing timers
        messageProcessingTimer = Timer.builder("message.processing.duration")
                .description("Time taken to process messages")
                .register(meterRegistry);

        kafkaProduceTimer = Timer.builder("kafka.produce.duration")
                .description("Time taken to produce messages to Kafka")
                .register(meterRegistry);
    }

    private void initializeGauges() {
        // System gauges
        pendingMessagesGauge = Gauge.builder("messages.pending")
                .description("Number of pending messages in the system")
                .register(meterRegistry, pendingMessageCount, AtomicLong::get);

        Gauge.builder("workers.active")
                .description("Number of active worker threads")
                .register(meterRegistry, activeWorkerCount, AtomicLong::get);
    }

    private void registerJvmMetrics() {
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmGcMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
    }

    // Message processing metrics
    public void recordMessageProcessed(String queueName) {
        messagesConsumedCounter.increment();
        getQueueCounter(queueName, "consumed").increment();
    }

    public void recordMessageProduced(String topic) {
        messagesProducedCounter.increment();
        getTopicCounter(topic, "produced").increment();
    }

    public void recordError(String source) {
        messagesFailedCounter.increment(Tags.of("source", source));
    }

    public Timer.Sample startMessageProcessingTimer() {
        return Timer.start(meterRegistry);
    }

    public void recordMessageProcessingTime(Timer.Sample sample) {
        sample.stop(messageProcessingTimer);
    }

    // Kafka specific metrics
    public void recordKafkaMessageSent(String topic) {
        getTopicCounter(topic, "sent").increment();
    }

    public void recordKafkaSuccess(String topic, int partition, long offset) {
        kafkaSuccessCounter.increment(Tags.of(
            "topic", topic,
            "partition", String.valueOf(partition)
        ));
    }

    public void recordKafkaError(String topic) {
        kafkaErrorCounter.increment(Tags.of("topic", topic));
    }

    public Timer.Sample startKafkaTimer() {
        return Timer.start(meterRegistry);
    }

    public void recordKafkaProduceTime(Timer.Sample sample) {
        sample.stop(kafkaProduceTimer);
    }

    // Worker health metrics
    public void recordWorkerHealthCheck(String queueName, int activeWorkers, int expectedWorkers) {
        double healthRatio = expectedWorkers > 0 ? (double) activeWorkers / expectedWorkers : 0.0;
        
        Gauge.builder("workers.health.ratio")
                .description("Worker health ratio")
                .tags("queue", queueName)
                .register(meterRegistry, () -> healthRatio);
        
        activeWorkerCount.set(activeWorkers);
    }

    // Reconciliation metrics
    public void recordReconciliationStats(int pending, int timeouts, int retries) {
        Gauge.builder("reconciliation.pending")
                .description("Pending reconciliation items")
                .register(meterRegistry, () -> pending);

        Counter.builder("reconciliation.timeouts")
                .description("Number of reconciliation timeouts")
                .register(meterRegistry)
                .increment(timeouts);

        Counter.builder("reconciliation.retries")
                .description("Number of reconciliation retries")
                .register(meterRegistry)
                .increment(retries);
        
        pendingMessageCount.set(pending);
    }

    // System metrics
    public void recordSystemMetrics() {
        // Memory usage
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        
        Gauge.builder("system.memory.used")
                .description("Used memory in bytes")
                .register(meterRegistry, () -> usedMemory);
        
        Gauge.builder("system.memory.free")
                .description("Free memory in bytes")
                .register(meterRegistry, () -> freeMemory);
        
        // Thread count
        int threadCount = Thread.activeCount();
        Gauge.builder("system.threads.active")
                .description("Active thread count")
                .register(meterRegistry, () -> threadCount);
    }

    // Throughput metrics
    public void recordThroughput(String component, long messagesPerSecond) {
        Gauge.builder("throughput.messages.per.second")
                .description("Messages processed per second")
                .tags("component", component)
                .register(meterRegistry, () -> messagesPerSecond);
    }

    // Error rate metrics
    public void recordErrorRate(String component, double errorRate) {
        Gauge.builder("error.rate")
                .description("Error rate percentage")
                .tags("component", component)
                .register(meterRegistry, () -> errorRate);
    }

    // Helper methods
    private Counter getQueueCounter(String queueName, String operation) {
        String key = queueName + ":" + operation;
        return queueCounters.computeIfAbsent(key, k -> 
            Counter.builder("mq.messages." + operation)
                    .description("MQ messages " + operation)
                    .tags("queue", queueName)
                    .register(meterRegistry)
        );
    }

    private Counter getTopicCounter(String topic, String operation) {
        String key = topic + ":" + operation;
        return topicCounters.computeIfAbsent(key, k -> 
            Counter.builder("kafka.messages." + operation)
                    .description("Kafka messages " + operation)
                    .tags("topic", topic)
                    .register(meterRegistry)
        );
    }

    private Timer getQueueTimer(String queueName) {
        return queueTimers.computeIfAbsent(queueName, k -> 
            Timer.builder("mq.processing.duration")
                    .description("MQ message processing duration")
                    .tags("queue", queueName)
                    .register(meterRegistry)
        );
    }

    // Health check metrics
    public void recordHealthCheck(String component, boolean isHealthy) {
        Gauge.builder("health.status")
                .description("Component health status (1=healthy, 0=unhealthy)")
                .tags("component", component)
                .register(meterRegistry, () -> isHealthy ? 1.0 : 0.0);
    }

    // Custom metrics for business logic
    public void recordCustomMetric(String metricName, String description, double value, Tags tags) {
        Gauge.builder(metricName)
                .description(description)
                .tags(tags)
                .register(meterRegistry, () -> value);
    }

    public void incrementCustomCounter(String counterName, String description, Tags tags) {
        Counter.builder(counterName)
                .description(description)
                .tags(tags)
                .register(meterRegistry)
                .increment();
    }

    // Metric export methods
    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public long getPendingMessageCount() {
        return pendingMessageCount.get();
    }

    public long getActiveWorkerCount() {
        return activeWorkerCount.get();
    }
}