package com.datastreaming.monitoring;

import com.datastreaming.consumer.MQConsumerManager;
import com.datastreaming.producer.HighThroughputKafkaProducer;
import com.datastreaming.reconciliation.MessageTracker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuator.health.Health;
import org.springframework.boot.actuator.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("transactionProcessor")
public class HealthIndicator implements org.springframework.boot.actuator.health.HealthIndicator {

    @Autowired
    private MQConsumerManager consumerManager;

    @Autowired
    private HighThroughputKafkaProducer kafkaProducer;

    @Autowired
    private MessageTracker messageTracker;

    @Autowired
    private MetricsCollector metricsCollector;

    @Override
    public Health health() {
        Health.Builder builder = Health.up();

        try {
            // Check MQ Consumer health
            boolean mqHealthy = consumerManager.isHealthy();
            builder.withDetail("mqConsumers", mqHealthy ? "UP" : "DOWN");

            // Check Kafka Producer health
            boolean kafkaHealthy = kafkaProducer.isHealthy();
            builder.withDetail("kafkaProducer", kafkaHealthy ? "UP" : "DOWN");

            // Check Message Tracker health
            boolean trackerHealthy = messageTracker.isHealthy();
            builder.withDetail("messageTracker", trackerHealthy ? "UP" : "DOWN");

            // Add metrics
            builder.withDetail("pendingMessages", metricsCollector.getPendingMessageCount());
            builder.withDetail("activeWorkers", metricsCollector.getActiveWorkerCount());

            // Overall health
            if (!mqHealthy || !kafkaHealthy || !trackerHealthy) {
                builder.down();
            }

        } catch (Exception e) {
            builder.down().withException(e);
        }

        return builder.build();
    }
}