package com.datastreaming.config;

import com.datastreaming.consumer.MQConsumerManager;
import com.datastreaming.producer.HighThroughputKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class ApplicationLifecycleManager {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationLifecycleManager.class);

    @Autowired
    private ProfileManager profileManager;

    @Autowired
    private MQConsumerManager consumerManager;

    @Autowired
    private HighThroughputKafkaProducer kafkaProducer;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        logger.info("Application ready - starting transaction processor");
        
        try {
            // Initialize Kafka producer with profile configuration
            ProcessorProfile profile = profileManager.getActiveProfile();
            kafkaProducer.initialize(profile.getKafkaConfiguration());
            
            logger.info("Transaction processor started successfully with profile: {}", 
                       profile.getProfileName());
            
        } catch (Exception e) {
            logger.error("Failed to start transaction processor", e);
            throw new RuntimeException("Application startup failed", e);
        }
    }

    @EventListener(ContextClosedEvent.class)
    public void onApplicationShutdown() {
        logger.info("Application shutdown initiated - performing graceful shutdown");
        
        try {
            // Stop consumers first
            consumerManager.stopConsumers();
            
            // Flush and shutdown Kafka producer
            kafkaProducer.flush();
            
            logger.info("Graceful shutdown completed");
            
        } catch (Exception e) {
            logger.error("Error during graceful shutdown", e);
        }
    }
}