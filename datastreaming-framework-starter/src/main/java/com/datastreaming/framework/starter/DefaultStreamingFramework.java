package com.datastreaming.framework.starter;

import com.datastreaming.framework.core.api.FrameworkStats;
import com.datastreaming.framework.core.api.StreamingFramework;
import com.datastreaming.framework.core.config.StreamingConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of StreamingFramework
 */
@Component
public class DefaultStreamingFramework implements StreamingFramework {

    private static final Logger logger = LoggerFactory.getLogger(DefaultStreamingFramework.class);

    @Autowired
    private DataStreamingProperties properties;

    @Autowired
    private StreamingConfigurationConverter configurationConverter;

    private StreamingConfiguration configuration;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicLong totalMessages = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private Instant startTime;

    @PostConstruct
    public void postConstruct() {
        if (properties.isEnabled()) {
            logger.info("Data Streaming Framework is enabled, initializing...");
            this.configuration = configurationConverter.convert(properties);
            initialize(configuration);
        } else {
            logger.info("Data Streaming Framework is disabled");
        }
    }

    @Override
    public void initialize(StreamingConfiguration configuration) {
        if (initialized.compareAndSet(false, true)) {
            this.configuration = configuration;
            logger.info("Streaming Framework initialized with {} sources", 
                       configuration.getSources().size());
        }
    }

    @Override
    public void start() {
        if (!initialized.get()) {
            throw new IllegalStateException("Framework not initialized");
        }

        if (running.compareAndSet(false, true)) {
            startTime = Instant.now();
            logger.info("Starting Streaming Framework...");
            
            // In a real implementation, this would start the actual consumers
            // For now, we'll simulate the startup
            
            logger.info("Streaming Framework started successfully");
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping Streaming Framework...");
            
            // In a real implementation, this would stop consumers gracefully
            
            logger.info("Streaming Framework stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public FrameworkStats getStats() {
        Map<String, Object> additionalMetrics = new HashMap<>();
        additionalMetrics.put("sourcesConfigured", configuration != null ? configuration.getSources().size() : 0);
        additionalMetrics.put("frameworkVersion", "1.0.0");

        return new FrameworkStats(
            totalMessages.get(),
            totalErrors.get(),
            0L, // pending messages
            isRunning() ? 1 : 0, // active workers
            1, // total workers
            startTime != null ? startTime : Instant.now(),
            additionalMetrics
        );
    }

    @Override
    public boolean isHealthy() {
        return initialized.get() && (configuration != null);
    }

    @PreDestroy
    public void destroy() {
        stop();
    }
}