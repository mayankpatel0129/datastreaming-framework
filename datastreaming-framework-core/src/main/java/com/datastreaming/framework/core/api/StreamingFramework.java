package com.datastreaming.framework.core.api;

import com.datastreaming.framework.core.config.StreamingConfiguration;

/**
 * Main API interface for the Data Streaming Framework
 * This is the primary entry point for framework consumers
 */
public interface StreamingFramework {

    /**
     * Initialize the streaming framework with the provided configuration
     * @param configuration The streaming configuration
     */
    void initialize(StreamingConfiguration configuration);

    /**
     * Start the streaming process
     */
    void start();

    /**
     * Stop the streaming process gracefully
     */
    void stop();

    /**
     * Check if the framework is currently running
     * @return true if running, false otherwise
     */
    boolean isRunning();

    /**
     * Get current framework statistics
     * @return Framework statistics
     */
    FrameworkStats getStats();

    /**
     * Check framework health
     * @return true if healthy, false otherwise
     */
    boolean isHealthy();
}