package com.datastreaming.framework.core.alert;

/**
 * Interface for sending alerts to external systems
 */
public interface AlertNotifier {
    
    /**
     * Send alert to the notification system
     * 
     * @param alert the alert to send
     * @return true if notification was sent successfully, false otherwise
     */
    boolean sendAlert(Alert alert);
    
    /**
     * Check if this notifier supports the given alert level
     * 
     * @param level the alert level to check
     * @return true if this notifier handles the alert level
     */
    boolean supportsLevel(AlertLevel level);
    
    /**
     * Get the name of this notifier for identification
     * 
     * @return notifier name
     */
    String getName();
    
    /**
     * Check if this notifier is currently healthy and available
     * 
     * @return true if notifier is healthy
     */
    boolean isHealthy();
}