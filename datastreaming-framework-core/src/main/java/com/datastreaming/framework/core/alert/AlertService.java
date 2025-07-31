package com.datastreaming.framework.core.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Central service for managing system alerts and notifications
 */
@Service
@ConditionalOnProperty(name = "datastreaming.alerts.enabled", havingValue = "true", matchIfMissing = true)
public class AlertService {
    
    private static final Logger logger = LoggerFactory.getLogger(AlertService.class);
    
    @Value("${datastreaming.alerts.rateLimitMinutes:5}")
    private int rateLimitMinutes;
    
    @Value("${datastreaming.alerts.maxAlertsPerHour:100}")
    private int maxAlertsPerHour;
    
    @Value("${datastreaming.instance.id:${HOSTNAME:unknown}}")
    private String instanceId;
    
    @Autowired(required = false)
    private List<AlertNotifier> notifiers = new ArrayList<>();
    
    // Rate limiting for similar alerts
    private final Map<String, Instant> alertRateLimit = new ConcurrentHashMap<>();
    
    // Alert counters for monitoring
    private final AtomicLong totalAlerts = new AtomicLong(0);
    private final AtomicLong criticalAlerts = new AtomicLong(0);
    private final AtomicLong failedNotifications = new AtomicLong(0);
    
    // Recent alerts for health monitoring
    private final Map<String, Alert> recentAlerts = new ConcurrentHashMap<>();
    
    @PostConstruct
    public void initialize() {
        logger.info("Initializing Alert Service for instance: {} with {} notifiers", 
                   instanceId, notifiers.size());
        
        notifiers.forEach(notifier -> 
            logger.info("Registered alert notifier: {} (supports: {})", 
                       notifier.getName(), getSupportedLevels(notifier)));
    }
    
    /**
     * Send a critical alert - these are never rate limited
     */
    public void sendCriticalAlert(String source, String title, String message) {
        sendCriticalAlert(source, title, message, null, null);
    }
    
    public void sendCriticalAlert(String source, String title, String message, Map<String, Object> context) {
        sendCriticalAlert(source, title, message, context, null);
    }
    
    public void sendCriticalAlert(String source, String title, String message, Map<String, Object> context, Throwable cause) {
        Alert alert = new Alert(AlertLevel.CRITICAL, source, title, message, context, cause);
        processAlert(alert, false); // Never rate limit critical alerts
    }
    
    /**
     * Send a high priority alert
     */
    public void sendHighAlert(String source, String title, String message) {
        sendHighAlert(source, title, message, null, null);
    }
    
    public void sendHighAlert(String source, String title, String message, Map<String, Object> context, Throwable cause) {
        Alert alert = new Alert(AlertLevel.HIGH, source, title, message, context, cause);
        processAlert(alert, true);
    }
    
    /**
     * Send a warning alert
     */
    public void sendWarning(String source, String title, String message) {
        sendWarning(source, title, message, null);
    }
    
    public void sendWarning(String source, String title, String message, Map<String, Object> context) {
        Alert alert = new Alert(AlertLevel.WARNING, source, title, message, context, null);
        processAlert(alert, true);
    }
    
    /**
     * Send an informational alert
     */
    public void sendInfo(String source, String title, String message) {
        sendInfo(source, title, message, null);
    }
    
    public void sendInfo(String source, String title, String message, Map<String, Object> context) {
        Alert alert = new Alert(AlertLevel.INFO, source, title, message, context, null);
        processAlert(alert, true);
    }
    
    private void processAlert(Alert alert, boolean allowRateLimit) {
        try {
            // Check rate limiting
            if (allowRateLimit && isRateLimited(alert)) {
                logger.debug("Alert rate limited: {}", alert.getTitle());
                return;
            }
            
            // Update counters
            totalAlerts.incrementAndGet();
            if (alert.isCritical()) {
                criticalAlerts.incrementAndGet();
            }
            
            // Store recent alert for monitoring
            recentAlerts.put(alert.getId(), alert);
            
            // Add instance context
            Map<String, Object> enhancedContext = new HashMap<>(alert.getContext());
            enhancedContext.put("instanceId", instanceId);
            enhancedContext.put("timestamp", alert.getTimestamp().toString());
            
            Alert enhancedAlert = new Alert(alert.getLevel(), alert.getSource(), 
                                          alert.getTitle(), alert.getMessage(), 
                                          enhancedContext, alert.getCause());
            
            // Send alert asynchronously
            sendAlertAsync(enhancedAlert);
            
            // Log alert
            logAlert(alert);
            
        } catch (Exception e) {
            logger.error("Error processing alert: {}", alert.getTitle(), e);
        }
    }
    
    @Async
    private void sendAlertAsync(Alert alert) {
        List<AlertNotifier> applicableNotifiers = notifiers.stream()
            .filter(notifier -> notifier.supportsLevel(alert.getLevel()))
            .filter(AlertNotifier::isHealthy)
            .toList();
        
        if (applicableNotifiers.isEmpty()) {
            logger.warn("No healthy notifiers available for alert level: {}", alert.getLevel());
            failedNotifications.incrementAndGet();
            return;
        }
        
        for (AlertNotifier notifier : applicableNotifiers) {
            try {
                boolean success = notifier.sendAlert(alert);
                if (!success) {
                    logger.warn("Notifier {} failed to send alert: {}", notifier.getName(), alert.getTitle());
                    failedNotifications.incrementAndGet();
                }
            } catch (Exception e) {
                logger.error("Error sending alert via notifier {}: {}", notifier.getName(), alert.getTitle(), e);
                failedNotifications.incrementAndGet();
            }
        }
    }
    
    private boolean isRateLimited(Alert alert) {
        String rateLimitKey = generateRateLimitKey(alert);
        Instant now = Instant.now();
        Instant lastAlert = alertRateLimit.get(rateLimitKey);
        
        if (lastAlert == null) {
            alertRateLimit.put(rateLimitKey, now);
            return false;
        }
        
        Duration timeSinceLastAlert = Duration.between(lastAlert, now);
        if (timeSinceLastAlert.toMinutes() >= rateLimitMinutes) {
            alertRateLimit.put(rateLimitKey, now);
            return false;
        }
        
        return true;
    }
    
    private String generateRateLimitKey(Alert alert) {
        return alert.getSource() + ":" + alert.getTitle().hashCode();
    }
    
    private void logAlert(Alert alert) {
        String logMessage = String.format("[ALERT] %s - %s: %s", 
                                        alert.getSource(), alert.getTitle(), alert.getMessage());
        
        switch (alert.getLevel()) {
            case CRITICAL:
            case HIGH:
                if (alert.getCause() != null) {
                    logger.error(logMessage, alert.getCause());
                } else {
                    logger.error(logMessage);
                }
                break;
            case WARNING:
                logger.warn(logMessage);
                break;
            case INFO:
                logger.info(logMessage);
                break;
        }
    }
    
    private List<AlertLevel> getSupportedLevels(AlertNotifier notifier) {
        return Arrays.stream(AlertLevel.values())
            .filter(notifier::supportsLevel)
            .toList();
    }
    
    /**
     * Get alert service statistics
     */
    public AlertServiceStats getStats() {
        cleanupOldAlerts();
        
        return new AlertServiceStats(
            totalAlerts.get(),
            criticalAlerts.get(),
            failedNotifications.get(),
            recentAlerts.size(),
            notifiers.size(),
            (int) notifiers.stream().filter(AlertNotifier::isHealthy).count()
        );
    }
    
    /**
     * Check if alert service is healthy
     */
    public boolean isHealthy() {
        // Service is healthy if at least one notifier is available for critical alerts
        boolean hasCriticalNotifier = notifiers.stream()
            .anyMatch(notifier -> notifier.supportsLevel(AlertLevel.CRITICAL) && notifier.isHealthy());
        
        // Check if failure rate is acceptable (less than 20%)
        long total = totalAlerts.get();
        long failed = failedNotifications.get();
        double failureRate = total > 0 ? (double) failed / total : 0.0;
        
        return hasCriticalNotifier && failureRate < 0.2;
    }
    
    private void cleanupOldAlerts() {
        Instant cutoff = Instant.now().minus(Duration.ofHours(1));
        
        recentAlerts.entrySet().removeIf(entry -> 
            entry.getValue().getTimestamp().isBefore(cutoff));
        
        alertRateLimit.entrySet().removeIf(entry -> 
            entry.getValue().isBefore(cutoff));
    }
    
    /**
     * Statistics class for alert service monitoring
     */
    public static class AlertServiceStats {
        private final long totalAlerts;
        private final long criticalAlerts;
        private final long failedNotifications;
        private final int recentAlertsCount;
        private final int totalNotifiers;
        private final int healthyNotifiers;
        
        public AlertServiceStats(long totalAlerts, long criticalAlerts, long failedNotifications,
                               int recentAlertsCount, int totalNotifiers, int healthyNotifiers) {
            this.totalAlerts = totalAlerts;
            this.criticalAlerts = criticalAlerts;
            this.failedNotifications = failedNotifications;
            this.recentAlertsCount = recentAlertsCount;
            this.totalNotifiers = totalNotifiers;
            this.healthyNotifiers = healthyNotifiers;
        }
        
        public long getTotalAlerts() { return totalAlerts; }
        public long getCriticalAlerts() { return criticalAlerts; }
        public long getFailedNotifications() { return failedNotifications; }
        public int getRecentAlertsCount() { return recentAlertsCount; }
        public int getTotalNotifiers() { return totalNotifiers; }
        public int getHealthyNotifiers() { return healthyNotifiers; }
        
        public double getFailureRate() {
            return totalAlerts > 0 ? (double) failedNotifications / totalAlerts : 0.0;
        }
    }
}