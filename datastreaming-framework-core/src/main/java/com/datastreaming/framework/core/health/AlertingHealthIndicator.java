package com.datastreaming.framework.core.health;

import com.datastreaming.framework.core.alert.AlertService;
import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.boot.actuator.health.Health;
// import org.springframework.boot.actuator.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Health indicator that monitors the alert service health
 */
@Component
@ConditionalOnProperty(name = "datastreaming.alerts.enabled", havingValue = "true", matchIfMissing = true)
public class AlertingHealthIndicator /* implements HealthIndicator */ {
    
    @Autowired
    private AlertService alertService;
    
    // @Override
    public Object health() {
        try {
            AlertService.AlertServiceStats stats = alertService.getStats();
            boolean isHealthy = alertService.isHealthy();
            
            Map<String, Object> details = Map.of(
                "totalAlerts", stats.getTotalAlerts(),
                "criticalAlerts", stats.getCriticalAlerts(),
                "failedNotifications", stats.getFailedNotifications(),
                "recentAlertsCount", stats.getRecentAlertsCount(),
                "totalNotifiers", stats.getTotalNotifiers(),
                "healthyNotifiers", stats.getHealthyNotifiers(),
                "failureRate", String.format("%.2f%%", stats.getFailureRate() * 100)
            );
            
            if (isHealthy) {
                return Map.of("status", "UP", "details", details);
            } else {
                return Map.of("status", "DOWN", "reason", "Alert service unhealthy", "details", details);
            }
            
        } catch (Exception e) {
            return Map.of("status", "DOWN", "error", e.getMessage());
        }
    }
}