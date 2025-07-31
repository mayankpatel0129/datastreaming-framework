package com.datastreaming.framework.core.health;

import com.datastreaming.framework.core.alert.AlertService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuator.health.Health;
import org.springframework.boot.actuator.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Health indicator that monitors the alert service health
 */
@Component
@ConditionalOnProperty(name = "datastreaming.alerts.enabled", havingValue = "true", matchIfMissing = true)
public class AlertingHealthIndicator implements HealthIndicator {
    
    @Autowired
    private AlertService alertService;
    
    @Override
    public Health health() {
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
                return Health.up()
                    .withDetails(details)
                    .build();
            } else {
                return Health.down()
                    .withDetail("reason", "Alert service unhealthy")
                    .withDetails(details)
                    .build();
            }
            
        } catch (Exception e) {
            return Health.down()
                .withDetail("error", e.getMessage())
                .build();
        }
    }
}