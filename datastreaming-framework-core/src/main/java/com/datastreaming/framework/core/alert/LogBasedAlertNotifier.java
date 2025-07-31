package com.datastreaming.framework.core.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Default alert notifier that logs alerts with structured format for log aggregation systems
 */
@Component
@ConditionalOnProperty(name = "datastreaming.alerts.notifiers.log.enabled", havingValue = "true", matchIfMissing = true)
public class LogBasedAlertNotifier implements AlertNotifier {
    
    private static final Logger alertLogger = LoggerFactory.getLogger("ALERTS");
    private static final Logger logger = LoggerFactory.getLogger(LogBasedAlertNotifier.class);
    
    @Override
    public boolean sendAlert(Alert alert) {
        try {
            String structuredLog = formatStructuredAlert(alert);
            
            switch (alert.getLevel()) {
                case CRITICAL:
                case HIGH:
                    alertLogger.error(structuredLog, alert.getCause());
                    break;
                case WARNING:
                    alertLogger.warn(structuredLog);
                    break;
                case INFO:
                    alertLogger.info(structuredLog);
                    break;
            }
            
            return true;
            
        } catch (Exception e) {
            logger.error("Failed to log alert: {}", alert.getTitle(), e);
            return false;
        }
    }
    
    private String formatStructuredAlert(Alert alert) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALERT_ID=").append(alert.getId())
          .append(" LEVEL=").append(alert.getLevel().getDisplayName())
          .append(" SOURCE=").append(alert.getSource())
          .append(" TITLE=\"").append(alert.getTitle()).append("\"")
          .append(" MESSAGE=\"").append(alert.getMessage()).append("\"")
          .append(" TIMESTAMP=").append(alert.getTimestamp());
        
        if (!alert.getContext().isEmpty()) {
            sb.append(" CONTEXT={");
            alert.getContext().entrySet().forEach(entry -> 
                sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" "));
            sb.append("}");
        }
        
        return sb.toString();
    }
    
    @Override
    public boolean supportsLevel(AlertLevel level) {
        return true; // Log notifier supports all levels
    }
    
    @Override
    public String getName() {
        return "LogBasedAlertNotifier";
    }
    
    @Override
    public boolean isHealthy() {
        return true; // Log notifier is always healthy
    }
}