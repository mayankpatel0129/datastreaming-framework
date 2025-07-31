package com.datastreaming.framework.core.alert;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Microsoft Teams-based alert notifier for critical and high-priority alerts
 */
@Component
@ConditionalOnProperty(name = "datastreaming.alerts.notifiers.teams.enabled", havingValue = "true")
public class TeamsAlertNotifier implements AlertNotifier {
    
    private static final Logger logger = LoggerFactory.getLogger(TeamsAlertNotifier.class);
    
    @Value("${datastreaming.alerts.notifiers.teams.webhookUrl}")
    private String webhookUrl;
    
    @Value("${datastreaming.alerts.notifiers.teams.title:Data Streaming Framework Alert}")
    private String cardTitle;
    
    @Value("${datastreaming.alerts.notifiers.teams.minLevel:HIGH}")
    private String minLevelStr;
    
    @Value("${datastreaming.instance.id:${HOSTNAME:unknown}}")
    private String instanceId;
    
    private AlertLevel minLevel;
    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;
    
    @PostConstruct
    public void initialize() {
        this.minLevel = AlertLevel.valueOf(minLevelStr.toUpperCase());
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
        
        logger.info("Initialized Teams notifier with webhook URL configured, minimum level: {}", minLevel);
    }
    
    @Override
    public boolean sendAlert(Alert alert) {
        try {
            Map<String, Object> teamsMessage = createTeamsMessage(alert);
            String jsonPayload = objectMapper.writeValueAsString(teamsMessage);
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            
            HttpEntity<String> request = new HttpEntity<>(jsonPayload, headers);
            
            ResponseEntity<String> response = restTemplate.exchange(
                webhookUrl, HttpMethod.POST, request, String.class);
            
            boolean success = response.getStatusCode().is2xxSuccessful();
            
            if (success) {
                logger.debug("Successfully sent Teams alert: {}", alert.getTitle());
            } else {
                logger.warn("Failed to send Teams alert: {} - Status: {}", 
                           alert.getTitle(), response.getStatusCode());
            }
            
            return success;
            
        } catch (Exception e) {
            logger.error("Error sending Teams alert: {}", alert.getTitle(), e);
            return false;
        }
    }
    
    private Map<String, Object> createTeamsMessage(Alert alert) {
        Map<String, Object> message = new HashMap<>();
        
        // Adaptive Card format for Teams
        message.put("@type", "MessageCard");
        message.put("@context", "https://schema.org/extensions");
        message.put("summary", alert.getTitle());
        message.put("themeColor", getThemeColorForLevel(alert.getLevel()));
        
        // Card sections
        List<Map<String, Object>> sections = new ArrayList<>();
        
        // Main section with alert details
        Map<String, Object> mainSection = new HashMap<>();
        mainSection.put("activityTitle", cardTitle);
        mainSection.put("activitySubtitle", String.format("**%s Alert from %s**", 
                        alert.getLevel().getDisplayName(), alert.getSource()));
        mainSection.put("activityImage", getActivityImageForLevel(alert.getLevel()));
        mainSection.put("text", String.format("**%s**\n\n%s", alert.getTitle(), alert.getMessage()));
        mainSection.put("markdown", true);
        
        sections.add(mainSection);
        
        // Facts section with alert metadata
        Map<String, Object> factsSection = new HashMap<>();
        List<Map<String, String>> facts = new ArrayList<>();
        
        facts.add(createFact("Alert Level", alert.getLevel().getDisplayName()));
        facts.add(createFact("Source", alert.getSource()));
        facts.add(createFact("Instance", instanceId));
        facts.add(createFact("Timestamp", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(alert.getTimestamp().atZone(java.time.ZoneOffset.UTC).toLocalDateTime())));
        facts.add(createFact("Alert ID", alert.getId()));
        
        // Add context information as facts
        if (!alert.getContext().isEmpty()) {
            alert.getContext().entrySet().forEach(entry -> {
                String key = formatFactKey(entry.getKey());
                String value = String.valueOf(entry.getValue());
                if (value.length() > 100) {
                    value = value.substring(0, 97) + "...";
                }
                facts.add(createFact(key, value));
            });
        }
        
        // Add error information if present
        if (alert.getCause() != null) {
            facts.add(createFact("Error Type", alert.getCause().getClass().getSimpleName()));
            String errorMessage = alert.getCause().getMessage();
            if (errorMessage != null) {
                if (errorMessage.length() > 150) {
                    errorMessage = errorMessage.substring(0, 147) + "...";
                }
                facts.add(createFact("Error Message", errorMessage));
            }
        }
        
        factsSection.put("facts", facts);
        sections.add(factsSection);
        
        message.put("sections", sections);
        
        // Add potential actions for critical alerts
        if (alert.isCritical()) {
            message.put("potentialAction", createPotentialActions(alert));
        }
        
        return message;
    }
    
    private Map<String, String> createFact(String name, String value) {
        Map<String, String> fact = new HashMap<>();
        fact.put("name", name);
        fact.put("value", value);
        return fact;
    }
    
    private String formatFactKey(String key) {
        // Convert camelCase to Title Case
        StringBuilder formatted = new StringBuilder();
        boolean capitalizeNext = true;
        
        for (char c : key.toCharArray()) {
            if (Character.isUpperCase(c) && formatted.length() > 0) {
                formatted.append(' ');
            }
            
            if (capitalizeNext) {
                formatted.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                formatted.append(Character.toLowerCase(c));
            }
        }
        
        return formatted.toString();
    }
    
    private String getThemeColorForLevel(AlertLevel level) {
        return switch (level) {
            case CRITICAL -> "FF0000"; // Red
            case HIGH -> "FF8C00";     // Dark Orange
            case WARNING -> "FFD700";  // Gold
            case INFO -> "00CED1";     // Dark Turquoise
        };
    }
    
    private String getActivityImageForLevel(AlertLevel level) {
        // Using standard emoji data URIs for consistency
        return switch (level) {
            case CRITICAL -> "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="; // Red dot
            case HIGH -> "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="; // Orange dot
            case WARNING -> "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="; // Yellow dot
            case INFO -> "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="; // Blue dot
        };
    }
    
    private List<Map<String, Object>> createPotentialActions(Alert alert) {
        List<Map<String, Object>> actions = new ArrayList<>();
        
        // Action to view logs (if log management system is available)
        Map<String, Object> viewLogsAction = new HashMap<>();
        viewLogsAction.put("@type", "OpenUri");
        viewLogsAction.put("name", "View Logs");
        viewLogsAction.put("targets", List.of(Map.of(
            "os", "default",
            "uri", "https://your-log-system.com/search?q=" + alert.getId()
        )));
        actions.add(viewLogsAction);
        
        // Action to view monitoring dashboard
        Map<String, Object> dashboardAction = new HashMap<>();
        dashboardAction.put("@type", "OpenUri");
        dashboardAction.put("name", "View Dashboard");
        dashboardAction.put("targets", List.of(Map.of(
            "os", "default",
            "uri", "https://your-monitoring-dashboard.com/datastreaming"
        )));
        actions.add(dashboardAction);
        
        // Action to view health status
        Map<String, Object> healthAction = new HashMap<>();
        healthAction.put("@type", "OpenUri");
        healthAction.put("name", "Check Health");
        healthAction.put("targets", List.of(Map.of(
            "os", "default",
            "uri", "http://" + instanceId + ":8080/actuator/health"
        )));
        actions.add(healthAction);
        
        return actions;
    }
    
    @Override
    public boolean supportsLevel(AlertLevel level) {
        return level.isHigherPriorityThan(minLevel) || level == minLevel;
    }
    
    @Override
    public String getName() {
        return "TeamsAlertNotifier";
    }
    
    @Override
    public boolean isHealthy() {
        try {
            // Simple health check - validate webhook URL format
            return webhookUrl != null && 
                   (webhookUrl.startsWith("https://outlook.office.com/webhook/") ||
                    webhookUrl.startsWith("https://outlook.office365.com/webhook/") ||
                    webhookUrl.contains("webhookb2"));
        } catch (Exception e) {
            logger.error("Teams notifier health check failed", e);
            return false;
        }
    }
}