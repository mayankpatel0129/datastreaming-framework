package com.datastreaming.framework.core.alert;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a system alert with context information
 */
public class Alert {
    private final String id;
    private final AlertLevel level;
    private final String source;
    private final String title;
    private final String message;
    private final Instant timestamp;
    private final Map<String, Object> context;
    private final Throwable cause;
    
    public Alert(AlertLevel level, String source, String title, String message) {
        this(level, source, title, message, null, null);
    }
    
    public Alert(AlertLevel level, String source, String title, String message, Map<String, Object> context) {
        this(level, source, title, message, context, null);
    }
    
    public Alert(AlertLevel level, String source, String title, String message, Map<String, Object> context, Throwable cause) {
        this.id = UUID.randomUUID().toString();
        this.level = Objects.requireNonNull(level, "Alert level cannot be null");
        this.source = Objects.requireNonNull(source, "Alert source cannot be null");
        this.title = Objects.requireNonNull(title, "Alert title cannot be null");
        this.message = Objects.requireNonNull(message, "Alert message cannot be null");
        this.context = context != null ? Map.copyOf(context) : Map.of();
        this.timestamp = Instant.now();
        this.cause = cause;
    }
    
    public String getId() {
        return id;
    }
    
    public AlertLevel getLevel() {
        return level;
    }
    
    public String getSource() {
        return source;
    }
    
    public String getTitle() {
        return title;
    }
    
    public String getMessage() {
        return message;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public Map<String, Object> getContext() {
        return context;
    }
    
    public Throwable getCause() {
        return cause;
    }
    
    public boolean isCritical() {
        return level == AlertLevel.CRITICAL;
    }
    
    public boolean isHigh() {
        return level == AlertLevel.HIGH;
    }
    
    @Override
    public String toString() {
        return String.format("[%s] %s - %s: %s", 
            level.getDisplayName(), source, title, message);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Alert alert = (Alert) o;
        return Objects.equals(id, alert.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}