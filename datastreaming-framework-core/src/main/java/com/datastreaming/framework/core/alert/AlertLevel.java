package com.datastreaming.framework.core.alert;

/**
 * Alert severity levels for framework notifications
 */
public enum AlertLevel {
    /**
     * Critical system failures requiring immediate attention
     */
    CRITICAL("CRITICAL", 1),
    
    /**
     * High priority issues affecting system performance
     */
    HIGH("HIGH", 2),
    
    /**
     * Warning conditions that may lead to issues
     */
    WARNING("WARNING", 3),
    
    /**
     * Informational alerts for system events
     */
    INFO("INFO", 4);
    
    private final String displayName;
    private final int priority;
    
    AlertLevel(String displayName, int priority) {
        this.displayName = displayName;
        this.priority = priority;
    }
    
    public String getDisplayName() {
        return displayName;
    }
    
    public int getPriority() {
        return priority;
    }
    
    public boolean isHigherPriorityThan(AlertLevel other) {
        return this.priority < other.priority;
    }
}