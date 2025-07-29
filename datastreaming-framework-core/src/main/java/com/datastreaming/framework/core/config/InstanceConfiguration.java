package com.datastreaming.framework.core.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for instance-specific settings
 */
public class InstanceConfiguration {
    
    private String id = "${HOSTNAME:unknown}"; // Instance identifier
    private String zone = "default"; // Deployment zone/region
    private String environment = "production"; // Environment name
    private Map<String, String> labels = new HashMap<>(); // Custom labels for instance
    
    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getZone() { return zone; }
    public void setZone(String zone) { this.zone = zone; }
    
    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }
    
    public Map<String, String> getLabels() { return labels; }
    public void setLabels(Map<String, String> labels) { this.labels = labels; }
}