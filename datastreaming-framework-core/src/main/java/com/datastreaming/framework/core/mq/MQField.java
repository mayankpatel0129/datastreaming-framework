package com.datastreaming.framework.core.mq;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a field in an MQ message contract
 */
public class MQField {
    
    private final String name;
    private final MQFieldType type;
    private final int position;        // For fixed-length format: start position (0-based)
    private final int length;          // For fixed-length format: field length
    private final boolean required;
    private final String defaultValue;
    private final String description;
    private final String pattern;      // Regex pattern for validation
    private final String dateFormat;   // Date format for DATE/DATETIME fields
    
    @JsonCreator
    public MQField(
            @JsonProperty("name") String name,
            @JsonProperty("type") MQFieldType type,
            @JsonProperty("position") int position,
            @JsonProperty("length") int length,
            @JsonProperty("required") boolean required,
            @JsonProperty("defaultValue") String defaultValue,
            @JsonProperty("description") String description,
            @JsonProperty("pattern") String pattern,
            @JsonProperty("dateFormat") String dateFormat) {
        this.name = name;
        this.type = type != null ? type : MQFieldType.STRING;
        this.position = position;
        this.length = length;
        this.required = required;
        this.defaultValue = defaultValue;
        this.description = description;
        this.pattern = pattern;
        this.dateFormat = dateFormat;
    }
    
    // Builder pattern for easier construction
    public static Builder builder(String name, MQFieldType type) {
        return new Builder(name, type);
    }
    
    // Getters
    public String getName() { return name; }
    public MQFieldType getType() { return type; }
    public int getPosition() { return position; }
    public int getLength() { return length; }
    public boolean isRequired() { return required; }
    public String getDefaultValue() { return defaultValue; }
    public String getDescription() { return description; }
    public String getPattern() { return pattern; }
    public String getDateFormat() { return dateFormat; }
    
    // Calculated properties
    public int getEndPosition() {
        return position + length;
    }
    
    public boolean hasDefaultValue() {
        return defaultValue != null && !defaultValue.isEmpty();
    }
    
    public boolean hasPattern() {
        return pattern != null && !pattern.isEmpty();
    }
    
    public boolean hasDateFormat() {
        return dateFormat != null && !dateFormat.isEmpty();
    }
    
    @Override
    public String toString() {
        return "MQField{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", position=" + position +
                ", length=" + length +
                ", required=" + required +
                '}';
    }
    
    /**
     * Builder class for MQField
     */
    public static class Builder {
        private final String name;
        private final MQFieldType type;
        private int position = 0;
        private int length = 0;
        private boolean required = false;
        private String defaultValue;
        private String description;
        private String pattern;
        private String dateFormat;
        
        private Builder(String name, MQFieldType type) {
            this.name = name;
            this.type = type;
        }
        
        public Builder position(int position) {
            this.position = position;
            return this;
        }
        
        public Builder length(int length) {
            this.length = length;
            return this;
        }
        
        public Builder required(boolean required) {
            this.required = required;
            return this;
        }
        
        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }
        
        public Builder description(String description) {
            this.description = description;
            return this;
        }
        
        public Builder pattern(String pattern) {
            this.pattern = pattern;
            return this;
        }
        
        public Builder dateFormat(String dateFormat) {
            this.dateFormat = dateFormat;
            return this;
        }
        
        public MQField build() {
            return new MQField(name, type, position, length, required, 
                             defaultValue, description, pattern, dateFormat);
        }
    }
}