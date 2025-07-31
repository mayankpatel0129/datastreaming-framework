package com.datastreaming.framework.core.mq;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Represents an MQ message contract defining the structure and format
 */
public class MQContract {
    
    private final String name;
    private final String version;
    private final String description;
    private final MQMessageFormat format;
    private final List<MQField> fields;
    private final Map<String, String> properties;
    
    @JsonCreator
    public MQContract(
            @JsonProperty("name") String name,
            @JsonProperty("version") String version,
            @JsonProperty("description") String description,
            @JsonProperty("format") MQMessageFormat format,
            @JsonProperty("fields") List<MQField> fields,
            @JsonProperty("properties") Map<String, String> properties) {
        this.name = name;
        this.version = version;
        this.description = description;
        this.format = format;
        this.fields = fields;
        this.properties = properties != null ? properties : Map.of();
    }
    
    // Getters
    public String getName() { return name; }
    public String getVersion() { return version; }
    public String getDescription() { return description; }
    public MQMessageFormat getFormat() { return format; }
    public List<MQField> getFields() { return fields; }
    public Map<String, String> getProperties() { return properties; }
    
    // Convenience methods
    public String getProperty(String key) {
        return properties.get(key);
    }
    
    public String getProperty(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }
    
    public String getDelimiter() {
        return getProperty("delimiter", ",");
    }
    
    public String getEncoding() {
        return getProperty("encoding", "UTF-8");
    }
    
    public boolean isTrimFields() {
        return Boolean.parseBoolean(getProperty("trimFields", "true"));
    }
    
    public boolean isStrictMode() {
        return Boolean.parseBoolean(getProperty("strictMode", "false"));
    }
    
    public String getContractId() {
        return name + "-" + version;
    }
    
    @Override
    public String toString() {
        return "MQContract{" +
                "name='" + name + '\'' +
                ", version='" + version + '\'' +
                ", format=" + format +
                ", fields=" + fields.size() +
                '}';
    }
}