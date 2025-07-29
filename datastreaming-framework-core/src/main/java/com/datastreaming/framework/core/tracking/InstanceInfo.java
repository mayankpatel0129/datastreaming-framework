package com.datastreaming.framework.core.tracking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Information about a streaming framework instance
 */
public class InstanceInfo {
    
    private final String instanceId;
    private final long registeredAt;
    private final String status;
    
    @JsonCreator
    public InstanceInfo(
            @JsonProperty("instanceId") String instanceId,
            @JsonProperty("registeredAt") long registeredAt,
            @JsonProperty("status") String status) {
        this.instanceId = instanceId;
        this.registeredAt = registeredAt;
        this.status = status;
    }
    
    public String getInstanceId() { return instanceId; }
    public long getRegisteredAt() { return registeredAt; }
    public String getStatus() { return status; }
}