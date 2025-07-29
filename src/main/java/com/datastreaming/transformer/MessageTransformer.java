package com.datastreaming.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MessageTransformer {

    private static final Logger logger = LoggerFactory.getLogger(MessageTransformer.class);
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, ContractMapping> contractMappings = new ConcurrentHashMap<>();

    public String transform(Message message, String messageContract, String correlationId) throws Exception {
        ContractMapping mapping = getContractMapping(messageContract);
        
        // Extract message content
        String messageContent = extractMessageContent(message);
        
        // Apply transformation based on contract
        String transformedMessage = applyTransformation(messageContent, mapping, correlationId);
        
        logger.debug("Message transformed successfully for contract: {} and correlationId: {}", 
                    messageContract, correlationId);
        
        return transformedMessage;
    }

    private String extractMessageContent(Message message) throws Exception {
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        }
        
        // Handle other message types (BytesMessage, MapMessage, etc.)
        throw new UnsupportedOperationException("Message type not supported: " + message.getClass().getSimpleName());
    }

    private String applyTransformation(String messageContent, ContractMapping mapping, String correlationId) throws Exception {
        JsonNode inputNode = objectMapper.readTree(messageContent);
        
        // Create output JSON structure based on mapping
        JsonNode outputNode = objectMapper.createObjectNode();
        
        // Apply field mappings
        for (FieldMapping fieldMapping : mapping.getFieldMappings()) {
            applyFieldMapping(inputNode, (ObjectNode) outputNode, fieldMapping);
        }
        
        // Add metadata
        ((ObjectNode) outputNode).put("correlationId", correlationId);
        ((ObjectNode) outputNode).put("transformedAt", System.currentTimeMillis());
        ((ObjectNode) outputNode).put("sourceContract", mapping.getSourceContract());
        
        return objectMapper.writeValueAsString(outputNode);
    }

    private void applyFieldMapping(JsonNode input, ObjectNode output, FieldMapping mapping) {
        try {
            JsonNode sourceValue = input.at(mapping.getSourcePath());
            
            if (!sourceValue.isMissingNode()) {
                Object transformedValue = applyFieldTransformation(sourceValue, mapping);
                
                if (transformedValue != null) {
                    setValueAtPath(output, mapping.getTargetPath(), transformedValue);
                }
            } else if (mapping.isRequired()) {
                throw new IllegalArgumentException("Required field missing: " + mapping.getSourcePath());
            }
            
        } catch (Exception e) {
            logger.error("Error applying field mapping: {} -> {}", mapping.getSourcePath(), mapping.getTargetPath(), e);
            if (mapping.isRequired()) {
                throw new RuntimeException("Failed to map required field: " + mapping.getSourcePath(), e);
            }
        }
    }

    private Object applyFieldTransformation(JsonNode value, FieldMapping mapping) {
        if (mapping.getTransformationType() == null) {
            return getValueAsObject(value);
        }

        switch (mapping.getTransformationType()) {
            case "STRING":
                return value.asText();
            case "INTEGER":
                return value.asInt();
            case "LONG":
                return value.asLong();
            case "DOUBLE":
                return value.asDouble();
            case "BOOLEAN":
                return value.asBoolean();
            case "TIMESTAMP":
                return System.currentTimeMillis();
            case "UPPERCASE":
                return value.asText().toUpperCase();
            case "LOWERCASE":
                return value.asText().toLowerCase();
            default:
                return getValueAsObject(value);
        }
    }

    private Object getValueAsObject(JsonNode value) {
        if (value.isTextual()) return value.asText();
        if (value.isInt()) return value.asInt();
        if (value.isLong()) return value.asLong();
        if (value.isDouble()) return value.asDouble();
        if (value.isBoolean()) return value.asBoolean();
        return value.toString();
    }

    private void setValueAtPath(ObjectNode output, String path, Object value) {
        String[] pathParts = path.split("\\.");
        ObjectNode current = output;
        
        for (int i = 0; i < pathParts.length - 1; i++) {
            if (!current.has(pathParts[i])) {
                current.set(pathParts[i], objectMapper.createObjectNode());
            }
            current = (ObjectNode) current.get(pathParts[i]);
        }
        
        String finalField = pathParts[pathParts.length - 1];
        
        if (value instanceof String) {
            current.put(finalField, (String) value);
        } else if (value instanceof Integer) {
            current.put(finalField, (Integer) value);
        } else if (value instanceof Long) {
            current.put(finalField, (Long) value);
        } else if (value instanceof Double) {
            current.put(finalField, (Double) value);
        } else if (value instanceof Boolean) {
            current.put(finalField, (Boolean) value);
        } else {
            current.put(finalField, value.toString());
        }
    }

    private ContractMapping getContractMapping(String contractName) {
        return contractMappings.computeIfAbsent(contractName, this::loadContractMapping);
    }

    private ContractMapping loadContractMapping(String contractName) {
        // In a real implementation, this would load from configuration files or database
        // For now, create a default mapping
        return createDefaultMapping(contractName);
    }

    private ContractMapping createDefaultMapping(String contractName) {
        ContractMapping mapping = new ContractMapping();
        mapping.setSourceContract(contractName);
        mapping.setTargetContract("kafka-standard");
        
        // Add some default field mappings
        mapping.addFieldMapping(new FieldMapping("/messageId", "id", "STRING", true));
        mapping.addFieldMapping(new FieldMapping("/payload", "data", null, true));
        mapping.addFieldMapping(new FieldMapping("/timestamp", "timestamp", "TIMESTAMP", false));
        
        return mapping;
    }

    public void reloadContractMappings() {
        contractMappings.clear();
        logger.info("Contract mappings reloaded");
    }
}