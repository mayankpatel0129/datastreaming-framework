package com.datastreaming.framework.core.mq.parsers;

import com.datastreaming.framework.core.mq.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Parser for JSON format MQ messages
 * Supports nested JSON structures with contract-based field mapping
 */
@Component
public class JsonMessageParser implements MQMessageParser {
    
    private static final Logger logger = LoggerFactory.getLogger(JsonMessageParser.class);
    
    private final ObjectMapper objectMapper;
    
    public JsonMessageParser() {
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public boolean supports(MQMessageFormat format) {
        return MQMessageFormat.JSON == format;
    }
    
    @Override
    public MQMessageFormat getSupportedFormat() {
        return MQMessageFormat.JSON;
    }
    
    @Override
    public Map<String, Object> parse(String messageContent, MQContract contract) throws MQMessageParsingException {
        logger.debug("Parsing JSON message with contract: {}", contract.getName());
        
        try {
            // Parse JSON content
            JsonNode rootNode = objectMapper.readTree(messageContent);
            
            Map<String, Object> result = new HashMap<>();
            
            // Process each field according to contract
            for (MQField field : contract.getFields()) {
                try {
                    Object value = extractField(rootNode, field, contract);
                    result.put(field.getName(), value);
                } catch (Exception e) {
                    logger.error("Failed to parse field '{}' in contract '{}'", field.getName(), contract.getName(), e);
                    
                    if (field.isRequired()) {
                        throw new MQMessageParsingException(
                            "Failed to parse required field '" + field.getName() + "': " + e.getMessage(),
                            messageContent, contract.getName(), e
                        );
                    } else {
                        // Use default value for optional fields
                        result.put(field.getName(), getDefaultValue(field));
                        logger.warn("Using default value for optional field '{}': {}", 
                                   field.getName(), result.get(field.getName()));
                    }
                }
            }
            
            logger.debug("Successfully parsed JSON message with {} fields", result.size());
            return result;
            
        } catch (Exception e) {
            logger.error("Failed to parse JSON message with contract '{}'", contract.getName(), e);
            throw new MQMessageParsingException(
                "Failed to parse JSON message: " + e.getMessage(),
                messageContent, contract.getName(), e
            );
        }
    }
    
    private Object extractField(JsonNode rootNode, MQField field, MQContract contract) 
            throws MQMessageParsingException {
        
        // Get JSON path or use field name as default
        String jsonPath = field.hasJsonPath() ? field.getJsonPath() : field.getName();
        
        // Navigate to the field using JSON path (supports dot notation)
        JsonNode fieldNode = navigateJsonPath(rootNode, jsonPath);
        
        // Handle missing or null values
        if (fieldNode == null || fieldNode.isNull()) {
            if (field.isRequired() && contract.isStrictMode()) {
                throw new MQMessageParsingException(
                    "Required field '" + field.getName() + "' is missing or null at path: " + jsonPath,
                    null, contract.getName()
                );
            } else {
                return getDefaultValue(field);
            }
        }
        
        // Handle empty strings
        if (fieldNode.isTextual() && fieldNode.asText().trim().isEmpty()) {
            if (field.isRequired() && contract.isStrictMode()) {
                throw new MQMessageParsingException(
                    "Required field '" + field.getName() + "' is empty at path: " + jsonPath,
                    null, contract.getName()
                );
            } else {
                return getDefaultValue(field);
            }
        }
        
        // Get raw value as string
        String rawValue = fieldNode.isTextual() ? fieldNode.asText() : fieldNode.toString();
        
        // Trim if configured
        if (contract.isTrimFields()) {
            rawValue = rawValue.trim();
        }
        
        // Validate pattern if specified
        if (field.hasPattern()) {
            if (!Pattern.matches(field.getPattern(), rawValue)) {
                throw new MQMessageParsingException(
                    "Field '" + field.getName() + "' value '" + rawValue + "' does not match pattern: " + field.getPattern(),
                    null, contract.getName()
                );
            }
        }
        
        // Convert to appropriate type
        return convertValue(fieldNode, rawValue, field, contract);
    }
    
    private JsonNode navigateJsonPath(JsonNode rootNode, String jsonPath) {
        String[] pathParts = jsonPath.split("\\.");
        JsonNode currentNode = rootNode;
        
        for (String part : pathParts) {
            if (currentNode == null) {
                return null;
            }
            
            // Handle array access like "items[0]" or "items[*]"
            if (part.contains("[") && part.contains("]")) {
                String arrayName = part.substring(0, part.indexOf('['));
                String indexPart = part.substring(part.indexOf('[') + 1, part.indexOf(']'));
                
                currentNode = currentNode.get(arrayName);
                if (currentNode != null && currentNode.isArray()) {
                    if ("*".equals(indexPart)) {
                        // For arrays with *, return the first element or handle differently
                        currentNode = currentNode.size() > 0 ? currentNode.get(0) : null;
                    } else {
                        try {
                            int index = Integer.parseInt(indexPart);
                            currentNode = currentNode.get(index);
                        } catch (NumberFormatException e) {
                            return null;
                        }
                    }
                }
            } else {
                currentNode = currentNode.get(part);
            }
        }
        
        return currentNode;
    }
    
    private Object convertValue(JsonNode fieldNode, String rawValue, MQField field, MQContract contract) 
            throws MQMessageParsingException {
        
        try {
            switch (field.getType()) {
                case STRING:
                    return rawValue;
                    
                case INTEGER:
                    return fieldNode.isNumber() ? fieldNode.asInt() : Integer.parseInt(rawValue);
                    
                case LONG:
                    return fieldNode.isNumber() ? fieldNode.asLong() : Long.parseLong(rawValue);
                    
                case DECIMAL:
                    if (fieldNode.isNumber()) {
                        return fieldNode.decimalValue();
                    } else {
                        return new BigDecimal(rawValue);
                    }
                    
                case BOOLEAN:
                    if (fieldNode.isBoolean()) {
                        return fieldNode.asBoolean();
                    } else {
                        return convertToBoolean(rawValue);
                    }
                    
                case DATE:
                    String dateFormat = field.hasDateFormat() ? field.getDateFormat() : "yyyy-MM-dd";
                    return LocalDate.parse(rawValue, DateTimeFormatter.ofPattern(dateFormat));
                    
                case DATETIME:
                    String datetimeFormat = field.hasDateFormat() ? field.getDateFormat() : "yyyy-MM-dd'T'HH:mm:ss";
                    return LocalDateTime.parse(rawValue, DateTimeFormatter.ofPattern(datetimeFormat));
                    
                case TIME:
                    String timeFormat = field.hasDateFormat() ? field.getDateFormat() : "HH:mm:ss";
                    return LocalTime.parse(rawValue, DateTimeFormatter.ofPattern(timeFormat));
                    
                case BINARY:
                    try {
                        return rawValue.getBytes(contract.getEncoding());
                    } catch (java.io.UnsupportedEncodingException e) {
                        throw new MQMessageParsingException(
                            "Unsupported encoding '" + contract.getEncoding() + "' for field '" + field.getName() + "'",
                            null, contract.getName(), e
                        );
                    }
                    
                default:
                    logger.warn("Unknown field type: {} for field: {}", field.getType(), field.getName());
                    return rawValue;
            }
            
        } catch (Exception e) {
            throw new MQMessageParsingException(
                "Failed to convert field '" + field.getName() + "' value '" + rawValue + "' to type " + field.getType(),
                null, contract.getName(), e
            );
        }
    }
    
    private boolean convertToBoolean(String value) {
        String normalizedValue = value.toLowerCase().trim();
        
        // Handle various boolean representations
        switch (normalizedValue) {
            case "true":
            case "t":
            case "yes":
            case "y":
            case "1":
                return true;
            case "false":
            case "f":
            case "no":
            case "n":
            case "0":
                return false;
            default:
                throw new IllegalArgumentException("Cannot convert '" + value + "' to boolean");
        }
    }
    
    private Object getDefaultValue(MQField field) {
        if (field.hasDefaultValue()) {
            try {
                // Convert default value using the same logic
                MQContract dummyContract = new MQContract("dummy", "1.0", "", MQMessageFormat.JSON, 
                                                         null, Map.of("trimFields", "true"));
                // Create a simple JSON node for the default value
                JsonNode defaultNode = objectMapper.valueToTree(field.getDefaultValue());
                return convertValue(defaultNode, field.getDefaultValue(), field, dummyContract);
            } catch (Exception e) {
                logger.warn("Cannot convert default value '{}' for field '{}', using null", 
                           field.getDefaultValue(), field.getName());
                return null;
            }
        }
        
        // Return type-appropriate default values
        switch (field.getType()) {
            case INTEGER:
                return 0;
            case LONG:
                return 0L;
            case DECIMAL:
                return BigDecimal.ZERO;
            case BOOLEAN:
                return false;
            case DATE:
                return null;
            case DATETIME:
                return null;
            case TIME:
                return null;
            case BINARY:
                return new byte[0];
            default:
                return null;
        }
    }
}