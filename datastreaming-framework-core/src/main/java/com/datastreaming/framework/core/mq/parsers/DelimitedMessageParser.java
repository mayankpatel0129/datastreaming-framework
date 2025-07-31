package com.datastreaming.framework.core.mq.parsers;

import com.datastreaming.framework.core.mq.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Parser for delimited MQ messages (CSV, TSV, pipe-separated, etc.)
 */
public class DelimitedMessageParser implements MQMessageParser {
    
    private static final Logger logger = LoggerFactory.getLogger(DelimitedMessageParser.class);
    
    @Override
    public Map<String, Object> parse(String messageContent, MQContract contract) throws MQMessageParsingException {
        if (messageContent == null) {
            throw new MQMessageParsingException("Message content cannot be null", messageContent, contract.getName());
        }
        
        if (!supports(contract.getFormat())) {
            throw new MQMessageParsingException(
                "Unsupported format: " + contract.getFormat() + ". Expected: " + getSupportedFormat(),
                messageContent, contract.getName()
            );
        }
        
        logger.debug("Parsing delimited message for contract: {}, content length: {}", 
                    contract.getName(), messageContent.length());
        
        String delimiter = contract.getDelimiter();
        String[] values = splitMessage(messageContent, delimiter, contract);
        
        logger.debug("Split message into {} values using delimiter '{}'", values.length, delimiter);
        
        Map<String, Object> result = new HashMap<>();
        
        for (int i = 0; i < contract.getFields().size(); i++) {
            MQField field = contract.getFields().get(i);
            
            try {
                Object value = extractField(values, field, i, contract);
                result.put(field.getName(), value);
                
                logger.trace("Extracted field '{}': '{}' (type: {})", 
                           field.getName(), value, field.getType());
                           
            } catch (Exception e) {
                String errorMsg = String.format(
                    "Failed to parse field '%s' at index %d in contract '%s'",
                    field.getName(), i, contract.getName()
                );
                
                logger.error(errorMsg, e);
                
                if (field.isRequired() && contract.isStrictMode()) {
                    throw new MQMessageParsingException(errorMsg, messageContent, contract.getName(), i, e);
                } else {
                    // Use default value or null for optional fields
                    Object defaultValue = getDefaultValue(field);
                    result.put(field.getName(), defaultValue);
                    
                    logger.warn("Using default value '{}' for field '{}'", defaultValue, field.getName());
                }
            }
        }
        
        logger.debug("Successfully parsed {} fields from delimited message", result.size());
        return result;
    }
    
    private String[] splitMessage(String messageContent, String delimiter, MQContract contract) 
            throws MQMessageParsingException {
        
        // Handle escaped delimiters and quoted fields
        boolean hasQuotes = contract.getProperty("hasQuotes", "false").equals("true");
        String quoteChar = contract.getProperty("quoteChar", "\"");
        
        if (hasQuotes) {
            return splitWithQuotes(messageContent, delimiter, quoteChar);
        } else {
            return messageContent.split(Pattern.quote(delimiter), -1); // -1 to preserve trailing empty strings
        }
    }
    
    private String[] splitWithQuotes(String messageContent, String delimiter, String quoteChar) {
        // Simple CSV-style parsing with quoted fields
        java.util.List<String> values = new java.util.ArrayList<>();
        StringBuilder currentValue = new StringBuilder();
        boolean insideQuotes = false;
        
        for (int i = 0; i < messageContent.length(); i++) {
            char c = messageContent.charAt(i);
            String charStr = String.valueOf(c);
            
            if (charStr.equals(quoteChar)) {
                // Check for escaped quotes (double quotes)
                if (i + 1 < messageContent.length() && 
                    String.valueOf(messageContent.charAt(i + 1)).equals(quoteChar)) {
                    currentValue.append(quoteChar);
                    i++; // Skip the next quote
                } else {
                    insideQuotes = !insideQuotes;
                }
            } else if (charStr.equals(delimiter) && !insideQuotes) {
                values.add(currentValue.toString());
                currentValue = new StringBuilder();
            } else {
                currentValue.append(c);
            }
        }
        
        values.add(currentValue.toString()); // Add the last value
        return values.toArray(new String[0]);
    }
    
    private Object extractField(String[] values, MQField field, int fieldIndex, MQContract contract) 
            throws MQMessageParsingException {
        
        // Check if we have enough values
        if (fieldIndex >= values.length) {
            if (field.isRequired()) {
                throw new MQMessageParsingException(
                    "Field '" + field.getName() + "' at index " + fieldIndex + " is missing from message",
                    null, contract.getName()
                );
            } else {
                return getDefaultValue(field);
            }
        }
        
        String rawValue = values[fieldIndex];
        
        // Trim if configured
        if (contract.isTrimFields()) {
            rawValue = rawValue.trim();
        }
        
        // Remove quotes if present
        boolean hasQuotes = contract.getProperty("hasQuotes", "false").equals("true");
        String quoteChar = contract.getProperty("quoteChar", "\"");
        
        if (hasQuotes && rawValue.startsWith(quoteChar) && rawValue.endsWith(quoteChar) && rawValue.length() >= 2) {
            rawValue = rawValue.substring(1, rawValue.length() - 1);
        }
        
        // Return null/default for empty optional fields
        if (rawValue.isEmpty()) {
            if (field.isRequired()) {
                throw new MQMessageParsingException(
                    "Required field '" + field.getName() + "' is empty",
                    null, contract.getName()
                );
            } else {
                return getDefaultValue(field);
            }
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
        return convertValue(rawValue, field, contract);
    }
    
    private Object convertValue(String rawValue, MQField field, MQContract contract) 
            throws MQMessageParsingException {
        
        try {
            switch (field.getType()) {
                case STRING:
                    return rawValue;
                    
                case INTEGER:
                    return Integer.parseInt(rawValue);
                    
                case LONG:
                    return Long.parseLong(rawValue);
                    
                case DECIMAL:
                    return new BigDecimal(rawValue);
                    
                case BOOLEAN:
                    return convertToBoolean(rawValue);
                    
                case DATE:
                    String dateFormat = field.hasDateFormat() ? field.getDateFormat() : "yyyy-MM-dd";
                    return LocalDate.parse(rawValue, DateTimeFormatter.ofPattern(dateFormat));
                    
                case DATETIME:
                    String datetimeFormat = field.hasDateFormat() ? field.getDateFormat() : "yyyy-MM-dd HH:mm:ss";
                    return LocalDateTime.parse(rawValue, DateTimeFormatter.ofPattern(datetimeFormat));
                    
                case TIME:
                    String timeFormat = field.hasDateFormat() ? field.getDateFormat() : "HH:mm:ss";
                    return LocalTime.parse(rawValue, DateTimeFormatter.ofPattern(timeFormat));
                    
                case BINARY:
                    try {
                        return rawValue.getBytes(contract.getEncoding());
                    } catch (java.io.UnsupportedEncodingException e) {
                        throw new MQMessageParsingException(
                            "Unsupported encoding: " + contract.getEncoding(),
                            null, contract.getName(), e
                        );
                    }
                    
                case CUSTOM:
                    // For custom types, return as string and let application handle conversion
                    return rawValue;
                    
                default:
                    throw new MQMessageParsingException(
                        "Unsupported field type: " + field.getType(),
                        null, contract.getName()
                    );
            }
            
        } catch (NumberFormatException e) {
            throw new MQMessageParsingException(
                "Cannot convert '" + rawValue + "' to " + field.getType() + " for field '" + field.getName() + "'",
                null, contract.getName(), e
            );
        } catch (DateTimeParseException e) {
            throw new MQMessageParsingException(
                "Cannot parse date/time '" + rawValue + "' for field '" + field.getName() + 
                "' using format '" + field.getDateFormat() + "'",
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
                MQContract dummyContract = new MQContract("dummy", "1.0", "", MQMessageFormat.DELIMITED, 
                                                         null, Map.of("trimFields", "true"));
                return convertValue(field.getDefaultValue(), field, dummyContract);
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
            default:
                return null;
        }
    }
    
    @Override
    public boolean supports(MQMessageFormat format) {
        return format == MQMessageFormat.DELIMITED;
    }
    
    @Override
    public MQMessageFormat getSupportedFormat() {
        return MQMessageFormat.DELIMITED;
    }
}