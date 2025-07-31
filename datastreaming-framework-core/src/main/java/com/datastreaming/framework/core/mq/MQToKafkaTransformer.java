package com.datastreaming.framework.core.mq;

import com.datastreaming.framework.core.api.MessageTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Transformer that parses MQ messages using contracts and transforms them for Kafka publishing
 */
public class MQToKafkaTransformer implements MessageTransformer {
    
    private static final Logger logger = LoggerFactory.getLogger(MQToKafkaTransformer.class);
    
    private final MQMessageParsingService parsingService;
    private final ObjectMapper objectMapper;
    
    @Autowired
    public MQToKafkaTransformer(MQMessageParsingService parsingService) {
        this.parsingService = parsingService;
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public String transform(Message sourceMessage, String sourceContract, String targetContract, String correlationId) 
            throws TransformationException {
        try {
            String messageContent = extractMessageContent(sourceMessage);
            Object transformedMessage = transform(messageContent, sourceContract);
            
            // For now, return JSON representation
            // In a full implementation, this would transform to the target contract format
            if (transformedMessage instanceof Map) {
                return objectMapper.writeValueAsString(transformedMessage);
            }
            
            return transformedMessage.toString();
            
        } catch (Exception e) {
            throw new TransformationException("Failed to transform message: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean supports(String sourceContract, String targetContract) {
        // For now, support any MQ contract to any target
        return sourceContract != null;
    }
    
    @Override
    public String getName() {
        return "MQToKafkaTransformer";
    }
    
    private String extractMessageContent(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        }
        
        // Handle other message types as needed
        return message.toString();
    }
    
    /**
     * Transform MQ message using string input (for testing and direct usage)
     */
    public Object transform(Object input) {
        return transform(input, null);
    }
    
    /**
     * Transform MQ message using specified contract
     */
    public Object transform(Object input, String contractName) {
        if (input == null) {
            logger.warn("Received null input for transformation");
            return null;
        }
        
        String messageContent = input.toString();
        
        try {
            if (contractName != null) {
                // Parse using specified contract
                Map<String, Object> parsedMessage = parsingService.parseMessage(messageContent, contractName);
                
                logger.debug("Successfully parsed MQ message using contract '{}' into {} fields", 
                           contractName, parsedMessage.size());
                
                return parsedMessage;
            } else {
                // Try to auto-detect format or use default transformation
                logger.debug("No contract specified, returning message as-is");
                return Map.of("rawMessage", messageContent, "timestamp", System.currentTimeMillis());
            }
            
        } catch (MQMessageParsingException e) {
            logger.error("Failed to parse MQ message with contract '{}': {}", contractName, e.getMessage());
            
            // Return error information for downstream handling
            return Map.of(
                "error", true,
                "errorMessage", e.getMessage(),
                "rawMessage", messageContent,
                "contractName", contractName != null ? contractName : "unknown",
                "timestamp", System.currentTimeMillis()
            );
        }
    }
    
    /**
     * Transform MQ message using specified contract and version
     */
    public Object transform(Object input, String contractName, String version) {
        if (input == null) {
            logger.warn("Received null input for transformation");
            return null;
        }
        
        String messageContent = input.toString();
        
        try {
            Map<String, Object> parsedMessage = parsingService.parseMessage(messageContent, contractName, version);
            
            logger.debug("Successfully parsed MQ message using contract '{}' (version: {}) into {} fields", 
                       contractName, version, parsedMessage.size());
            
            return parsedMessage;
            
        } catch (MQMessageParsingException e) {
            logger.error("Failed to parse MQ message with contract '{}' (version: {}): {}", 
                        contractName, version, e.getMessage());
            
            // Return error information for downstream handling
            return Map.of(
                "error", true,
                "errorMessage", e.getMessage(),
                "rawMessage", messageContent,
                "contractName", contractName != null ? contractName : "unknown",
                "contractVersion", version != null ? version : "unknown",
                "timestamp", System.currentTimeMillis()
            );
        }
    }
    
    /**
     * Transform and enrich MQ message with additional metadata
     */
    public Object transformWithMetadata(Object input, String contractName, String sourceQueue, String targetTopic) {
        Object transformedMessage = transform(input, contractName);
        
        if (transformedMessage instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> messageMap = (Map<String, Object>) transformedMessage;
            
            // Add metadata
            messageMap.put("_metadata", Map.of(
                "sourceQueue", sourceQueue != null ? sourceQueue : "unknown",
                "targetTopic", targetTopic != null ? targetTopic : "unknown",
                "contractName", contractName != null ? contractName : "unknown",
                "transformedAt", System.currentTimeMillis(),
                "transformerVersion", "1.0.0"
            ));
            
            return messageMap;
        }
        
        return transformedMessage;
    }
    
    /**
     * Validate message format without full transformation
     */
    public boolean validateMessage(String messageContent, String contractName) {
        return parsingService.validateMessageFormat(messageContent, contractName);
    }
    
    /**
     * Validate message format with version without full transformation
     */
    public boolean validateMessage(String messageContent, String contractName, String version) {
        return parsingService.validateMessageFormat(messageContent, contractName, version);
    }
}