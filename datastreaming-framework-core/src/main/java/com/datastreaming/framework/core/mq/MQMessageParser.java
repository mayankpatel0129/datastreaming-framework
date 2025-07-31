package com.datastreaming.framework.core.mq;

import java.util.Map;

/**
 * Interface for parsing MQ messages into structured data
 */
public interface MQMessageParser {
    
    /**
     * Parse raw MQ message content into a map of field values
     * 
     * @param messageContent Raw message content as string
     * @param contract MQ contract defining the message structure
     * @return Map of field names to values
     * @throws MQMessageParsingException if parsing fails
     */
    Map<String, Object> parse(String messageContent, MQContract contract) throws MQMessageParsingException;
    
    /**
     * Validate if the message format is supported by this parser
     * 
     * @param format The message format to check
     * @return true if supported, false otherwise
     */
    boolean supports(MQMessageFormat format);
    
    /**
     * Get the supported message format
     * 
     * @return The message format this parser handles
     */
    MQMessageFormat getSupportedFormat();
}