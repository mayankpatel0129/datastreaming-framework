package com.datastreaming.framework.core.mq;

/**
 * Enumeration of supported MQ message formats
 */
public enum MQMessageFormat {
    
    /**
     * Fixed-length format where each field has a specific position and length
     */
    FIXED_LENGTH,
    
    /**
     * Delimited format where fields are separated by a specific delimiter
     */
    DELIMITED,
    
    /**
     * JSON format for structured messages
     */
    JSON,
    
    /**
     * XML format for structured messages
     */
    XML,
    
    /**
     * Custom format requiring custom parser implementation
     */
    CUSTOM
}