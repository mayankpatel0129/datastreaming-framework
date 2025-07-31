package com.datastreaming.framework.core.mq;

/**
 * Enumeration of supported MQ field data types
 */
public enum MQFieldType {
    
    /**
     * String/text field
     */
    STRING,
    
    /**
     * Integer number field
     */
    INTEGER,
    
    /**
     * Long number field
     */
    LONG,
    
    /**
     * Decimal number field
     */
    DECIMAL,
    
    /**
     * Boolean field (true/false, Y/N, 1/0)
     */
    BOOLEAN,
    
    /**
     * Date field (without time)
     */
    DATE,
    
    /**
     * Date and time field
     */
    DATETIME,
    
    /**
     * Time field (without date)
     */
    TIME,
    
    /**
     * Binary data field
     */
    BINARY,
    
    /**
     * Custom field type requiring custom conversion
     */
    CUSTOM
}