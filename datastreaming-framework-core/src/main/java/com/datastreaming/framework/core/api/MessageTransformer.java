package com.datastreaming.framework.core.api;

import javax.jms.Message;

/**
 * SPI interface for message transformation
 * Implement this interface to provide custom message transformation logic
 */
public interface MessageTransformer {

    /**
     * Transform a message from MQ format to target format
     * @param sourceMessage The source JMS message
     * @param sourceContract The source message contract identifier
     * @param targetContract The target message contract identifier
     * @param correlationId The correlation ID for tracking
     * @return Transformed message as string
     * @throws TransformationException if transformation fails
     */
    String transform(Message sourceMessage, 
                    String sourceContract, 
                    String targetContract, 
                    String correlationId) throws TransformationException;

    /**
     * Validate if the transformer supports the given contract combination
     * @param sourceContract Source contract identifier
     * @param targetContract Target contract identifier
     * @return true if supported, false otherwise
     */
    boolean supports(String sourceContract, String targetContract);

    /**
     * Get transformer name/identifier
     * @return transformer name
     */
    String getName();

    /**
     * Exception thrown during message transformation
     */
    class TransformationException extends Exception {
        public TransformationException(String message) {
            super(message);
        }

        public TransformationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}