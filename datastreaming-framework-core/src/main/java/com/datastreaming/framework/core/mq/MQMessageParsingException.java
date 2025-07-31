package com.datastreaming.framework.core.mq;

/**
 * Exception thrown when MQ message parsing fails
 */
public class MQMessageParsingException extends Exception {
    
    private final String messageContent;
    private final String contractName;
    private final int fieldIndex;
    
    public MQMessageParsingException(String message) {
        super(message);
        this.messageContent = null;
        this.contractName = null;
        this.fieldIndex = -1;
    }
    
    public MQMessageParsingException(String message, Throwable cause) {
        super(message, cause);
        this.messageContent = null;
        this.contractName = null;
        this.fieldIndex = -1;
    }
    
    public MQMessageParsingException(String message, String messageContent, String contractName) {
        super(message);
        this.messageContent = messageContent;
        this.contractName = contractName;
        this.fieldIndex = -1;
    }
    
    public MQMessageParsingException(String message, String messageContent, String contractName, int fieldIndex) {
        super(message);
        this.messageContent = messageContent;
        this.contractName = contractName;
        this.fieldIndex = fieldIndex;
    }
    
    public MQMessageParsingException(String message, String messageContent, String contractName, int fieldIndex, Throwable cause) {
        super(message, cause);
        this.messageContent = messageContent;
        this.contractName = contractName;
        this.fieldIndex = fieldIndex;
    }
    
    public MQMessageParsingException(String message, String messageContent, String contractName, Throwable cause) {
        super(message, cause);
        this.messageContent = messageContent;
        this.contractName = contractName;
        this.fieldIndex = -1;
    }
    
    public String getMessageContent() { return messageContent; }
    public String getContractName() { return contractName; }
    public int getFieldIndex() { return fieldIndex; }
}