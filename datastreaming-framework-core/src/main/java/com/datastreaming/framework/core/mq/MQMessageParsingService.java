package com.datastreaming.framework.core.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Service for parsing MQ messages using appropriate parsers based on format and contract
 */
public class MQMessageParsingService {
    
    private static final Logger logger = LoggerFactory.getLogger(MQMessageParsingService.class);
    
    private final List<MQMessageParser> parsers;
    private final MQContractRegistry contractRegistry;
    
    @Autowired
    public MQMessageParsingService(List<MQMessageParser> parsers, MQContractRegistry contractRegistry) {
        this.parsers = parsers;
        this.contractRegistry = contractRegistry;
        
        logger.info("Initialized MQMessageParsingService with {} parsers", parsers.size());
        parsers.forEach(parser -> 
            logger.debug("Registered parser: {} for format: {}", 
                        parser.getClass().getSimpleName(), parser.getSupportedFormat())
        );
    }
    
    /**
     * Parse MQ message using specified contract
     */
    public Map<String, Object> parseMessage(String messageContent, String contractName) throws MQMessageParsingException {
        return parseMessage(messageContent, contractName, null);
    }
    
    /**
     * Parse MQ message using specified contract and version
     */
    public Map<String, Object> parseMessage(String messageContent, String contractName, String version) 
            throws MQMessageParsingException {
        
        logger.debug("Parsing message with contract: {}, version: {}", contractName, version);
        
        // Get contract from registry
        MQContract contract = contractRegistry.getContract(contractName, version);
        if (contract == null) {
            throw new MQMessageParsingException(
                "Contract not found: " + contractName + (version != null ? "-" + version : "")
            );
        }
        
        return parseMessage(messageContent, contract);
    }
    
    /**
     * Parse MQ message using provided contract
     */
    public Map<String, Object> parseMessage(String messageContent, MQContract contract) throws MQMessageParsingException {
        if (messageContent == null) {
            throw new MQMessageParsingException("Message content cannot be null");
        }
        
        if (contract == null) {
            throw new MQMessageParsingException("Contract cannot be null");
        }
        
        logger.debug("Parsing message using contract: {} (format: {})", 
                    contract.getName(), contract.getFormat());
        
        // Find appropriate parser
        MQMessageParser parser = findParser(contract.getFormat());
        if (parser == null) {
            throw new MQMessageParsingException(
                "No parser found for format: " + contract.getFormat()
            );
        }
        
        logger.debug("Using parser: {} for format: {}", 
                    parser.getClass().getSimpleName(), contract.getFormat());
        
        try {
            Map<String, Object> result = parser.parse(messageContent, contract);
            
            logger.debug("Successfully parsed message with {} fields", result.size());
            logger.trace("Parsed result: {}", result);
            
            return result;
            
        } catch (MQMessageParsingException e) {
            logger.error("Failed to parse message with contract '{}': {}", 
                        contract.getName(), e.getMessage());
            throw e;
        } catch (Exception e) {
            String errorMsg = "Unexpected error parsing message with contract '" + contract.getName() + "': " + e.getMessage();
            logger.error(errorMsg, e);
            throw new MQMessageParsingException(errorMsg, e);
        }
    }
    
    /**
     * Validate message format against contract without full parsing
     */
    public boolean validateMessageFormat(String messageContent, String contractName) {
        return validateMessageFormat(messageContent, contractName, null);
    }
    
    /**
     * Validate message format against contract without full parsing
     */
    public boolean validateMessageFormat(String messageContent, String contractName, String version) {
        try {
            MQContract contract = contractRegistry.getContract(contractName, version);
            return validateMessageFormat(messageContent, contract);
        } catch (Exception e) {
            logger.warn("Failed to validate message format: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Validate message format against contract without full parsing
     */
    public boolean validateMessageFormat(String messageContent, MQContract contract) {
        if (messageContent == null || contract == null) {
            return false;
        }
        
        try {
            // Perform basic format validation without full parsing
            switch (contract.getFormat()) {
                case FIXED_LENGTH:
                    return validateFixedLengthFormat(messageContent, contract);
                case DELIMITED:
                    return validateDelimitedFormat(messageContent, contract);
                default:
                    // For other formats, try parsing and catch exceptions
                    parseMessage(messageContent, contract);
                    return true;
            }
        } catch (Exception e) {
            logger.debug("Message format validation failed: {}", e.getMessage());
            return false;
        }
    }
    
    private boolean validateFixedLengthFormat(String messageContent, MQContract contract) {
        // Check if message length is sufficient for all required fields
        int expectedMinLength = contract.getFields().stream()
            .filter(MQField::isRequired)
            .mapToInt(MQField::getEndPosition)
            .max()
            .orElse(0);
            
        return messageContent.length() >= expectedMinLength;
    }
    
    private boolean validateDelimitedFormat(String messageContent, MQContract contract) {
        String delimiter = contract.getDelimiter();
        String[] parts = messageContent.split(Pattern.quote(delimiter), -1);
        
        long requiredFieldCount = contract.getFields().stream()
            .filter(MQField::isRequired)
            .count();
            
        return parts.length >= requiredFieldCount;
    }
    
    private MQMessageParser findParser(MQMessageFormat format) {
        Optional<MQMessageParser> parser = parsers.stream()
            .filter(p -> p.supports(format))
            .findFirst();
            
        return parser.orElse(null);
    }
    
    /**
     * Get list of supported message formats
     */
    public List<MQMessageFormat> getSupportedFormats() {
        return parsers.stream()
            .map(MQMessageParser::getSupportedFormat)
            .distinct()
            .collect(java.util.stream.Collectors.toList());
    }
    
    /**
     * Get parser information for debugging
     */
    public Map<MQMessageFormat, String> getParserInfo() {
        return parsers.stream()
            .collect(java.util.stream.Collectors.toMap(
                MQMessageParser::getSupportedFormat,
                p -> p.getClass().getSimpleName()
            ));
    }
}