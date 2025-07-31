package com.datastreaming.sample;

import com.datastreaming.framework.core.mq.MQContractRegistry;
import com.datastreaming.framework.core.mq.MQMessageParsingException;
import com.datastreaming.framework.core.mq.MQMessageParsingService;
import com.datastreaming.framework.core.mq.MQToKafkaTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Demo class showing how to use MQ message parsing capabilities
 */
@Component
@ConditionalOnProperty(prefix = "datastreaming", name = "enabled", havingValue = "true")
public class MQParsingDemo implements CommandLineRunner {
    
    private static final Logger logger = LoggerFactory.getLogger(MQParsingDemo.class);
    
    @Autowired
    private MQMessageParsingService parsingService;
    
    @Autowired
    private MQContractRegistry contractRegistry;
    
    @Autowired
    private MQToKafkaTransformer transformer;
    
    @Override
    public void run(String... args) throws Exception {
        runAllDemos();
    }
    
    /**
     * Run all MQ parsing demonstrations
     */
    public void runAllDemos() {
        logger.info("=== MQ Message Parsing Demo ===");
        
        try {
            // Demo 1: Parse fixed-length transaction message
            demoFixedLengthParsing();
            
            // Demo 2: Parse CSV customer message
            demoCSVParsing();
            
            // Demo 3: Parse pipe-delimited order message
            demoPipeDelimitedParsing();
            
            // Demo 4: Show transformation capabilities
            demoTransformation();
            
            // Demo 5: Show validation capabilities
            demoValidation();
            
            // Demo 6: Show contract registry info
            demoContractRegistryInfo();
            
            logger.info("=== MQ parsing demo complete ===");
        } catch (Exception e) {
            logger.error("Error during MQ parsing demo", e);
        }
    }
    
    private void demoFixedLengthParsing() {
        logger.info("--- Fixed-Length Message Parsing ---");
        
        // Sample fixed-length transaction message
        String message = "TX12345678ACC123456789     1234.56USD2024073114305P1";
        
        try {
            Map<String, Object> parsed = parsingService.parseMessage(message, "transaction-fixed");
            
            logger.info("Successfully parsed fixed-length message:");
            parsed.forEach((key, value) -> 
                logger.info("  {}: {} ({})", key, value, value != null ? value.getClass().getSimpleName() : "null")
            );
            
        } catch (MQMessageParsingException e) {
            logger.error("Failed to parse fixed-length message: {}", e.getMessage());
        }
    }
    
    private void demoCSVParsing() {
        logger.info("--- CSV Message Parsing ---");
        
        // Sample CSV customer message
        String message = "\"CUST123456\",\"John\",\"Doe\",\"john.doe@example.com\",\"1990-05-15\",\"2024-01-15 10:30:00\",\"true\",\"5000.00\"";
        
        try {
            Map<String, Object> parsed = parsingService.parseMessage(message, "customer-csv");
            
            logger.info("Successfully parsed CSV message:");
            parsed.forEach((key, value) -> 
                logger.info("  {}: {} ({})", key, value, value != null ? value.getClass().getSimpleName() : "null")
            );
            
        } catch (MQMessageParsingException e) {
            logger.error("Failed to parse CSV message: {}", e.getMessage());
        }
    }
    
    private void demoPipeDelimitedParsing() {
        logger.info("--- Pipe-Delimited Message Parsing ---");
        
        // Sample pipe-delimited order message
        String message = "ORD12345678|CUST123456|PROD001|5|19.99|99.95|2024-07-31 14:30:05|HIGH";
        
        try {
            Map<String, Object> parsed = parsingService.parseMessage(message, "order-pipe");
            
            logger.info("Successfully parsed pipe-delimited message:");
            parsed.forEach((key, value) -> 
                logger.info("  {}: {} ({})", key, value, value != null ? value.getClass().getSimpleName() : "null")
            );
            
        } catch (MQMessageParsingException e) {
            logger.error("Failed to parse pipe-delimited message: {}", e.getMessage());
        }
    }
    
    private void demoTransformation() {
        logger.info("--- Message Transformation ---");
        
        String message = "TX12345678ACC123456789     1234.56USD2024073114305P1";
        
        // Transform using the MQToKafkaTransformer
        Object transformed = transformer.transform(message, "transaction-fixed");
        
        if (transformed instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> transformedMap = (Map<String, Object>) transformed;
            
            logger.info("Successfully transformed message:");
            transformedMap.forEach((key, value) -> 
                logger.info("  {}: {} ({})", key, value, value != null ? value.getClass().getSimpleName() : "null")
            );
        } else {
            logger.info("Transformed message: {} ({})", transformed, transformed != null ? transformed.getClass().getSimpleName() : "null");
        }
        
        // Transform with metadata
        Object transformedWithMetadata = transformer.transformWithMetadata(
            message, "transaction-fixed", "FINANCE.TRANSACTION.QUEUE", "financial-transactions"
        );
        
        if (transformedWithMetadata instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metadataMap = (Map<String, Object>) transformedWithMetadata;
            
            logger.info("Message with metadata:");
            if (metadataMap.containsKey("_metadata")) {
                logger.info("  Metadata: {}", metadataMap.get("_metadata"));
            }
        }
    }
    
    private void demoValidation() {
        logger.info("--- Message Validation ---");
        
        // Valid messages
        String validFixed = "TX12345678ACC123456789     1234.56USD2024073114305P1";
        String validCSV = "\"CUST123456\",\"John\",\"Doe\",\"john.doe@example.com\",,\"2024-01-15 10:30:00\",,";
        String validPipe = "ORD12345678|CUST123456|PROD001|5|19.99|99.95|2024-07-31 14:30:05|HIGH";
        
        // Invalid messages
        String invalidFixed = "SHORT"; // Too short
        String invalidCSV = "CUST123456"; // Missing required fields
        String invalidPipe = "ORD12345678|CUST123456"; // Missing required fields
        
        logger.info("Validation results:");
        logger.info("  Valid fixed-length: {}", parsingService.validateMessageFormat(validFixed, "transaction-fixed"));
        logger.info("  Valid CSV: {}", parsingService.validateMessageFormat(validCSV, "customer-csv"));
        logger.info("  Valid pipe-delimited: {}", parsingService.validateMessageFormat(validPipe, "order-pipe"));
        logger.info("  Invalid fixed-length: {}", parsingService.validateMessageFormat(invalidFixed, "transaction-fixed"));
        logger.info("  Invalid CSV: {}", parsingService.validateMessageFormat(invalidCSV, "customer-csv"));
        logger.info("  Invalid pipe-delimited: {}", parsingService.validateMessageFormat(invalidPipe, "order-pipe"));
    }
    
    private void demoContractRegistryInfo() {
        logger.info("--- Contract Registry Information ---");
        
        // Show supported formats
        logger.info("Supported formats: {}", parsingService.getSupportedFormats());
        
        // Show parser information
        logger.info("Parser information: {}", parsingService.getParserInfo());
        
        // Show contract registry stats
        var stats = contractRegistry.getStats();
        logger.info("Contract registry stats: {}", stats);
        
        // Show all loaded contracts
        logger.info("Loaded contracts:");
        contractRegistry.getAllContracts().forEach((key, contract) -> 
            logger.info("  {}: {} (format: {}, fields: {})", 
                       key, contract.getDescription(), contract.getFormat(), contract.getFields().size())
        );
    }
}