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
 * Simple demo class showing MQ message parsing capabilities without external dependencies
 */
@Component
@ConditionalOnProperty(prefix = "datastreaming.mq", name = "enabled", havingValue = "true")
public class SimpleMQDemo implements CommandLineRunner {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleMQDemo.class);
    
    @Autowired(required = false)
    private MQMessageParsingService parsingService;
    
    @Autowired(required = false)
    private MQContractRegistry contractRegistry;
    
    @Autowired(required = false)
    private MQToKafkaTransformer transformer;
    
    @Override
    public void run(String... args) throws Exception {
        if (parsingService == null || contractRegistry == null) {
            logger.info("MQ parsing services not available - skipping demo");
            return;
        }
        
        logger.info("=== Simple MQ Message Parsing Demo ===");
        
        try {
            // Demo 1: Parse fixed-length transaction message
            demoFixedLengthParsing();
            
            // Demo 2: Parse CSV customer message
            demoCSVParsing();
            
            // Demo 3: Parse pipe-delimited order message
            demoPipeDelimitedParsing();
            
            // Demo 4: Show contract registry info
            demoContractRegistryInfo();
            
            logger.info("=== MQ Demo Complete - All parsing operations successful! ===");
            
        } catch (Exception e) {
            logger.error("Demo failed: ", e);
        }
    }
    
    private void demoFixedLengthParsing() {
        logger.info("--- Fixed-Length Message Parsing ---");
        
        // Sample fixed-length transaction message - 56 characters
        String message = "TX12345678ACC123456789000000001234.56USD20240731143005P1";
        
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