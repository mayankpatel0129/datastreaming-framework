package com.datastreaming.sample;

import com.datastreaming.framework.core.mq.*;
import com.datastreaming.framework.core.mq.parsers.JsonMessageParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Demonstration of JSON MQ message parsing capabilities
 */
@Component
public class JsonMQDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(JsonMQDemo.class);
    
    private final JsonMessageParser jsonParser;
    
    public JsonMQDemo() {
        this.jsonParser = new JsonMessageParser();
    }
    
    /**
     * Run all JSON parsing demonstrations
     */
    public void runAllDemos() {
        logger.info("=== JSON MQ Message Parsing Demos ===");
        
        try {
            demoSimpleJsonParsing();
            demoNestedJsonParsing();
            demoComplexTransactionParsing();
            demoErrorHandling();
            
            logger.info("=== All JSON parsing demos completed successfully ===");
            
        } catch (Exception e) {
            logger.error("Demo execution failed", e);
        }
    }
    
    /**
     * Demonstrate simple flat JSON parsing
     */
    public void demoSimpleJsonParsing() throws MQMessageParsingException {
        logger.info("\n--- Demo 1: Simple JSON Customer Parsing ---");
        
        // Create customer contract
        List<MQField> customerFields = List.of(
            MQField.builder("customerId", MQFieldType.STRING)
                .required(true).jsonPath("customer_id").build(),
            MQField.builder("firstName", MQFieldType.STRING)
                .required(true).jsonPath("first_name").build(),
            MQField.builder("lastName", MQFieldType.STRING)
                .required(true).jsonPath("last_name").build(),
            MQField.builder("email", MQFieldType.STRING)
                .required(true).build(),
            MQField.builder("age", MQFieldType.INTEGER)
                .required(false).defaultValue("0").build(),
            MQField.builder("isActive", MQFieldType.BOOLEAN)
                .required(false).jsonPath("active_flag").defaultValue("true").build(),
            MQField.builder("creditLimit", MQFieldType.DECIMAL)
                .required(false).jsonPath("credit_limit").defaultValue("0.00").build()
        );
        
        MQContract customerContract = new MQContract(
            "customer-json", "1.0", "JSON customer contract", 
            MQMessageFormat.JSON, customerFields, 
            Map.of("trimFields", "true")
        );
        
        // Sample JSON message
        String jsonMessage = """
            {
                "customer_id": "CUST123456",
                "first_name": "John",
                "last_name": "Doe", 
                "email": "john.doe@example.com",
                "age": 35,
                "active_flag": true,
                "credit_limit": 5000.50
            }
            """;
        
        logger.info("Input JSON Message:");
        logger.info(jsonMessage);
        
        // Parse message
        Map<String, Object> result = jsonParser.parse(jsonMessage, customerContract);
        
        logger.info("Parsed Results:");
        result.forEach((key, value) -> 
            logger.info("  {} = {} ({})", key, value, value != null ? value.getClass().getSimpleName() : "null"));
        
        logger.info("Simple JSON parsing completed successfully!");
    }
    
    /**
     * Demonstrate nested JSON parsing with complex structures
     */
    public void demoNestedJsonParsing() throws MQMessageParsingException {
        logger.info("\n--- Demo 2: Nested JSON Order Parsing ---");
        
        // Create order contract with nested paths
        List<MQField> orderFields = List.of(
            MQField.builder("orderId", MQFieldType.STRING)
                .required(true).jsonPath("order.id").build(),
            MQField.builder("customerId", MQFieldType.STRING)
                .required(true).jsonPath("order.customer.id").build(),
            MQField.builder("customerName", MQFieldType.STRING)
                .required(true).jsonPath("order.customer.name").build(),
            MQField.builder("shippingStreet", MQFieldType.STRING)
                .required(false).jsonPath("order.shipping_address.street").build(),
            MQField.builder("shippingCity", MQFieldType.STRING)
                .required(false).jsonPath("order.shipping_address.city").build(),
            MQField.builder("totalAmount", MQFieldType.DECIMAL)
                .required(true).jsonPath("order.total_amount").build(),
            MQField.builder("currency", MQFieldType.STRING)
                .required(false).jsonPath("order.currency").defaultValue("USD").build(),
            MQField.builder("firstItemName", MQFieldType.STRING)
                .required(false).jsonPath("order.items[0].name").build(),
            MQField.builder("firstItemQuantity", MQFieldType.INTEGER)
                .required(false).jsonPath("order.items[0].quantity").build(),
            MQField.builder("isPriority", MQFieldType.BOOLEAN)
                .required(false).jsonPath("order.metadata.priority_flag").build()
        );
        
        MQContract orderContract = new MQContract(
            "order-json-nested", "2.0", "Nested JSON order contract", 
            MQMessageFormat.JSON, orderFields, 
            Map.of("trimFields", "true")
        );
        
        // Complex nested JSON message
        String nestedJsonMessage = """
            {
                "order": {
                    "id": "ORD20240131001",
                    "customer": {
                        "id": "CUST789012",
                        "name": "Jane Smith",
                        "email": "jane.smith@example.com"
                    },
                    "shipping_address": {
                        "street": "456 Oak Street",
                        "city": "Denver",
                        "state": "CO",
                        "postal_code": "80202"
                    },
                    "items": [
                        {
                            "name": "Premium Widget",
                            "sku": "WIDGET-001",
                            "quantity": 3,
                            "unit_price": 29.99
                        },
                        {
                            "name": "Standard Gadget",
                            "sku": "GADGET-002", 
                            "quantity": 1,
                            "unit_price": 15.50
                        }
                    ],
                    "total_amount": 105.47,
                    "currency": "USD",
                    "metadata": {
                        "priority_flag": true,
                        "source": "WEB",
                        "campaign_code": "WINTER2024"
                    },
                    "status": "CONFIRMED"
                }
            }
            """;
        
        logger.info("Input Nested JSON Message:");
        logger.info(nestedJsonMessage);
        
        // Parse nested message
        Map<String, Object> result = jsonParser.parse(nestedJsonMessage, orderContract);
        
        logger.info("Parsed Results:");
        result.forEach((key, value) -> 
            logger.info("  {} = {} ({})", key, value, value != null ? value.getClass().getSimpleName() : "null"));
        
        logger.info("Nested JSON parsing completed successfully!");
    }
    
    /**
     * Demonstrate complex financial transaction parsing
     */
    public void demoComplexTransactionParsing() throws MQMessageParsingException {
        logger.info("\n--- Demo 3: Complex Financial Transaction Parsing ---");
        
        // Create transaction contract
        List<MQField> transactionFields = List.of(
            MQField.builder("transactionId", MQFieldType.STRING)
                .required(true).jsonPath("transaction.id").build(),
            MQField.builder("accountNumber", MQFieldType.STRING)
                .required(true).jsonPath("transaction.account.number").build(),
            MQField.builder("amount", MQFieldType.DECIMAL)
                .required(true).jsonPath("transaction.amount").build(),
            MQField.builder("transactionType", MQFieldType.STRING)
                .required(true).jsonPath("transaction.type").build(),
            MQField.builder("merchantName", MQFieldType.STRING)
                .required(false).jsonPath("transaction.merchant.name").build(),
            MQField.builder("merchantCategory", MQFieldType.STRING)
                .required(false).jsonPath("transaction.merchant.category_code").build(),
            MQField.builder("isApproved", MQFieldType.BOOLEAN)
                .required(true).jsonPath("transaction.authorization.approved").build(),
            MQField.builder("authCode", MQFieldType.STRING)
                .required(false).jsonPath("transaction.authorization.code").build(),
            MQField.builder("riskScore", MQFieldType.INTEGER)
                .required(false).jsonPath("transaction.risk_assessment.score").defaultValue("0").build(),
            MQField.builder("balanceAfter", MQFieldType.DECIMAL)
                .required(false).jsonPath("transaction.account.balance_after").build(),
            MQField.builder("channelType", MQFieldType.STRING)
                .required(true).jsonPath("transaction.channel.type").build()
        );
        
        MQContract transactionContract = new MQContract(
            "transaction-json", "1.0", "Financial transaction contract", 
            MQMessageFormat.JSON, transactionFields, 
            Map.of("trimFields", "true", "strictMode", "false")
        );
        
        // Complex transaction JSON
        String transactionMessage = """
            {
                "transaction": {
                    "id": "TXN202401310001",
                    "account": {
                        "number": "1234567890123456",
                        "type": "CHECKING",
                        "balance_before": 1500.75,
                        "balance_after": 1425.50
                    },
                    "amount": 75.25,
                    "currency": "USD",
                    "type": "DEBIT",
                    "description": "Coffee Shop Purchase",
                    "merchant": {
                        "name": "Starbucks #1234",
                        "category_code": "5814",
                        "location": {
                            "city": "Seattle",
                            "state": "WA"
                        }
                    },
                    "authorization": {
                        "approved": true,
                        "code": "AUTH789012",
                        "response_code": "00"
                    },
                    "risk_assessment": {
                        "score": 15,
                        "level": "LOW",
                        "factors": ["location_match", "spending_pattern"]
                    },
                    "channel": {
                        "type": "CONTACTLESS",
                        "device_id": "CARD_4567",
                        "terminal_id": "T123456"
                    },
                    "timestamp": "2024-01-31T10:30:45.123Z"
                }
            }
            """;
        
        logger.info("Input Transaction JSON Message:");
        logger.info(transactionMessage);
        
        // Parse transaction
        Map<String, Object> result = jsonParser.parse(transactionMessage, transactionContract);
        
        logger.info("Parsed Transaction Results:");
        result.forEach((key, value) -> 
            logger.info("  {} = {} ({})", key, value, value != null ? value.getClass().getSimpleName() : "null"));
        
        logger.info("Complex transaction parsing completed successfully!");
    }
    
    /**
     * Demonstrate error handling scenarios
     */
    public void demoErrorHandling() {
        logger.info("\n--- Demo 4: Error Handling Scenarios ---");
        
        // Create a strict contract
        List<MQField> strictFields = List.of(
            MQField.builder("id", MQFieldType.STRING)
                .required(true).pattern("^ID\\d{6}$").build(),
            MQField.builder("email", MQFieldType.STRING)
                .required(true).pattern("^[\\w._%+-]+@[\\w.-]+\\.[A-Za-z]{2,}$").build(),
            MQField.builder("amount", MQFieldType.DECIMAL)
                .required(true).build()
        );
        
        MQContract strictContract = new MQContract(
            "strict-test", "1.0", "Strict validation contract", 
            MQMessageFormat.JSON, strictFields, 
            Map.of("strictMode", "true")
        );
        
        // Test cases for error handling
        testErrorScenario("Invalid JSON Format", 
            "{ \"id\": \"ID123456\", \"email\": \"test@example.com\" // missing brace", 
            strictContract);
        
        testErrorScenario("Missing Required Field", 
            "{ \"email\": \"test@example.com\", \"amount\": 100.50 }", 
            strictContract);
        
        testErrorScenario("Pattern Validation Failure", 
            "{ \"id\": \"INVALID123\", \"email\": \"invalid-email\", \"amount\": 100.50 }", 
            strictContract);
        
        testErrorScenario("Type Conversion Error", 
            "{ \"id\": \"ID123456\", \"email\": \"test@example.com\", \"amount\": \"not-a-number\" }", 
            strictContract);
        
        logger.info("Error handling demos completed!");
    }
    
    private void testErrorScenario(String scenarioName, String jsonMessage, MQContract contract) {
        logger.info("\nTesting: {}", scenarioName);
        logger.info("Input: {}", jsonMessage);
        
        try {
            Map<String, Object> result = jsonParser.parse(jsonMessage, contract);
            logger.warn("Expected error but parsing succeeded: {}", result);
        } catch (MQMessageParsingException e) {
            logger.info("✓ Expected error caught: {}", e.getMessage());
        }
    }
    
    /**
     * Display JSON parsing capabilities and features
     */
    public void displayCapabilities() {
        logger.info("\n=== JSON MQ Message Parsing Capabilities ===");
        logger.info("✓ Flat JSON structure parsing");
        logger.info("✓ Nested JSON object navigation (e.g., order.customer.name)");
        logger.info("✓ Array element access (e.g., items[0].quantity)");
        logger.info("✓ Multiple data type support (STRING, INTEGER, DECIMAL, BOOLEAN, DATE, etc.)");
        logger.info("✓ Custom JSON path mapping with jsonPath field property");
        logger.info("✓ Pattern validation with regex support");
        logger.info("✓ Default value handling for optional fields");
        logger.info("✓ Strict mode validation for required fields");
        logger.info("✓ Date/time parsing with custom formats");
        logger.info("✓ Boolean conversion from multiple representations");
        logger.info("✓ Comprehensive error handling and validation");
        logger.info("✓ Field trimming and encoding support");
        logger.info("===============================================");
    }
}