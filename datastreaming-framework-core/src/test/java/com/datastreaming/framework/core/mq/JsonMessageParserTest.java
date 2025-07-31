package com.datastreaming.framework.core.mq;

import com.datastreaming.framework.core.mq.parsers.JsonMessageParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonMessageParserTest {

    private JsonMessageParser parser;
    private MQContract customerContract;
    private MQContract orderContract;

    @BeforeEach
    void setUp() {
        parser = new JsonMessageParser();

        // Customer contract for flat JSON
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
                .required(false).defaultValue("true").build(),
            MQField.builder("creditLimit", MQFieldType.DECIMAL)
                .required(false).defaultValue("0.00").build(),
            MQField.builder("registrationDate", MQFieldType.DATETIME)
                .required(true).dateFormat("yyyy-MM-dd'T'HH:mm:ss").build()
        );

        customerContract = new MQContract(
            "customer-json", "1.0", "JSON customer contract", 
            MQMessageFormat.JSON, customerFields, 
            Map.of(
                "trimFields", "true",
                "strictMode", "false"
            )
        );

        // Order contract for nested JSON
        List<MQField> orderFields = List.of(
            MQField.builder("orderId", MQFieldType.STRING)
                .required(true).jsonPath("order.id").build(),
            MQField.builder("customerId", MQFieldType.STRING)
                .required(true).jsonPath("order.customer.id").build(),
            MQField.builder("customerName", MQFieldType.STRING)
                .required(true).jsonPath("order.customer.name").build(),
            MQField.builder("street", MQFieldType.STRING)
                .required(false).jsonPath("order.shipping_address.street").build(),
            MQField.builder("city", MQFieldType.STRING)
                .required(false).jsonPath("order.shipping_address.city").build(),
            MQField.builder("totalAmount", MQFieldType.DECIMAL)
                .required(true).jsonPath("order.total").build(),
            MQField.builder("itemCount", MQFieldType.INTEGER)
                .required(false).jsonPath("order.items[0].quantity").build(),
            MQField.builder("orderDate", MQFieldType.DATE)
                .required(true).jsonPath("order.created_date")
                .dateFormat("yyyy-MM-dd").build(),
            MQField.builder("isPriority", MQFieldType.BOOLEAN)
                .required(false).jsonPath("order.priority_flag").build()
        );

        orderContract = new MQContract(
            "order-json", "2.0", "JSON order contract with nested fields", 
            MQMessageFormat.JSON, orderFields, 
            Map.of("trimFields", "true", "strictMode", "false")
        );
    }

    @Test
    void testSupportsFormat() {
        assertTrue(parser.supports(MQMessageFormat.JSON));
        assertFalse(parser.supports(MQMessageFormat.FIXED_LENGTH));
        assertFalse(parser.supports(MQMessageFormat.DELIMITED));
    }

    @Test
    void testGetSupportedFormat() {
        assertEquals(MQMessageFormat.JSON, parser.getSupportedFormat());
    }

    @Test
    void testParseSimpleJsonMessage() throws MQMessageParsingException {
        String jsonMessage = """
            {
                "customer_id": "CUST123456",
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@example.com",
                "age": 35,
                "isActive": true,
                "creditLimit": 5000.50,
                "registrationDate": "2024-01-15T10:30:00"
            }
            """;

        Map<String, Object> result = parser.parse(jsonMessage, customerContract);

        assertEquals("CUST123456", result.get("customerId"));
        assertEquals("John", result.get("firstName"));
        assertEquals("Doe", result.get("lastName"));
        assertEquals("john.doe@example.com", result.get("email"));
        assertEquals(35, result.get("age"));
        assertEquals(true, result.get("isActive"));
        assertEquals(new BigDecimal("5000.50"), result.get("creditLimit"));
        assertEquals(LocalDateTime.of(2024, 1, 15, 10, 30, 0), result.get("registrationDate"));
    }

    @Test
    void testParseNestedJsonMessage() throws MQMessageParsingException {
        String jsonMessage = """
            {
                "order": {
                    "id": "ORD20240115001",
                    "customer": {
                        "id": "CUST123456",
                        "name": "John Doe"
                    },
                    "shipping_address": {
                        "street": "123 Main St",
                        "city": "Springfield",
                        "country": "USA"
                    },
                    "items": [
                        {
                            "product_id": "PROD001",
                            "quantity": 2,
                            "price": 25.99
                        }
                    ],
                    "total": 51.98,
                    "created_date": "2024-01-15",
                    "priority_flag": true,
                    "status": "PENDING"
                }
            }
            """;

        Map<String, Object> result = parser.parse(jsonMessage, orderContract);

        assertEquals("ORD20240115001", result.get("orderId"));
        assertEquals("CUST123456", result.get("customerId"));
        assertEquals("John Doe", result.get("customerName"));
        assertEquals("123 Main St", result.get("street"));
        assertEquals("Springfield", result.get("city"));
        assertEquals(new BigDecimal("51.98"), result.get("totalAmount"));
        assertEquals(2, result.get("itemCount")); // From items[0].quantity
        assertEquals(LocalDate.of(2024, 1, 15), result.get("orderDate"));
        assertEquals(true, result.get("isPriority"));
    }

    @Test
    void testParseWithMissingOptionalFields() throws MQMessageParsingException {
        String jsonMessage = """
            {
                "customer_id": "CUST789012",
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane.smith@example.com",
                "registrationDate": "2024-02-20T14:45:30"
            }
            """;

        Map<String, Object> result = parser.parse(jsonMessage, customerContract);

        assertEquals("CUST789012", result.get("customerId"));
        assertEquals("Jane", result.get("firstName"));
        assertEquals("Smith", result.get("lastName"));
        assertEquals("jane.smith@example.com", result.get("email"));
        assertEquals(0, result.get("age")); // Default value
        assertEquals(true, result.get("isActive")); // Default value
        assertEquals(BigDecimal.ZERO, result.get("creditLimit")); // Default value
        assertEquals(LocalDateTime.of(2024, 2, 20, 14, 45, 30), result.get("registrationDate"));
    }

    @Test
    void testParseWithEmptyStringFields() throws MQMessageParsingException {
        String jsonMessage = """
            {
                "customer_id": "CUST345678",
                "first_name": "",
                "last_name": "Johnson",
                "email": "johnson@example.com",
                "registrationDate": "2024-03-10T09:15:45"
            }
            """;

        // With strictMode false, empty strings should use default values
        Map<String, Object> result = parser.parse(jsonMessage, customerContract);

        assertEquals("CUST345678", result.get("customerId"));
        assertNull(result.get("firstName")); // Empty string becomes null (no default)
        assertEquals("Johnson", result.get("lastName"));
        assertEquals("johnson@example.com", result.get("email"));
    }

    @Test
    void testParseStrictModeWithMissingRequiredField() {
        String jsonMessage = """
            {
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@example.com",
                "registrationDate": "2024-01-15T10:30:00"
            }
            """;

        MQContract strictContract = new MQContract(
            "customer-json", "1.0", "JSON customer contract", 
            MQMessageFormat.JSON, customerContract.getFields(), 
            Map.of("strictMode", "true")
        );

        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(jsonMessage, strictContract);
        });
    }

    @Test
    void testParseBooleanConversions() throws MQMessageParsingException {
        String jsonMessage = """
            {
                "customer_id": "CUST999999",
                "first_name": "Test",
                "last_name": "User",
                "email": "test@example.com",
                "isActive": "yes",
                "registrationDate": "2024-01-01T00:00:00"
            }
            """;

        Map<String, Object> result = parser.parse(jsonMessage, customerContract);

        assertEquals(true, result.get("isActive")); // "yes" converts to true
    }

    @Test
    void testParseWithTypeConversionErrors() {
        String jsonMessage = """
            {
                "customer_id": "CUST888888",
                "first_name": "Error",
                "last_name": "Test",
                "email": "error@example.com",
                "age": "not-a-number",
                "registrationDate": "2024-01-01T00:00:00"
            }
            """;

        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(jsonMessage, customerContract);
        });
    }

    @Test
    void testParseInvalidJson() {
        String invalidJson = """
            {
                "customer_id": "CUST777777",
                "first_name": "Invalid"
                // Missing closing brace and comma
            """;

        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(invalidJson, customerContract);
        });
    }

    @Test
    void testParseWithPatternValidation() throws MQMessageParsingException {
        // Create a contract with pattern validation
        List<MQField> patternFields = List.of(
            MQField.builder("customerId", MQFieldType.STRING)
                .required(true).pattern("^CUST\\d{6}$").build(),
            MQField.builder("email", MQFieldType.STRING)
                .required(true).pattern("^[\\w._%+-]+@[\\w.-]+\\.[A-Za-z]{2,}$").build()
        );

        MQContract patternContract = new MQContract(
            "pattern-test", "1.0", "Pattern validation test", 
            MQMessageFormat.JSON, patternFields, Map.of()
        );

        String validMessage = """
            {
                "customerId": "CUST123456",
                "email": "valid@example.com"
            }
            """;

        Map<String, Object> result = parser.parse(validMessage, patternContract);
        assertEquals("CUST123456", result.get("customerId"));
        assertEquals("valid@example.com", result.get("email"));

        // Test invalid pattern
        String invalidMessage = """
            {
                "customerId": "INVALID123",
                "email": "invalid-email"
            }
            """;

        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(invalidMessage, patternContract);
        });
    }

    @Test
    void testParseWithArrayAccess() throws MQMessageParsingException {
        String jsonMessage = """
            {
                "order": {
                    "items": [
                        {"name": "Item 1", "quantity": 5},
                        {"name": "Item 2", "quantity": 3}
                    ]
                }
            }
            """;

        List<MQField> arrayFields = List.of(
            MQField.builder("firstItemName", MQFieldType.STRING)
                .required(true).jsonPath("order.items[0].name").build(),
            MQField.builder("firstItemQuantity", MQFieldType.INTEGER)
                .required(true).jsonPath("order.items[0].quantity").build()
        );

        MQContract arrayContract = new MQContract(
            "array-test", "1.0", "Array access test", 
            MQMessageFormat.JSON, arrayFields, Map.of()
        );

        Map<String, Object> result = parser.parse(jsonMessage, arrayContract);
        assertEquals("Item 1", result.get("firstItemName"));
        assertEquals(5, result.get("firstItemQuantity"));
    }
}