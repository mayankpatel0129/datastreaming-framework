package com.datastreaming.framework.core.mq;

import com.datastreaming.framework.core.mq.parsers.DelimitedMessageParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DelimitedMessageParserTest {
    
    private DelimitedMessageParser parser;
    private MQContract csvContract;
    private MQContract pipeContract;
    
    @BeforeEach
    void setUp() {
        parser = new DelimitedMessageParser();
        
        // CSV contract with quotes
        List<MQField> csvFields = List.of(
            MQField.builder("customerId", MQFieldType.STRING)
                .required(true).pattern("^CUST\\d{6}$").build(),
            MQField.builder("firstName", MQFieldType.STRING)
                .required(true).build(),
            MQField.builder("lastName", MQFieldType.STRING)
                .required(true).build(),
            MQField.builder("email", MQFieldType.STRING)
                .required(true).build(),
            MQField.builder("birthDate", MQFieldType.DATE)
                .required(false).dateFormat("yyyy-MM-dd").build(),
            MQField.builder("registrationDate", MQFieldType.DATETIME)
                .required(true).dateFormat("yyyy-MM-dd HH:mm:ss").build(),
            MQField.builder("isActive", MQFieldType.BOOLEAN)
                .required(false).defaultValue("true").build(),
            MQField.builder("creditLimit", MQFieldType.DECIMAL)
                .required(false).defaultValue("0.00").build()
        );
        
        csvContract = new MQContract(
            "customer-csv", "1.0", "CSV customer contract", 
            MQMessageFormat.DELIMITED, csvFields, 
            Map.of(
                "delimiter", ",",
                "hasQuotes", "true",
                "quoteChar", "\"",
                "trimFields", "true",
                "strictMode", "false"
            )
        );
        
        // Pipe-delimited contract without quotes
        List<MQField> pipeFields = List.of(
            MQField.builder("orderId", MQFieldType.STRING)
                .required(true).pattern("^ORD\\d{8}$").build(),
            MQField.builder("customerId", MQFieldType.STRING)
                .required(true).build(),
            MQField.builder("productId", MQFieldType.STRING)
                .required(true).build(),
            MQField.builder("quantity", MQFieldType.INTEGER)
                .required(true).build(),
            MQField.builder("unitPrice", MQFieldType.DECIMAL)
                .required(true).build(),
            MQField.builder("totalAmount", MQFieldType.DECIMAL)
                .required(true).build(),
            MQField.builder("orderDate", MQFieldType.DATETIME)
                .required(true).dateFormat("yyyy-MM-dd HH:mm:ss").build(),
            MQField.builder("priority", MQFieldType.STRING)
                .required(false).defaultValue("NORMAL").build()
        );
        
        pipeContract = new MQContract(
            "order-pipe", "2.0", "Pipe-delimited order contract", 
            MQMessageFormat.DELIMITED, pipeFields, 
            Map.of(
                "delimiter", "|",
                "hasQuotes", "false",
                "trimFields", "true",
                "strictMode", "true"
            )
        );
    }
    
    @Test
    void testParseCSVWithQuotes() throws MQMessageParsingException {
        String message = "\"CUST123456\",\"John\",\"Doe\",\"john.doe@example.com\",\"1990-05-15\",\"2024-01-15 10:30:00\",\"true\",\"5000.00\"";
        
        Map<String, Object> result = parser.parse(message, csvContract);
        
        assertNotNull(result);
        assertEquals("CUST123456", result.get("customerId"));
        assertEquals("John", result.get("firstName"));
        assertEquals("Doe", result.get("lastName"));
        assertEquals("john.doe@example.com", result.get("email"));
        assertEquals(LocalDate.of(1990, 5, 15), result.get("birthDate"));
        assertEquals(LocalDateTime.of(2024, 1, 15, 10, 30, 0), result.get("registrationDate"));
        assertEquals(true, result.get("isActive"));
        assertEquals(new BigDecimal("5000.00"), result.get("creditLimit"));
    }
    
    @Test
    void testParseCSVWithOptionalFieldsMissing() throws MQMessageParsingException {
        String message = "\"CUST123456\",\"John\",\"Doe\",\"john.doe@example.com\",,\"2024-01-15 10:30:00\",,";
        
        Map<String, Object> result = parser.parse(message, csvContract);
        
        assertNotNull(result);
        assertEquals("CUST123456", result.get("customerId"));
        assertNull(result.get("birthDate")); // optional empty field
        assertEquals(true, result.get("isActive")); // default value
        assertEquals(new BigDecimal("0.00"), result.get("creditLimit")); // default value
    }
    
    @Test
    void testParsePipeDelimited() throws MQMessageParsingException {
        String message = "ORD12345678|CUST123456|PROD001|5|19.99|99.95|2024-07-31 14:30:05|HIGH";
        
        Map<String, Object> result = parser.parse(message, pipeContract);
        
        assertNotNull(result);
        assertEquals("ORD12345678", result.get("orderId"));
        assertEquals("CUST123456", result.get("customerId"));
        assertEquals("PROD001", result.get("productId"));
        assertEquals(5, result.get("quantity"));
        assertEquals(new BigDecimal("19.99"), result.get("unitPrice"));
        assertEquals(new BigDecimal("99.95"), result.get("totalAmount"));
        assertEquals(LocalDateTime.of(2024, 7, 31, 14, 30, 5), result.get("orderDate"));
        assertEquals("HIGH", result.get("priority"));
    }
    
    @Test
    void testParsePipeDelimitedWithMissingOptionalField() throws MQMessageParsingException {
        String message = "ORD12345678|CUST123456|PROD001|5|19.99|99.95|2024-07-31 14:30:05";
        
        Map<String, Object> result = parser.parse(message, pipeContract);
        
        assertNotNull(result);
        assertEquals("NORMAL", result.get("priority")); // default value
    }
    
    @Test
    void testParseNullMessage() {
        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(null, csvContract);
        });
    }
    
    @Test
    void testParseWithUnsupportedFormat() {
        MQContract fixedContract = new MQContract(
            "test", "1.0", "", MQMessageFormat.FIXED_LENGTH, List.of(), Map.of()
        );
        
        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse("test", fixedContract);
        });
    }
    
    @Test
    void testParseWithPatternValidationFailure() {
        String message = "\"INVALID\",\"John\",\"Doe\",\"john.doe@example.com\",,\"2024-01-15 10:30:00\",,";
        
        // This should fail in strict mode due to pattern validation
        MQContract strictContract = new MQContract(
            "customer-csv", "1.0", "CSV strict contract", 
            MQMessageFormat.DELIMITED, csvContract.getFields(), 
            Map.of(
                "delimiter", ",",
                "hasQuotes", "true",
                "quoteChar", "\"",
                "trimFields", "true",
                "strictMode", "true"
            )
        );
        
        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(message, strictContract);
        });
    }
    
    @Test
    void testParseWithRequiredFieldMissing() {
        String message = "ORD12345678|CUST123456"; // Missing required fields
        
        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(message, pipeContract);
        });
    }
    
    @Test
    void testParseInvalidNumericField() {
        String message = "ORD12345678|CUST123456|PROD001|INVALID|19.99|99.95|2024-07-31 14:30:05|HIGH";
        
        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(message, pipeContract);
        });
    }
    
    @Test
    void testParseInvalidDateField() {
        String message = "ORD12345678|CUST123456|PROD001|5|19.99|99.95|INVALID-DATE|HIGH";
        
        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(message, pipeContract);
        });
    }
    
    @Test
    void testSupportsFormat() {
        assertTrue(parser.supports(MQMessageFormat.DELIMITED));
        assertFalse(parser.supports(MQMessageFormat.FIXED_LENGTH));
        assertFalse(parser.supports(MQMessageFormat.JSON));
    }
    
    @Test
    void testGetSupportedFormat() {
        assertEquals(MQMessageFormat.DELIMITED, parser.getSupportedFormat());
    }
    
    @Test
    void testParseWithEscapedQuotes() throws MQMessageParsingException {
        String message = "\"CUST123456\",\"John \"\"The Great\"\"\",\"O'Doe\",\"john@example.com\",,\"2024-01-15 10:30:00\",,";
        
        Map<String, Object> result = parser.parse(message, csvContract);
        
        assertNotNull(result);
        assertEquals("CUST123456", result.get("customerId"));
        assertEquals("John \"The Great\"", result.get("firstName"));
        assertEquals("O'Doe", result.get("lastName"));
    }
    
    @Test
    void testParseEmptyFields() throws MQMessageParsingException {
        String message = "\"CUST123456\",\"\",\"Doe\",\"john.doe@example.com\",,\"2024-01-15 10:30:00\",,";
        
        // This should fail because firstName is required but empty
        assertThrows(MQMessageParsingException.class, () -> {
            MQContract strictContract = new MQContract(
                "customer-csv", "1.0", "CSV strict contract", 
                MQMessageFormat.DELIMITED, csvContract.getFields(), 
                Map.of(
                    "delimiter", ",",
                    "hasQuotes", "true",
                    "quoteChar", "\"",
                    "trimFields", "true",
                    "strictMode", "true"
                )
            );
            parser.parse(message, strictContract);
        });
    }
}