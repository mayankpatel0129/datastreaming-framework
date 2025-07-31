package com.datastreaming.framework.core.mq;

import com.datastreaming.framework.core.mq.parsers.FixedLengthMessageParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FixedLengthMessageParserTest {
    
    private FixedLengthMessageParser parser;
    private MQContract contract;
    
    @BeforeEach
    void setUp() {
        parser = new FixedLengthMessageParser();
        
        List<MQField> fields = List.of(
            MQField.builder("transactionId", MQFieldType.STRING)
                .position(0).length(10).required(true).build(),
            MQField.builder("accountNumber", MQFieldType.STRING)
                .position(10).length(12).required(true).build(),
            MQField.builder("amount", MQFieldType.DECIMAL)
                .position(22).length(15).required(true).build(),
            MQField.builder("currency", MQFieldType.STRING)
                .position(37).length(3).required(true).build(),
            MQField.builder("transactionDate", MQFieldType.DATE)
                .position(40).length(8).required(true).dateFormat("yyyyMMdd").build(),
            MQField.builder("transactionTime", MQFieldType.TIME)
                .position(48).length(6).required(true).dateFormat("HHmmss").build(),
            MQField.builder("status", MQFieldType.STRING)
                .position(54).length(1).required(false).defaultValue("P").build(),
            MQField.builder("isVip", MQFieldType.BOOLEAN)
                .position(55).length(1).required(false).defaultValue("0").build()
        );
        
        contract = new MQContract(
            "transaction-fixed", "1.0", "Test contract", 
            MQMessageFormat.FIXED_LENGTH, fields, 
            Map.of("trimFields", "true", "strictMode", "false")
        );
    }
    
    @Test
    void testParseValidMessage() throws MQMessageParsingException {
        // Create properly formatted message: positions 0-55 (56 chars total)
        // TX12345678 (10) + ACC123456789 (12) + 000000001234.56 (15) + USD (3) + 20240731 (8) + 143005 (6) + P (1) + 1 (1)
        String message = "TX12345678ACC123456789000000001234.56USD20240731143005P1";
        
        Map<String, Object> result = parser.parse(message, contract);
        
        assertNotNull(result);
        assertEquals("TX12345678", result.get("transactionId"));
        assertEquals("ACC123456789", result.get("accountNumber"));
        assertEquals(new BigDecimal("000000001234.56"), result.get("amount"));
        assertEquals("USD", result.get("currency"));
        assertEquals(LocalDate.of(2024, 7, 31), result.get("transactionDate"));
        assertEquals(LocalTime.of(14, 30, 5), result.get("transactionTime"));
        assertEquals("P", result.get("status"));
        assertEquals(true, result.get("isVip"));
    }
    
    @Test
    void testParseWithOptionalFieldsMissing() throws MQMessageParsingException {
        String message = "TX12345678ACC123456789000000001234.56USD20240731143005";
        
        Map<String, Object> result = parser.parse(message, contract);
        
        assertNotNull(result);
        assertEquals("P", result.get("status")); // default value
        assertEquals(false, result.get("isVip")); // default value
    }
    
    @Test
    void testParseNullMessage() {
        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse(null, contract);
        });
    }
    
    @Test
    void testParseWithUnsupportedFormat() {
        MQContract delimitedContract = new MQContract(
            "test", "1.0", "", MQMessageFormat.DELIMITED, List.of(), Map.of()
        );
        
        assertThrows(MQMessageParsingException.class, () -> {
            parser.parse("test", delimitedContract);
        });
    }
    
    @Test
    void testParseMessageTooShort() {
        String shortMessage = "TX12345678";
        
        assertThrows(MQMessageParsingException.class, () -> {
            // Create strict mode contract
            MQContract strictContract = new MQContract(
                "transaction-fixed", "1.0", "Test contract", 
                MQMessageFormat.FIXED_LENGTH, contract.getFields(), 
                Map.of("trimFields", "true", "strictMode", "true")
            );
            parser.parse(shortMessage, strictContract);
        });
    }
    
    @Test
    void testParseInvalidNumericField() {
        String message = "TX12345678ACC123456789     INVALID USD2024073114305P1";
        
        assertThrows(MQMessageParsingException.class, () -> {
            MQContract strictContract = new MQContract(
                "transaction-fixed", "1.0", "Test contract", 
                MQMessageFormat.FIXED_LENGTH, contract.getFields(), 
                Map.of("trimFields", "true", "strictMode", "true")
            );
            parser.parse(message, strictContract);
        });
    }
    
    @Test
    void testParseInvalidDateField() {
        String message = "TX12345678ACC123456789     1234.56USD20240231143055P1";
        
        assertThrows(MQMessageParsingException.class, () -> {
            MQContract strictContract = new MQContract(
                "transaction-fixed", "1.0", "Test contract", 
                MQMessageFormat.FIXED_LENGTH, contract.getFields(), 
                Map.of("trimFields", "true", "strictMode", "true")
            );
            parser.parse(message, strictContract);
        });
    }
    
    @Test
    void testSupportsFormat() {
        assertTrue(parser.supports(MQMessageFormat.FIXED_LENGTH));
        assertFalse(parser.supports(MQMessageFormat.DELIMITED));
        assertFalse(parser.supports(MQMessageFormat.JSON));
    }
    
    @Test
    void testGetSupportedFormat() {
        assertEquals(MQMessageFormat.FIXED_LENGTH, parser.getSupportedFormat());
    }
    
    @Test
    void testBooleanConversion() throws MQMessageParsingException {
        List<MQField> boolFields = List.of(
            MQField.builder("flag1", MQFieldType.BOOLEAN).position(0).length(1).required(true).build(),
            MQField.builder("flag2", MQFieldType.BOOLEAN).position(1).length(1).required(true).build(),
            MQField.builder("flag3", MQFieldType.BOOLEAN).position(2).length(4).required(true).build(),
            MQField.builder("flag4", MQFieldType.BOOLEAN).position(6).length(1).required(true).build()
        );
        
        MQContract boolContract = new MQContract(
            "bool-test", "1.0", "Boolean test", 
            MQMessageFormat.FIXED_LENGTH, boolFields, Map.of("trimFields", "true")
        );
        
        String message = "1ttrue Y";
        Map<String, Object> result = parser.parse(message, boolContract);
        
        assertEquals(true, result.get("flag1"));  // "1" → true
        assertEquals(true, result.get("flag2"));  // "t" → true
        assertEquals(true, result.get("flag3"));  // "true" → true
        assertEquals(true, result.get("flag4"));  // "Y" → true
    }
}