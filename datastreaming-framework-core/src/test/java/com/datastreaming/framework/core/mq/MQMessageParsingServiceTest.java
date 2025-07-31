package com.datastreaming.framework.core.mq;

import com.datastreaming.framework.core.mq.parsers.DelimitedMessageParser;
import com.datastreaming.framework.core.mq.parsers.FixedLengthMessageParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MQMessageParsingServiceTest {
    
    @Mock
    private MQContractRegistry contractRegistry;
    
    private MQMessageParsingService parsingService;
    private MQContract fixedLengthContract;
    private MQContract delimitedContract;
    
    @BeforeEach
    void setUp() {
        List<MQMessageParser> parsers = List.of(
            new FixedLengthMessageParser(),
            new DelimitedMessageParser()
        );
        
        parsingService = new MQMessageParsingService(parsers, contractRegistry);
        
        // Setup test contracts
        List<MQField> fixedFields = List.of(
            MQField.builder("id", MQFieldType.STRING)
                .position(0).length(10).required(true).build(),
            MQField.builder("name", MQFieldType.STRING)
                .position(10).length(20).required(true).build(),
            MQField.builder("amount", MQFieldType.DECIMAL)
                .position(30).length(10).required(true).build()
        );
        
        fixedLengthContract = new MQContract(
            "test-fixed", "1.0", "Test fixed-length contract",
            MQMessageFormat.FIXED_LENGTH, fixedFields,
            Map.of("trimFields", "true", "strictMode", "false")
        );
        
        List<MQField> delimitedFields = List.of(
            MQField.builder("id", MQFieldType.STRING).required(true).build(),
            MQField.builder("name", MQFieldType.STRING).required(true).build(),
            MQField.builder("amount", MQFieldType.DECIMAL).required(true).build()
        );
        
        delimitedContract = new MQContract(
            "test-delimited", "1.0", "Test delimited contract",
            MQMessageFormat.DELIMITED, delimitedFields,
            Map.of("delimiter", ",", "trimFields", "true", "strictMode", "false")
        );
    }
    
    @Test
    void testParseMessageWithContractName() throws MQMessageParsingException {
        String message = "ID12345678NAME_TEST_123      100.50";
        
        when(contractRegistry.getContract("test-fixed", null))
            .thenReturn(fixedLengthContract);
        
        Map<String, Object> result = parsingService.parseMessage(message, "test-fixed");
        
        assertNotNull(result);
        assertEquals("ID12345678", result.get("id"));
        assertEquals("NAME_TEST_123", result.get("name"));
        assertEquals(new java.math.BigDecimal("100.50"), result.get("amount"));
        
        verify(contractRegistry).getContract("test-fixed", null);
    }
    
    @Test
    void testParseMessageWithContractNameAndVersion() throws MQMessageParsingException {
        String message = "ID123,TEST_NAME,150.75";
        
        when(contractRegistry.getContract("test-delimited", "1.0"))
            .thenReturn(delimitedContract);
        
        Map<String, Object> result = parsingService.parseMessage(message, "test-delimited", "1.0");
        
        assertNotNull(result);
        assertEquals("ID123", result.get("id"));
        assertEquals("TEST_NAME", result.get("name"));
        assertEquals(new java.math.BigDecimal("150.75"), result.get("amount"));
        
        verify(contractRegistry).getContract("test-delimited", "1.0");
    }
    
    @Test
    void testParseMessageWithProvidedContract() throws MQMessageParsingException {
        String message = "ID123,TEST_NAME,150.75";
        
        Map<String, Object> result = parsingService.parseMessage(message, delimitedContract);
        
        assertNotNull(result);
        assertEquals("ID123", result.get("id"));
        assertEquals("TEST_NAME", result.get("name"));
        assertEquals(new java.math.BigDecimal("150.75"), result.get("amount"));
        
        verifyNoInteractions(contractRegistry);
    }
    
    @Test
    void testParseMessageWithContractNotFound() {
        String message = "test message";
        
        when(contractRegistry.getContract("non-existent", null))
            .thenReturn(null);
        
        assertThrows(MQMessageParsingException.class, () -> {
            parsingService.parseMessage(message, "non-existent");
        });
        
        verify(contractRegistry).getContract("non-existent", null);
    }
    
    @Test
    void testParseMessageWithNullContent() {
        assertThrows(MQMessageParsingException.class, () -> {
            parsingService.parseMessage(null, delimitedContract);
        });
    }
    
    @Test
    void testParseMessageWithNullContract() {
        assertThrows(MQMessageParsingException.class, () -> {
            parsingService.parseMessage("test", (MQContract) null);
        });
    }
    
    @Test
    void testParseMessageWithUnsupportedFormat() {
        List<MQField> jsonFields = List.of(
            MQField.builder("id", MQFieldType.STRING).required(true).build()
        );
        
        MQContract jsonContract = new MQContract(
            "test-json", "1.0", "Test JSON contract",
            MQMessageFormat.JSON, jsonFields, Map.of()
        );
        
        assertThrows(MQMessageParsingException.class, () -> {
            parsingService.parseMessage("{\"id\":\"123\"}", jsonContract);
        });
    }
    
    @Test
    void testValidateMessageFormat() {
        String message = "ID123,TEST_NAME,150.75";
        
        when(contractRegistry.getContract("test-delimited", null))
            .thenReturn(delimitedContract);
        
        boolean result = parsingService.validateMessageFormat(message, "test-delimited");
        
        assertTrue(result);
        verify(contractRegistry).getContract("test-delimited", null);
    }
    
    @Test
    void testValidateMessageFormatWithVersion() {
        String message = "ID123,TEST_NAME,150.75";
        
        when(contractRegistry.getContract("test-delimited", "1.0"))
            .thenReturn(delimitedContract);
        
        boolean result = parsingService.validateMessageFormat(message, "test-delimited", "1.0");
        
        assertTrue(result);
        verify(contractRegistry).getContract("test-delimited", "1.0");
    }
    
    @Test
    void testValidateMessageFormatWithContract() {
        String message = "ID123,TEST_NAME,150.75";
        
        boolean result = parsingService.validateMessageFormat(message, delimitedContract);
        
        assertTrue(result);
    }
    
    @Test
    void testValidateMessageFormatInvalid() {
        String message = "ID123"; // Too few fields
        
        boolean result = parsingService.validateMessageFormat(message, delimitedContract);
        
        assertFalse(result);
    }
    
    @Test
    void testValidateMessageFormatWithNullMessage() {
        boolean result = parsingService.validateMessageFormat(null, delimitedContract);
        
        assertFalse(result);
    }
    
    @Test
    void testValidateMessageFormatWithNullContract() {
        boolean result = parsingService.validateMessageFormat("test", (MQContract) null);
        
        assertFalse(result);
    }
    
    @Test
    void testGetSupportedFormats() {
        List<MQMessageFormat> formats = parsingService.getSupportedFormats();
        
        assertNotNull(formats);
        assertEquals(2, formats.size());
        assertTrue(formats.contains(MQMessageFormat.FIXED_LENGTH));
        assertTrue(formats.contains(MQMessageFormat.DELIMITED));
    }
    
    @Test
    void testGetParserInfo() {
        Map<MQMessageFormat, String> parserInfo = parsingService.getParserInfo();
        
        assertNotNull(parserInfo);
        assertEquals(2, parserInfo.size());
        assertEquals("FixedLengthMessageParser", parserInfo.get(MQMessageFormat.FIXED_LENGTH));
        assertEquals("DelimitedMessageParser", parserInfo.get(MQMessageFormat.DELIMITED));
    }
    
    @Test
    void testParseMessageHandlesParsingException() {
        String invalidMessage = "INVALID"; // Too short for fixed-length
        
        when(contractRegistry.getContract("test-fixed", null))
            .thenReturn(fixedLengthContract);
        
        // Should handle the parsing exception gracefully and re-throw as MQMessageParsingException
        assertThrows(MQMessageParsingException.class, () -> {
            parsingService.parseMessage(invalidMessage, "test-fixed");
        });
    }
    
    @Test
    void testValidateFixedLengthFormat() {
        String validMessage = "ID12345678NAME_TEST_123      100.50"; // 40 characters
        String invalidMessage = "SHORT"; // Too short
        
        assertTrue(parsingService.validateMessageFormat(validMessage, fixedLengthContract));
        assertFalse(parsingService.validateMessageFormat(invalidMessage, fixedLengthContract));
    }
    
    @Test
    void testValidateDelimitedFormat() {
        String validMessage = "ID123,NAME,100.50"; // 3 fields
        String invalidMessage = "ID123,NAME"; // Only 2 fields, missing required field
        
        assertTrue(parsingService.validateMessageFormat(validMessage, delimitedContract));
        assertFalse(parsingService.validateMessageFormat(invalidMessage, delimitedContract));
    }
}