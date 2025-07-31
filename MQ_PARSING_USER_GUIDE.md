# MQ Message Parsing User Guide

This guide provides comprehensive documentation for using the Data Streaming Framework's MQ message parsing capabilities.

## 📋 Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Message Formats](#message-formats)
- [Contract Definition](#contract-definition)
- [Configuration](#configuration)
- [Programming Interface](#programming-interface)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Examples](#examples)

## 🔍 Overview

The MQ Message Parsing module enables the Data Streaming Framework to parse various MQ message formats using externalized, configurable contracts. This provides:

- **Type-safe message processing** with automatic type conversion
- **Flexible format support** for fixed-length, delimited, and custom formats
- **Externalized configuration** through JSON contract definitions
- **Comprehensive validation** with detailed error reporting
- **Performance optimization** through efficient parsing algorithms

### Supported Features

✅ **Message Formats**: Fixed-length, CSV, TSV, pipe-delimited, custom  
✅ **Data Types**: STRING, INTEGER, LONG, DECIMAL, BOOLEAN, DATE, DATETIME, TIME, BINARY, CUSTOM  
✅ **Validation**: Required fields, pattern matching, length validation, date formatting  
✅ **Error Handling**: Detailed exceptions with field-level error reporting  
✅ **Performance**: Optimized parsing with minimal memory allocation  
✅ **Integration**: Seamless Spring Boot auto-configuration  

## 🚀 Quick Start

### 1. Enable MQ Parsing

Add configuration to your `application.yml`:

```yaml
datastreaming:
  mq:
    enabled: true
    contracts:
      location: "classpath*:mq/contracts/**/*.json"
```

### 2. Create a Contract

Create `src/main/resources/mq/contracts/my-contract.json`:

```json
{
  "name": "simple-transaction",
  "version": "1.0",
  "format": "FIXED_LENGTH",
  "fields": [
    {"name": "id", "type": "STRING", "position": 0, "length": 10, "required": true},
    {"name": "amount", "type": "DECIMAL", "position": 10, "length": 12, "required": true}
  ]
}
```

### 3. Parse Messages

```java
@Service
public class MyService {
    
    @Autowired
    private MQMessageParsingService parsingService;
    
    public void processMessage(String message) throws MQMessageParsingException {
        Map<String, Object> parsed = parsingService.parseMessage(message, "simple-transaction");
        
        String id = (String) parsed.get("id");
        BigDecimal amount = (BigDecimal) parsed.get("amount");
        
        // Process the parsed data
        processTransaction(id, amount);
    }
}
```

## 📨 Message Formats

### Fixed-Length Messages

Fixed-length messages have fields at specific positions with defined lengths.

**Characteristics:**
- Fields are positioned by character offset (0-based)
- Each field has a fixed length
- Padding (spaces/zeros) may be used
- No delimiters between fields

**Example Message:**
```
TX12345678000000123456USD
```

**Contract Definition:**
```json
{
  "name": "fixed-transaction",
  "version": "1.0", 
  "format": "FIXED_LENGTH",
  "properties": {
    "trimFields": "true",
    "encoding": "UTF-8"
  },
  "fields": [
    {"name": "transactionId", "type": "STRING", "position": 0, "length": 10},
    {"name": "amount", "type": "DECIMAL", "position": 10, "length": 12},
    {"name": "currency", "type": "STRING", "position": 22, "length": 3}
  ]
}
```

### Delimited Messages

Delimited messages separate fields using specific characters (comma, pipe, tab, etc.).

**Characteristics:**
- Fields separated by configurable delimiter
- Optional quote character support
- Escape sequence handling
- Variable field lengths

**CSV Example:**
```
"TX12345678","123.45","USD","2024-07-31 14:30:05"
```

**Contract Definition:**
```json
{
  "name": "csv-transaction",
  "version": "1.0",
  "format": "DELIMITED", 
  "properties": {
    "delimiter": ",",
    "hasQuotes": "true",
    "quoteChar": "\"",
    "trimFields": "true"
  },
  "fields": [
    {"name": "transactionId", "type": "STRING", "required": true},
    {"name": "amount", "type": "DECIMAL", "required": true},
    {"name": "currency", "type": "STRING", "required": true},
    {"name": "timestamp", "type": "DATETIME", "dateFormat": "yyyy-MM-dd HH:mm:ss"}
  ]
}
```

**Pipe-Delimited Example:**
```
TX12345678|123.45|USD|20240731143005
```

**Contract Definition:**
```json
{
  "name": "pipe-transaction",
  "version": "1.0",
  "format": "DELIMITED",
  "properties": {
    "delimiter": "|",
    "hasQuotes": "false"
  },
  "fields": [
    {"name": "transactionId", "type": "STRING", "required": true},
    {"name": "amount", "type": "DECIMAL", "required": true}, 
    {"name": "currency", "type": "STRING", "required": true},
    {"name": "timestamp", "type": "DATETIME", "dateFormat": "yyyyMMddHHmmss"}
  ]
}
```

## 🔧 Contract Definition

### Contract Structure

Every MQ contract must include:

```json
{
  "name": "contract-name",           // Unique contract identifier
  "version": "1.0",                 // Contract version
  "description": "Description",     // Optional description
  "format": "FIXED_LENGTH",         // Message format type
  "properties": {                   // Format-specific properties
    "trimFields": "true"
  },
  "fields": [                       // Field definitions
    {
      "name": "fieldName",
      "type": "STRING",
      "position": 0,                // For fixed-length only
      "length": 10,                 // For fixed-length only
      "required": true,
      "description": "Field description",
      "pattern": "^[A-Z0-9]+$",     // Optional regex validation
      "dateFormat": "yyyy-MM-dd",   // For date/time fields
      "defaultValue": "DEFAULT"     // Default value for optional fields
    }
  ]
}
```

### Field Properties

| Property | Required | Description | Applies To |
|----------|----------|-------------|------------|
| `name` | Yes | Field identifier | All formats |
| `type` | Yes | Data type (STRING, INTEGER, etc.) | All formats |
| `position` | Yes | Start position (0-based) | Fixed-length only |
| `length` | Yes | Field length in characters | Fixed-length only |
| `required` | No | Whether field is mandatory (default: false) | All formats |
| `description` | No | Field documentation | All formats |
| `pattern` | No | Regex validation pattern | All formats |
| `dateFormat` | No | Date/time parsing format | DATE, DATETIME, TIME |
| `defaultValue` | No | Default value if empty/missing | All formats |

### Data Types

| Type | Java Type | Description | Example Values |
|------|-----------|-------------|----------------|
| `STRING` | `String` | Text data | `"Hello"`, `"TX12345"` |
| `INTEGER` | `Integer` | 32-bit signed integer | `123`, `-456` |
| `LONG` | `Long` | 64-bit signed integer | `123456789L` |
| `DECIMAL` | `BigDecimal` | Arbitrary precision decimal | `123.45`, `0.001` |
| `BOOLEAN` | `Boolean` | Boolean value | `true`, `false`, `Y`, `N`, `1`, `0` |
| `DATE` | `LocalDate` | Date without time | `2024-07-31` |
| `DATETIME` | `LocalDateTime` | Date with time | `2024-07-31 14:30:05` |
| `TIME` | `LocalTime` | Time without date | `14:30:05` |
| `BINARY` | `byte[]` | Binary data | Raw byte array |
| `CUSTOM` | `String` | Application-specific | Custom handling required |

### Contract Properties

#### Fixed-Length Properties

| Property | Default | Description |
|----------|---------|-------------|
| `trimFields` | `"true"` | Remove leading/trailing whitespace |
| `strictMode` | `"false"` | Strict validation (fail on any error) |
| `encoding` | `"UTF-8"` | Character encoding |

#### Delimited Properties

| Property | Default | Description | 
|----------|---------|-------------|
| `delimiter` | `","` | Field separator character |
| `hasQuotes` | `"false"` | Whether fields are quoted |
| `quoteChar` | `"\""` | Quote character |
| `trimFields` | `"true"` | Remove leading/trailing whitespace |
| `strictMode` | `"false"` | Strict validation mode |
| `encoding` | `"UTF-8"` | Character encoding |

## ⚙️ Configuration

### Application Configuration

Enable and configure MQ parsing in `application.yml`:

```yaml
datastreaming:
  mq:
    enabled: true
    
    # Contract loading configuration
    contracts:
      location: "classpath*:mq/contracts/**/*.json"
      reloadOnChange: false
      validateOnStartup: true
    
    # Parsing behavior
    parsing:
      strictMode: false
      trimFields: true
      defaultEncoding: "UTF-8"
      maxMessageSize: 1048576
      enableMetrics: true
    
    # Queue to topic mapping
    queueToTopicMapping:
      "FINANCE.QUEUE": "financial-transactions"
      "PAYMENTS.QUEUE": "payment-events"
    
    # Queue to contract mapping
    queueToContractMapping:
      "FINANCE.QUEUE": "transaction-fixed"
      "PAYMENTS.QUEUE": "payment-csv"
```

### Configuration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `datastreaming.mq.enabled` | boolean | `true` | Enable MQ parsing |
| `datastreaming.mq.contracts.location` | String | `classpath*:mq/contracts/**/*.json` | Contract file location pattern |
| `datastreaming.mq.contracts.reloadOnChange` | boolean | `false` | Reload contracts when files change |
| `datastreaming.mq.contracts.validateOnStartup` | boolean | `true` | Validate contracts on startup |
| `datastreaming.mq.parsing.strictMode` | boolean | `false` | Global strict parsing mode |
| `datastreaming.mq.parsing.trimFields` | boolean | `true` | Global field trimming |
| `datastreaming.mq.parsing.defaultEncoding` | String | `"UTF-8"` | Default character encoding |
| `datastreaming.mq.parsing.maxMessageSize` | int | `1048576` | Maximum message size (bytes) |
| `datastreaming.mq.parsing.enableMetrics` | boolean | `true` | Enable parsing metrics |

## 💻 Programming Interface

### MQMessageParsingService

Primary service for parsing messages:

```java
@Service
public class MessageProcessor {
    
    @Autowired
    private MQMessageParsingService parsingService;
    
    // Parse message using contract name
    public Map<String, Object> parseMessage(String message, String contractName) 
            throws MQMessageParsingException {
        return parsingService.parseMessage(message, contractName);
    }
    
    // Parse message using contract name and version
    public Map<String, Object> parseMessage(String message, String contractName, String version) 
            throws MQMessageParsingException {
        return parsingService.parseMessage(message, contractName, version);
    }
    
    // Parse message using contract object
    public Map<String, Object> parseMessage(String message, MQContract contract) 
            throws MQMessageParsingException {
        return parsingService.parseMessage(message, contract);
    }
    
    // Validate message format
    public boolean validateMessage(String message, String contractName) {
        return parsingService.validateMessageFormat(message, contractName);
    }
    
    // Get supported formats
    public List<MQMessageFormat> getSupportedFormats() {
        return parsingService.getSupportedFormats();
    }
}
```

### MQContractRegistry

Service for managing contracts:

```java
@Component
public class ContractManager {
    
    @Autowired
    private MQContractRegistry contractRegistry;
    
    // Get contract by name (latest version)
    public MQContract getContract(String name) {
        return contractRegistry.getContract(name);
    }
    
    // Get contract by name and version
    public MQContract getContract(String name, String version) {
        return contractRegistry.getContract(name, version);
    }
    
    // Register contract programmatically
    public void registerContract(MQContract contract) {
        contractRegistry.registerContract(contract);
    }
    
    // Remove contract
    public void removeContract(String name, String version) {
        contractRegistry.removeContract(name, version);
    }
    
    // Get all contracts
    public Map<String, MQContract> getAllContracts() {
        return contractRegistry.getAllContracts();
    }
    
    // Reload contracts from files
    public void reloadContracts() {
        contractRegistry.reloadContracts();
    }
    
    // Get registry statistics
    public MQContractRegistry.MQContractRegistryStats getStats() {
        return contractRegistry.getStats();
    }
}
```

### MQToKafkaTransformer

Transform parsed messages for Kafka:

```java
@Service
public class MessageTransformationService {
    
    @Autowired
    private MQToKafkaTransformer transformer;
    
    // Transform message using contract
    public Object transform(String message, String contractName) {
        return transformer.transform(message, contractName);
    }
    
    // Transform with metadata
    public Object transformWithMetadata(String message, String contractName, 
                                      String sourceQueue, String targetTopic) {
        return transformer.transformWithMetadata(message, contractName, sourceQueue, targetTopic);
    }
    
    // Validate message before transformation
    public boolean validateMessage(String message, String contractName) {
        return transformer.validateMessage(message, contractName);
    }
}
```

### Exception Handling

Handle parsing errors with detailed information:

```java
public void processMessage(String message, String contractName) {
    try {
        Map<String, Object> parsed = parsingService.parseMessage(message, contractName);
        // Process parsed message
        
    } catch (MQMessageParsingException e) {
        // Get detailed error information
        String contractName = e.getContractName();
        String messageContent = e.getMessageContent();
        int fieldIndex = e.getFieldIndex();
        Throwable rootCause = e.getCause();
        
        logger.error("Parsing failed for contract '{}' at field index {}: {}", 
                    contractName, fieldIndex, e.getMessage());
        
        // Handle specific error scenarios
        if (fieldIndex >= 0) {
            logger.error("Error in field at index: {}", fieldIndex);
        }
        
        if (rootCause instanceof NumberFormatException) {
            logger.error("Invalid number format in message");
        }
        
        // Implement error recovery logic
        handleParsingError(e);
    }
}
```

## 🎯 Best Practices

### Contract Design

1. **Use Descriptive Names**: Choose clear, meaningful contract and field names
   ```json
   {"name": "customer-registration-v1"}  // Good
   {"name": "cr1"}                       // Poor
   ```

2. **Version Your Contracts**: Always specify explicit versions
   ```json
   {"name": "transaction", "version": "1.0"}  // Good
   {"name": "transaction"}                    // Poor - no version
   ```

3. **Document Fields**: Provide descriptions for complex fields
   ```json
   {
     "name": "amount",
     "type": "DECIMAL", 
     "description": "Transaction amount in account currency",
     "required": true
   }
   ```

4. **Use Appropriate Data Types**: Choose the most specific type
   ```json
   {"name": "price", "type": "DECIMAL"}     // Good for money
   {"name": "price", "type": "STRING"}      // Poor - requires conversion
   ```

5. **Implement Validation**: Use patterns and constraints
   ```json
   {
     "name": "email",
     "type": "STRING",
     "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"
   }
   ```

### Performance Optimization

1. **Cache Contracts**: Contracts are automatically cached, but avoid frequent reloading
2. **Use Appropriate Modes**: Use lenient mode for better performance when possible
3. **Optimize Field Order**: Put frequently accessed fields first in delimited messages
4. **Minimize Pattern Matching**: Use patterns only when necessary
5. **Choose Efficient Formats**: Fixed-length parsing is faster than delimited

### Error Handling

1. **Implement Graceful Degradation**: Handle parsing errors appropriately
   ```java
   try {
       parsed = parsingService.parseMessage(message, contract);
   } catch (MQMessageParsingException e) {
       // Log error and use fallback processing
       logger.warn("Parse failed, using raw message: {}", e.getMessage());
       processRawMessage(message);
   }
   ```

2. **Log Detailed Information**: Include context in error logs
   ```java
   logger.error("Failed to parse message from queue '{}' using contract '{}': {}", 
               queueName, contractName, e.getMessage());
   ```

3. **Monitor Parsing Metrics**: Track success/failure rates
4. **Validate Before Processing**: Use format validation for early error detection

### Testing

1. **Test Contract Validity**: Validate contracts during development
   ```java
   @Test
   public void testContractValidity() {
       MQContract contract = loadContract("my-contract.json");
       assertNotNull(contract);
       assertFalse(contract.getFields().isEmpty());
   }
   ```

2. **Test Message Parsing**: Verify parsing with sample messages
   ```java
   @Test 
   public void testMessageParsing() throws MQMessageParsingException {
       String message = "TX12345678000001234.56USD";
       Map<String, Object> result = parser.parse(message, contract);
       
       assertEquals("TX12345678", result.get("transactionId"));
       assertEquals(new BigDecimal("1234.56"), result.get("amount"));
   }
   ```

3. **Test Error Scenarios**: Verify error handling
   ```java
   @Test
   public void testInvalidMessage() {
       String invalidMessage = "INVALID";
       assertThrows(MQMessageParsingException.class, 
                   () -> parser.parse(invalidMessage, contract));
   }
   ```

## 🐛 Troubleshooting

### Common Issues

#### Contract Not Found
```
Contract not found: my-contract-1.0
```

**Solutions:**
- Verify contract file exists in configured location
- Check contract name and version in JSON file
- Ensure file has `.json` extension
- Verify classpath resource loading

#### Field Position Errors
```
Field position 15 exceeds message length 10
```

**Solutions:**
- Check message format matches contract
- Verify field positions and lengths in contract
- Ensure message is not truncated
- Check for character encoding issues

#### Type Conversion Errors
```
Cannot convert 'INVALID' to DECIMAL for field 'amount'
```

**Solutions:**
- Validate message data format
- Check field type definitions in contract
- Implement proper data validation at source
- Use lenient mode for better error tolerance

#### Date Parsing Errors
```
Cannot parse date/time '20240731' using format 'yyyy-MM-dd'
```

**Solutions:**
- Verify date format in contract matches message
- Check for consistent date formatting
- Handle missing date components
- Use appropriate date/time types

### Debugging Tips

1. **Enable Debug Logging**:
   ```yaml
   logging:
     level:
       com.datastreaming.framework.core.mq: DEBUG
   ```

2. **Use Message Validation**: Test format compatibility before parsing
   ```java
   if (!parsingService.validateMessageFormat(message, contractName)) {
       logger.warn("Message format validation failed");
   }
   ```

3. **Inspect Contract Registry**: Check loaded contracts
   ```java
   var stats = contractRegistry.getStats();
   logger.info("Loaded contracts: {}", stats);
   ```

4. **Test with Sample Data**: Create unit tests with known good/bad messages

5. **Monitor Performance**: Track parsing times and error rates

## 📚 Examples

### Complete Fixed-Length Example

**Contract** (`transaction-fixed.json`):
```json
{
  "name": "transaction-fixed",
  "version": "1.0",
  "description": "Banking transaction fixed-length format",
  "format": "FIXED_LENGTH",
  "properties": {
    "trimFields": "true",
    "strictMode": "false",
    "encoding": "UTF-8"
  },
  "fields": [
    {
      "name": "recordType",
      "type": "STRING",
      "position": 0,
      "length": 2,
      "required": true,
      "description": "Record type identifier",
      "pattern": "^[A-Z]{2}$"
    },
    {
      "name": "transactionId",
      "type": "STRING", 
      "position": 2,
      "length": 12,
      "required": true,
      "description": "Unique transaction identifier"
    },
    {
      "name": "accountNumber",
      "type": "STRING",
      "position": 14,
      "length": 16,
      "required": true,
      "description": "Customer account number"
    },
    {
      "name": "amount",
      "type": "DECIMAL",
      "position": 30,
      "length": 18,
      "required": true,
      "description": "Transaction amount with 2 decimal places"
    },
    {
      "name": "currency",
      "type": "STRING",
      "position": 48,
      "length": 3,
      "required": true,
      "description": "ISO currency code",
      "pattern": "^[A-Z]{3}$"
    },
    {
      "name": "transactionDate",
      "type": "DATE",
      "position": 51,
      "length": 8,
      "required": true,
      "description": "Transaction date",
      "dateFormat": "yyyyMMdd"
    },
    {
      "name": "transactionTime",
      "type": "TIME",
      "position": 59,
      "length": 6,
      "required": true,
      "description": "Transaction time",
      "dateFormat": "HHmmss"
    },
    {
      "name": "status",
      "type": "STRING",
      "position": 65,
      "length": 1,
      "required": false,
      "description": "Transaction status",
      "defaultValue": "P"
    },
    {
      "name": "isReversal",
      "type": "BOOLEAN",
      "position": 66,
      "length": 1,
      "required": false,
      "description": "Reversal flag",
      "defaultValue": "0"
    }
  ]
}
```

**Sample Message**:
```
TXTX123456789001234567890123450000000000001234.56USD20240731143005P0
```

**Processing Code**:
```java
@Service
public class TransactionProcessor {
    
    @Autowired
    private MQMessageParsingService parsingService;
    
    public void processTransaction(String message) {
        try {
            Map<String, Object> parsed = parsingService.parseMessage(message, "transaction-fixed");
            
            // Extract typed values
            String recordType = (String) parsed.get("recordType");
            String transactionId = (String) parsed.get("transactionId");
            String accountNumber = (String) parsed.get("accountNumber");
            BigDecimal amount = (BigDecimal) parsed.get("amount");
            String currency = (String) parsed.get("currency");
            LocalDate transactionDate = (LocalDate) parsed.get("transactionDate");
            LocalTime transactionTime = (LocalTime) parsed.get("transactionTime");
            String status = (String) parsed.get("status");
            Boolean isReversal = (Boolean) parsed.get("isReversal");
            
            // Process the transaction
            Transaction transaction = new Transaction();
            transaction.setRecordType(recordType);
            transaction.setTransactionId(transactionId);
            transaction.setAccountNumber(accountNumber);
            transaction.setAmount(amount);
            transaction.setCurrency(currency);
            transaction.setTransactionDate(transactionDate);
            transaction.setTransactionTime(transactionTime);
            transaction.setStatus(status);
            transaction.setReversal(isReversal);
            
            // Save or publish
            transactionService.saveTransaction(transaction);
            
        } catch (MQMessageParsingException e) {
            logger.error("Failed to parse transaction: {}", e.getMessage());
            errorHandler.handleParsingError(message, e);
        }
    }
}
```

### Complete CSV Example

**Contract** (`customer-registration.json`):
```json
{
  "name": "customer-registration",
  "version": "2.0",
  "description": "Customer registration CSV format",
  "format": "DELIMITED",
  "properties": {
    "delimiter": ",",
    "hasQuotes": "true",
    "quoteChar": "\"",
    "trimFields": "true",
    "strictMode": "true",
    "encoding": "UTF-8"
  },
  "fields": [
    {
      "name": "customerId",
      "type": "STRING",
      "required": true,
      "description": "Unique customer identifier",
      "pattern": "^CUST[0-9]{8}$"
    },
    {
      "name": "firstName",
      "type": "STRING",
      "required": true,
      "description": "Customer first name"
    },
    {
      "name": "lastName",
      "type": "STRING", 
      "required": true,
      "description": "Customer last name"
    },
    {
      "name": "email",
      "type": "STRING",
      "required": true,
      "description": "Customer email address",
      "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"
    },
    {
      "name": "phoneNumber",
      "type": "STRING",
      "required": false,
      "description": "Customer phone number",
      "pattern": "^\\+?[1-9]\\d{1,14}$"
    },
    {
      "name": "birthDate",
      "type": "DATE",
      "required": false,
      "description": "Customer birth date",
      "dateFormat": "yyyy-MM-dd"
    },
    {
      "name": "registrationDate",
      "type": "DATETIME",
      "required": true,
      "description": "Registration timestamp",
      "dateFormat": "yyyy-MM-dd HH:mm:ss"
    },
    {
      "name": "isVip",
      "type": "BOOLEAN",
      "required": false,
      "description": "VIP status flag",
      "defaultValue": "false"
    },
    {
      "name": "creditLimit",
      "type": "DECIMAL",
      "required": false,
      "description": "Initial credit limit",
      "defaultValue": "1000.00"
    },
    {
      "name": "preferredContact",
      "type": "STRING",
      "required": false,
      "description": "Preferred contact method",
      "defaultValue": "EMAIL"
    }
  ]
}
```

**Sample Message**:
```csv
"CUST12345678","John","Doe","john.doe@example.com","+1234567890","1985-03-15","2024-07-31 14:30:05","true","5000.00","EMAIL"
```

**Processing Code**:
```java
@Service
public class CustomerRegistrationProcessor {
    
    @Autowired
    private MQMessageParsingService parsingService;
    
    @Autowired
    private CustomerService customerService;
    
    public void processRegistration(String csvMessage) {
        try {
            Map<String, Object> parsed = parsingService.parseMessage(csvMessage, "customer-registration");
            
            // Build customer object
            Customer customer = Customer.builder()
                .customerId((String) parsed.get("customerId"))
                .firstName((String) parsed.get("firstName"))
                .lastName((String) parsed.get("lastName"))
                .email((String) parsed.get("email"))
                .phoneNumber((String) parsed.get("phoneNumber"))
                .birthDate((LocalDate) parsed.get("birthDate"))
                .registrationDate((LocalDateTime) parsed.get("registrationDate"))
                .isVip((Boolean) parsed.get("isVip"))
                .creditLimit((BigDecimal) parsed.get("creditLimit"))
                .preferredContact((String) parsed.get("preferredContact"))
                .build();
            
            // Validate business rules
            validateCustomer(customer);
            
            // Save customer
            customerService.registerCustomer(customer);
            
            logger.info("Successfully registered customer: {}", customer.getCustomerId());
            
        } catch (MQMessageParsingException e) {
            logger.error("Failed to parse customer registration: {}", e.getMessage());
            // Send to error queue or handle appropriately
            handleRegistrationError(csvMessage, e);
        } catch (CustomerValidationException e) {
            logger.error("Customer validation failed: {}", e.getMessage());
            // Handle business validation errors
            handleValidationError(csvMessage, e);
        }
    }
    
    private void validateCustomer(Customer customer) throws CustomerValidationException {
        // Implement business validation rules
        if (customer.getBirthDate() != null && customer.getBirthDate().isAfter(LocalDate.now().minusYears(18))) {
            throw new CustomerValidationException("Customer must be at least 18 years old");
        }
        
        if (customer.getCreditLimit().compareTo(BigDecimal.ZERO) < 0) {
            throw new CustomerValidationException("Credit limit cannot be negative");
        }
    }
}
```

This comprehensive guide covers all aspects of using the MQ Message Parsing capabilities in the Data Streaming Framework. For additional support or advanced use cases, refer to the main framework documentation or contact the development team.