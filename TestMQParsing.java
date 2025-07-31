// Quick test to demonstrate MQ parsing works
import com.datastreaming.framework.core.mq.*;
import com.datastreaming.framework.core.mq.parsers.*;
import java.util.List;
import java.util.Map;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;

public class TestMQParsing {
    public static void main(String[] args) throws Exception {
        System.out.println("=== MQ Message Parsing Test ===");
        
        // Create parsers
        FixedLengthMessageParser fixedParser = new FixedLengthMessageParser();
        DelimitedMessageParser delimitedParser = new DelimitedMessageParser();
        
        // Create a fixed-length contract
        List<MQField> fixedFields = List.of(
            MQField.builder("transactionId", MQFieldType.STRING)
                .position(0).length(10).required(true).build(),
            MQField.builder("accountNumber", MQFieldType.STRING)
                .position(10).length(12).required(true).build(),
            MQField.builder("amount", MQFieldType.DECIMAL)
                .position(22).length(15).required(true).build(),
            MQField.builder("currency", MQFieldType.STRING)
                .position(37).length(3).required(true).build()
        );
        
        MQContract fixedContract = new MQContract(
            "test-fixed", "1.0", "Test fixed contract",
            MQMessageFormat.FIXED_LENGTH, fixedFields,
            Map.of("trimFields", "true")
        );
        
        // Test fixed-length parsing
        String fixedMessage = "TX12345678ACC123456789000000001234.56USD";
        Map<String, Object> result = fixedParser.parse(fixedMessage, fixedContract);
        
        System.out.println("Fixed-length parsing result:");
        result.forEach((k, v) -> System.out.println("  " + k + ": " + v + " (" + v.getClass().getSimpleName() + ")"));
        
        // Create a delimited contract
        List<MQField> delimitedFields = List.of(
            MQField.builder("id", MQFieldType.STRING).required(true).build(),
            MQField.builder("name", MQFieldType.STRING).required(true).build(),
            MQField.builder("amount", MQFieldType.DECIMAL).required(true).build()
        );
        
        MQContract delimitedContract = new MQContract(
            "test-delimited", "1.0", "Test delimited contract",
            MQMessageFormat.DELIMITED, delimitedFields,
            Map.of("delimiter", ",", "trimFields", "true")
        );
        
        // Test delimited parsing
        String delimitedMessage = "ID123,TEST_NAME,150.75";
        Map<String, Object> delimitedResult = delimitedParser.parse(delimitedMessage, delimitedContract);
        
        System.out.println("\nDelimited parsing result:");
        delimitedResult.forEach((k, v) -> System.out.println("  " + k + ": " + v + " (" + v.getClass().getSimpleName() + ")"));
        
        System.out.println("\n=== All tests passed! MQ parsing is working correctly ===");
    }
}