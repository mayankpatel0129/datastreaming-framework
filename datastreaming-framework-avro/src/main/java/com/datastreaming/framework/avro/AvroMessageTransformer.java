package com.datastreaming.framework.avro;

import com.datastreaming.framework.core.api.ContractRegistry;
import com.datastreaming.framework.core.api.MessageTransformer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

/**
 * Avro-based message transformer that converts messages using Avro schemas
 */
@Component
public class AvroMessageTransformer implements MessageTransformer {

    private static final Logger logger = LoggerFactory.getLogger(AvroMessageTransformer.class);

    @Autowired
    private ContractRegistry contractRegistry;

    @Autowired
    private AvroSchemaManager schemaManager;

    @Override
    public String transform(Message sourceMessage, String sourceContract, String targetContract, String correlationId) 
            throws TransformationException {
        
        try {
            logger.debug("Transforming message from {} to {} with correlationId: {}", 
                        sourceContract, targetContract, correlationId);

            // Get source and target schemas
            Schema sourceSchema = getSchema(sourceContract);
            Schema targetSchema = getSchema(targetContract);

            // Extract message content
            String messageContent = extractMessageContent(sourceMessage);

            // Parse source message using source schema
            GenericRecord sourceRecord = parseMessage(messageContent, sourceSchema);

            // Transform to target format
            GenericRecord targetRecord = transformRecord(sourceRecord, sourceSchema, targetSchema, correlationId);

            // Serialize target record
            String result = serializeRecord(targetRecord, targetSchema);

            logger.debug("Message transformation completed successfully for correlationId: {}", correlationId);
            return result;

        } catch (Exception e) {
            logger.error("Error transforming message with correlationId: {}", correlationId, e);
            throw new TransformationException("Failed to transform message: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean supports(String sourceContract, String targetContract) {
        return contractRegistry.hasContract(sourceContract) && 
               contractRegistry.hasContract(targetContract) &&
               isAvroContract(sourceContract) &&
               isAvroContract(targetContract);
    }

    @Override
    public String getName() {
        return "AvroMessageTransformer";
    }

    private Schema getSchema(String contractId) throws TransformationException {
        Optional<ContractRegistry.ContractDefinition> contractOpt = contractRegistry.getContract(contractId);
        
        if (contractOpt.isEmpty()) {
            throw new TransformationException("Contract not found: " + contractId);
        }

        ContractRegistry.ContractDefinition contract = contractOpt.get();
        
        if (contract.getType() != ContractRegistry.ContractType.AVRO) {
            throw new TransformationException("Contract is not Avro type: " + contractId);
        }

        try {
            return new Schema.Parser().parse(contract.getDefinition());
        } catch (Exception e) {
            throw new TransformationException("Invalid Avro schema for contract: " + contractId, e);
        }
    }

    private String extractMessageContent(Message message) throws TransformationException {
        try {
            if (message instanceof TextMessage) {
                return ((TextMessage) message).getText();
            } else {
                throw new TransformationException("Unsupported message type: " + message.getClass().getSimpleName());
            }
        } catch (Exception e) {
            throw new TransformationException("Failed to extract message content", e);
        }
    }

    private GenericRecord parseMessage(String messageContent, Schema schema) throws TransformationException {
        try {
            // If message is JSON, convert to Avro
            if (messageContent.trim().startsWith("{")) {
                return parseJsonToAvro(messageContent, schema);
            } else {
                // Assume it's already Avro binary format
                return parseAvroMessage(messageContent.getBytes(), schema);
            }
        } catch (Exception e) {
            throw new TransformationException("Failed to parse message content", e);
        }
    }

    private GenericRecord parseJsonToAvro(String jsonContent, Schema schema) throws IOException {
        // Convert JSON to Avro using schema
        GenericRecord record = new GenericData.Record(schema);
        
        // This is a simplified implementation - in production, you'd use proper JSON to Avro conversion
        // using libraries like avro-json-converter or custom mapping logic
        
        return record;
    }

    private GenericRecord parseAvroMessage(byte[] messageBytes, Schema schema) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(messageBytes);
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        return datumReader.read(null, decoder);
    }

    private GenericRecord transformRecord(GenericRecord sourceRecord, Schema sourceSchema, 
                                        Schema targetSchema, String correlationId) throws TransformationException {
        
        try {
            GenericRecord targetRecord = new GenericData.Record(targetSchema);

            // Apply field mappings based on schema compatibility
            for (Schema.Field targetField : targetSchema.getFields()) {
                String fieldName = targetField.name();
                
                // Check if source has this field
                Schema.Field sourceField = sourceSchema.getField(fieldName);
                if (sourceField != null) {
                    Object value = sourceRecord.get(fieldName);
                    if (value != null) {
                        targetRecord.put(fieldName, convertValue(value, sourceField.schema(), targetField.schema()));
                    }
                }
            }

            // Add processing metadata if target schema supports it
            addProcessingMetadata(targetRecord, targetSchema, correlationId);

            return targetRecord;

        } catch (Exception e) {
            throw new TransformationException("Failed to transform record structure", e);
        }
    }

    private Object convertValue(Object value, Schema sourceSchema, Schema targetSchema) {
        // Implement type conversion logic based on schema types
        // This is a simplified implementation
        
        if (sourceSchema.getType() == targetSchema.getType()) {
            return value;
        }
        
        // Add type conversion logic as needed
        return value;
    }

    private void addProcessingMetadata(GenericRecord record, Schema schema, String correlationId) {
        // Add correlation ID and processing timestamp if schema supports it
        Schema.Field correlationField = schema.getField("correlationId");
        if (correlationField != null) {
            record.put("correlationId", correlationId);
        }
        
        Schema.Field timestampField = schema.getField("processingTime");
        if (timestampField != null) {
            record.put("processingTime", System.currentTimeMillis());
        }
    }

    private String serializeRecord(GenericRecord record, Schema schema) throws TransformationException {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            
            datumWriter.write(record, encoder);
            encoder.flush();
            
            return outputStream.toString();
            
        } catch (IOException e) {
            throw new TransformationException("Failed to serialize Avro record", e);
        }
    }

    private boolean isAvroContract(String contractId) {
        Optional<ContractRegistry.ContractDefinition> contract = contractRegistry.getContract(contractId);
        return contract.isPresent() && contract.get().getType() == ContractRegistry.ContractType.AVRO;
    }
}