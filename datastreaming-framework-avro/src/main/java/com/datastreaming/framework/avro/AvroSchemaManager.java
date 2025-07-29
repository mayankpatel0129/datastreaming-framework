package com.datastreaming.framework.avro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Avro schemas and integrates with Confluent Schema Registry
 */
@Component
public class AvroSchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(AvroSchemaManager.class);

    @Value("${datastreaming.kafka.schema-registry-url:}")
    private String schemaRegistryUrl;

    @Value("${datastreaming.kafka.schema-registry-credentials:}")
    private String schemaRegistryCredentials;

    private SchemaRegistryClient schemaRegistryClient;
    private final ConcurrentHashMap<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> schemaIdCache = new ConcurrentHashMap<>();

    @PostConstruct
    public void initialize() {
        if (schemaRegistryUrl != null && !schemaRegistryUrl.isEmpty()) {
            initializeSchemaRegistry();
            logger.info("Avro Schema Manager initialized with Schema Registry: {}", schemaRegistryUrl);
        } else {
            logger.info("Avro Schema Manager initialized without Schema Registry");
        }
    }

    private void initializeSchemaRegistry() {
        try {
            schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
            
            // Test connectivity
            schemaRegistryClient.getAllSubjects();
            logger.info("Successfully connected to Schema Registry at: {}", schemaRegistryUrl);
            
        } catch (Exception e) {
            logger.error("Failed to connect to Schema Registry at: {}", schemaRegistryUrl, e);
            // Continue without Schema Registry
            schemaRegistryClient = null;
        }
    }

    /**
     * Register a schema with the Schema Registry
     */
    public int registerSchema(String subject, Schema schema) throws IOException, RestClientException {
        if (schemaRegistryClient == null) {
            throw new IllegalStateException("Schema Registry client not initialized");
        }

        String schemaString = schema.toString();
        int schemaId = schemaRegistryClient.register(subject, 
            new io.confluent.kafka.schemaregistry.avro.AvroSchema(schemaString));
        
        // Cache the schema
        schemaCache.put(subject, schema);
        schemaIdCache.put(subject, schemaId);
        
        logger.info("Registered schema for subject: {} with ID: {}", subject, schemaId);
        return schemaId;
    }

    /**
     * Get schema by subject name
     */
    public Schema getSchema(String subject) throws IOException, RestClientException {
        // Check cache first
        Schema cachedSchema = schemaCache.get(subject);
        if (cachedSchema != null) {
            return cachedSchema;
        }

        if (schemaRegistryClient == null) {
            throw new IllegalStateException("Schema Registry client not initialized");
        }

        // Fetch from Schema Registry - simplified approach
        try {
            io.confluent.kafka.schemaregistry.client.SchemaMetadata metadata = 
                schemaRegistryClient.getLatestSchemaMetadata(subject);
            
            Schema schema = new Schema.Parser().parse(metadata.getSchema());
            
            // Cache the schema
            schemaCache.put(subject, schema);
            schemaIdCache.put(subject, metadata.getId());
            
            return schema;
        } catch (Exception e) {
            throw new IOException("Failed to fetch schema from registry for subject: " + subject, e);
        }
    }

    /**
     * Get schema by ID
     */
    public Schema getSchemaById(int schemaId) throws IOException, RestClientException {
        if (schemaRegistryClient == null) {
            throw new IllegalStateException("Schema Registry client not initialized");
        }

        // Get schema by ID and parse it
        try {
            io.confluent.kafka.schemaregistry.ParsedSchema parsedSchema = 
                schemaRegistryClient.getSchemaById(schemaId);
            
            return new Schema.Parser().parse(parsedSchema.canonicalString());
        } catch (Exception e) {
            throw new IOException("Failed to fetch schema by ID: " + schemaId, e);
        }
    }

    /**
     * Check if schema registry is available
     */
    public boolean isSchemaRegistryAvailable() {
        return schemaRegistryClient != null;
    }

    /**
     * Get all registered subjects
     */
    public java.util.Collection<String> getAllSubjects() throws IOException, RestClientException {
        if (schemaRegistryClient == null) {
            throw new IllegalStateException("Schema Registry client not initialized");
        }
        
        return schemaRegistryClient.getAllSubjects();
    }

    /**
     * Parse schema from string
     */
    public Schema parseSchema(String schemaString) {
        try {
            return new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
            logger.error("Failed to parse Avro schema", e);
            throw new IllegalArgumentException("Invalid Avro schema: " + e.getMessage(), e);
        }
    }

    /**
     * Validate schema compatibility
     */
    public boolean isCompatible(Schema readerSchema, Schema writerSchema) {
        try {
            // Use Avro's schema resolution to check compatibility
            org.apache.avro.io.ResolvingDecoder.resolve(writerSchema, readerSchema);
            return true;
        } catch (Exception e) {
            logger.debug("Schema compatibility check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Clear schema cache
     */
    public void clearCache() {
        schemaCache.clear();
        schemaIdCache.clear();
        logger.info("Schema cache cleared");
    }

    /**
     * Get cached schema count
     */
    public int getCachedSchemaCount() {
        return schemaCache.size();
    }
}