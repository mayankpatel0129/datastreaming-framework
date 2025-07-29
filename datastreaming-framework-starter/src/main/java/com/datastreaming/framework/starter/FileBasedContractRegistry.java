package com.datastreaming.framework.starter;

import com.datastreaming.framework.core.api.ContractRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * File-based contract registry implementation
 */
@Component
public class FileBasedContractRegistry implements ContractRegistry {

    private static final Logger logger = LoggerFactory.getLogger(FileBasedContractRegistry.class);

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private DataStreamingProperties properties;

    private final ConcurrentHashMap<String, ContractDefinition> contracts = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void initialize() {
        loadContracts();
    }

    @Override
    public void registerContract(String contractId, String contractDefinition, ContractType contractType) {
        contracts.put(contractId, new ContractDefinition(contractId, contractDefinition, contractType, System.currentTimeMillis()));
        logger.info("Registered contract: {} of type: {}", contractId, contractType);
    }

    @Override
    public Optional<ContractDefinition> getContract(String contractId) {
        return Optional.ofNullable(contracts.get(contractId));
    }

    @Override
    public boolean hasContract(String contractId) {
        return contracts.containsKey(contractId);
    }

    @Override
    public void removeContract(String contractId) {
        contracts.remove(contractId);
        logger.info("Removed contract: {}", contractId);
    }

    @Override
    public void reloadContracts() {
        contracts.clear();
        loadContracts();
        logger.info("Reloaded all contracts");
    }

    private void loadContracts() {
        try {
            String location = properties.getContracts().getLocation();
            logger.info("Loading contracts from: {}", location);

            if (resourceLoader instanceof ResourcePatternResolver) {
                ResourcePatternResolver resolver = (ResourcePatternResolver) resourceLoader;
                
                // Load Avro schemas
                Resource[] avroResources = resolver.getResources(location + "**/*.avsc");
                for (Resource resource : avroResources) {
                    loadAvroContract(resource);
                }

                // Load JSON schemas
                Resource[] jsonResources = resolver.getResources(location + "**/*.json");
                for (Resource resource : jsonResources) {
                    loadJsonContract(resource);
                }
            }

            logger.info("Loaded {} contracts", contracts.size());

        } catch (IOException e) {
            logger.error("Error loading contracts", e);
        }
    }

    private void loadAvroContract(Resource resource) {
        try {
            String contractId = extractContractId(resource.getFilename());
            String content = resource.getContentAsString(StandardCharsets.UTF_8);
            
            registerContract(contractId, content, ContractType.AVRO);
            logger.debug("Loaded Avro contract: {}", contractId);
            
        } catch (IOException e) {
            logger.error("Error loading Avro contract from: {}", resource.getFilename(), e);
        }
    }

    private void loadJsonContract(Resource resource) {
        try {
            String contractId = extractContractId(resource.getFilename());
            String content = resource.getContentAsString(StandardCharsets.UTF_8);
            
            // Determine contract type from content or filename
            ContractType type = determineContractType(content);
            
            registerContract(contractId, content, type);
            logger.debug("Loaded JSON contract: {} as type: {}", contractId, type);
            
        } catch (IOException e) {
            logger.error("Error loading JSON contract from: {}", resource.getFilename(), e);
        }
    }

    private String extractContractId(String filename) {
        if (filename == null) {
            return "unknown";
        }
        
        // Remove extension
        int lastDot = filename.lastIndexOf('.');
        if (lastDot > 0) {
            return filename.substring(0, lastDot);
        }
        
        return filename;
    }

    private ContractType determineContractType(String content) {
        try {
            // Try to parse as JSON to determine if it's a schema definition
            objectMapper.readTree(content);
            
            if (content.contains("\"type\"") && content.contains("\"record\"")) {
                return ContractType.AVRO;
            } else if (content.contains("\"$schema\"")) {
                return ContractType.JSON_SCHEMA;
            } else {
                return ContractType.CUSTOM;
            }
            
        } catch (Exception e) {
            return ContractType.CUSTOM;
        }
    }
}