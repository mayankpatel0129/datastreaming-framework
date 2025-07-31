package com.datastreaming.framework.core.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for managing externalized MQ contracts
 */
public class MQContractRegistry {
    
    private static final Logger logger = LoggerFactory.getLogger(MQContractRegistry.class);
    
    private final Map<String, MQContract> contracts = new ConcurrentHashMap<>();
    private final ObjectMapper jsonMapper = new ObjectMapper();
    
    @Value("${datastreaming.mq.contracts.location:classpath*:mq/contracts/**/*.json}")
    private String contractsLocation;
    
    @Value("${datastreaming.mq.contracts.reload-on-change:false}")
    private boolean reloadOnChange;
    
    @PostConstruct
    public void initialize() {
        logger.info("Initializing MQ Contract Registry from location: {}", contractsLocation);
        loadContracts();
        logger.info("Loaded {} MQ contracts", contracts.size());
    }
    
    /**
     * Get contract by name (latest version)
     */
    public MQContract getContract(String name) {
        return getContract(name, null);
    }
    
    /**
     * Get contract by name and version
     */
    public MQContract getContract(String name, String version) {
        if (name == null) {
            return null;
        }
        
        String contractKey;
        if (version != null) {
            contractKey = name + "-" + version;
        } else {
            // Find latest version for this contract name
            contractKey = findLatestVersion(name);
        }
        
        MQContract contract = contracts.get(contractKey);
        
        if (contract == null) {
            logger.warn("Contract not found: {} (version: {})", name, version);
        } else {
            logger.debug("Retrieved contract: {} (version: {})", contract.getName(), contract.getVersion());
        }
        
        return contract;
    }
    
    /**
     * Register a contract programmatically
     */
    public void registerContract(MQContract contract) {
        if (contract == null) {
            throw new IllegalArgumentException("Contract cannot be null");
        }
        
        String contractKey = contract.getContractId();
        contracts.put(contractKey, contract);
        
        logger.info("Registered contract: {} (version: {})", contract.getName(), contract.getVersion());
    }
    
    /**
     * Remove a contract
     */
    public void removeContract(String name, String version) {
        String contractKey = name + "-" + version;
        MQContract removed = contracts.remove(contractKey);
        
        if (removed != null) {
            logger.info("Removed contract: {} (version: {})", name, version);
        } else {
            logger.warn("Contract not found for removal: {} (version: {})", name, version);
        }
    }
    
    /**
     * Get all registered contracts
     */
    public Map<String, MQContract> getAllContracts() {
        return Map.copyOf(contracts);
    }
    
    /**
     * Reload contracts from configured location
     */
    public void reloadContracts() {
        logger.info("Reloading MQ contracts from location: {}", contractsLocation);
        contracts.clear();
        loadContracts();
        logger.info("Reloaded {} MQ contracts", contracts.size());
    }
    
    /**
     * Get contract registry statistics
     */
    public MQContractRegistryStats getStats() {
        Map<MQMessageFormat, Long> formatCounts = contracts.values().stream()
            .collect(java.util.stream.Collectors.groupingBy(
                MQContract::getFormat,
                java.util.stream.Collectors.counting()
            ));
            
        return new MQContractRegistryStats(contracts.size(), formatCounts);
    }
    
    private void loadContracts() {
        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources(contractsLocation);
            
            logger.debug("Found {} contract files to load", resources.length);
            
            for (Resource resource : resources) {
                if (resource.exists() && resource.isReadable()) {
                    loadContractFromResource(resource);
                }
            }
            
        } catch (IOException e) {
            logger.error("Failed to load MQ contracts from location: {}", contractsLocation, e);
            throw new RuntimeException("Failed to load MQ contracts", e);
        }
    }
    
    private void loadContractFromResource(Resource resource) {
        try {
            logger.debug("Loading contract from: {}", resource.getFilename());
            
            ObjectMapper mapper = getMapperForResource(resource);
            MQContract contract = mapper.readValue(resource.getInputStream(), MQContract.class);
            
            // Validate contract
            validateContract(contract);
            
            String contractKey = contract.getContractId();
            contracts.put(contractKey, contract);
            
            logger.info("Loaded contract: {} (version: {}) from {}", 
                       contract.getName(), contract.getVersion(), resource.getFilename());
                       
        } catch (Exception e) {
            logger.error("Failed to load contract from resource: {}", resource.getFilename(), e);
            // Continue loading other contracts instead of failing completely
        }
    }
    
    private ObjectMapper getMapperForResource(Resource resource) {
        // For now, only support JSON until we add YAML dependency
        return jsonMapper;
    }
    
    private void validateContract(MQContract contract) {
        if (contract.getName() == null || contract.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Contract name cannot be null or empty");
        }
        
        if (contract.getVersion() == null || contract.getVersion().trim().isEmpty()) {
            throw new IllegalArgumentException("Contract version cannot be null or empty");
        }
        
        if (contract.getFormat() == null) {
            throw new IllegalArgumentException("Contract format cannot be null");
        }
        
        if (contract.getFields() == null || contract.getFields().isEmpty()) {
            throw new IllegalArgumentException("Contract must have at least one field");
        }
        
        // Validate field positions for fixed-length format
        if (contract.getFormat() == MQMessageFormat.FIXED_LENGTH) {
            validateFixedLengthFields(contract);
        }
    }
    
    private void validateFixedLengthFields(MQContract contract) {
        for (int i = 0; i < contract.getFields().size(); i++) {
            MQField field = contract.getFields().get(i);
            
            if (field.getPosition() < 0) {
                throw new IllegalArgumentException(
                    "Field '" + field.getName() + "' has invalid position: " + field.getPosition()
                );
            }
            
            if (field.getLength() <= 0) {
                throw new IllegalArgumentException(
                    "Field '" + field.getName() + "' has invalid length: " + field.getLength()
                );
            }
            
            // Check for overlapping fields
            for (int j = i + 1; j < contract.getFields().size(); j++) {
                MQField otherField = contract.getFields().get(j);
                
                if (fieldsOverlap(field, otherField)) {
                    throw new IllegalArgumentException(
                        "Fields '" + field.getName() + "' and '" + otherField.getName() + "' overlap"
                    );
                }
            }
        }
    }
    
    private boolean fieldsOverlap(MQField field1, MQField field2) {
        int field1Start = field1.getPosition();
        int field1End = field1.getEndPosition();
        int field2Start = field2.getPosition();
        int field2End = field2.getEndPosition();
        
        return !(field1End <= field2Start || field2End <= field1Start);
    }
    
    private String findLatestVersion(String name) {
        return contracts.keySet().stream()
            .filter(key -> key.startsWith(name + "-"))
            .max(String::compareTo) // Simple string comparison for version ordering
            .orElse(name); // Fallback to just the name
    }
    
    /**
     * Statistics class for contract registry
     */
    public static class MQContractRegistryStats {
        private final int totalContracts;
        private final Map<MQMessageFormat, Long> contractsByFormat;
        
        public MQContractRegistryStats(int totalContracts, Map<MQMessageFormat, Long> contractsByFormat) {
            this.totalContracts = totalContracts;
            this.contractsByFormat = contractsByFormat;
        }
        
        public int getTotalContracts() { return totalContracts; }
        public Map<MQMessageFormat, Long> getContractsByFormat() { return contractsByFormat; }
        
        @Override
        public String toString() {
            return "MQContractRegistryStats{" +
                    "totalContracts=" + totalContracts +
                    ", contractsByFormat=" + contractsByFormat +
                    '}';
        }
    }
}