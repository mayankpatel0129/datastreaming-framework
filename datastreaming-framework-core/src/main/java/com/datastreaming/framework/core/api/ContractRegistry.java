package com.datastreaming.framework.core.api;

import java.util.Optional;

/**
 * SPI interface for contract registry
 * Implement this interface to provide custom contract management
 */
public interface ContractRegistry {

    /**
     * Register a message contract
     * @param contractId Contract identifier
     * @param contractDefinition Contract definition (JSON Schema, Avro Schema, etc.)
     * @param contractType Type of contract (avro, json, xml, etc.)
     */
    void registerContract(String contractId, String contractDefinition, ContractType contractType);

    /**
     * Get contract definition by ID
     * @param contractId Contract identifier
     * @return Optional contract definition
     */
    Optional<ContractDefinition> getContract(String contractId);

    /**
     * Check if contract exists
     * @param contractId Contract identifier
     * @return true if exists, false otherwise
     */
    boolean hasContract(String contractId);

    /**
     * Remove contract from registry
     * @param contractId Contract identifier
     */
    void removeContract(String contractId);

    /**
     * Reload contracts from external source
     */
    void reloadContracts();

    /**
     * Contract definition holder
     */
    class ContractDefinition {
        private final String contractId;
        private final String definition;
        private final ContractType type;
        private final long version;

        public ContractDefinition(String contractId, String definition, ContractType type, long version) {
            this.contractId = contractId;
            this.definition = definition;
            this.type = type;
            this.version = version;
        }

        public String getContractId() { return contractId; }
        public String getDefinition() { return definition; }
        public ContractType getType() { return type; }
        public long getVersion() { return version; }
    }

    /**
     * Supported contract types
     */
    enum ContractType {
        AVRO("avro"),
        JSON_SCHEMA("json"),
        XML_SCHEMA("xml"),
        PROTOBUF("protobuf"),
        CUSTOM("custom");

        private final String identifier;

        ContractType(String identifier) {
            this.identifier = identifier;
        }

        public String getIdentifier() { return identifier; }

        public static ContractType fromIdentifier(String identifier) {
            for (ContractType type : values()) {
                if (type.identifier.equals(identifier)) {
                    return type;
                }
            }
            return CUSTOM;
        }
    }
}