package com.datastreaming.framework.core.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ContractRegistryTest {

    @Test
    void shouldCreateContractDefinition() {
        // Given
        String contractId = "test-contract";
        String definition = "{ \"type\": \"record\" }";
        ContractRegistry.ContractType type = ContractRegistry.ContractType.AVRO;
        long version = 1L;

        // When
        ContractRegistry.ContractDefinition contract = new ContractRegistry.ContractDefinition(
            contractId, definition, type, version
        );

        // Then
        assertEquals(contractId, contract.getContractId());
        assertEquals(definition, contract.getDefinition());
        assertEquals(type, contract.getType());
        assertEquals(version, contract.getVersion());
    }

    @Test
    void shouldTestContractTypeEnumValues() {
        // When & Then
        assertEquals("avro", ContractRegistry.ContractType.AVRO.getIdentifier());
        assertEquals("json", ContractRegistry.ContractType.JSON_SCHEMA.getIdentifier());
        assertEquals("xml", ContractRegistry.ContractType.XML_SCHEMA.getIdentifier());
        assertEquals("protobuf", ContractRegistry.ContractType.PROTOBUF.getIdentifier());
        assertEquals("custom", ContractRegistry.ContractType.CUSTOM.getIdentifier());
    }

    @Test
    void shouldCreateContractTypeFromIdentifier() {
        // When & Then
        assertEquals(ContractRegistry.ContractType.AVRO, 
                    ContractRegistry.ContractType.fromIdentifier("avro"));
        assertEquals(ContractRegistry.ContractType.JSON_SCHEMA, 
                    ContractRegistry.ContractType.fromIdentifier("json"));
        assertEquals(ContractRegistry.ContractType.XML_SCHEMA, 
                    ContractRegistry.ContractType.fromIdentifier("xml"));
        assertEquals(ContractRegistry.ContractType.PROTOBUF, 
                    ContractRegistry.ContractType.fromIdentifier("protobuf"));
        assertEquals(ContractRegistry.ContractType.CUSTOM, 
                    ContractRegistry.ContractType.fromIdentifier("unknown"));
    }

    @Test
    void shouldReturnCustomForUnknownIdentifier() {
        // When
        ContractRegistry.ContractType type = ContractRegistry.ContractType.fromIdentifier("unknown-type");

        // Then
        assertEquals(ContractRegistry.ContractType.CUSTOM, type);
    }
}