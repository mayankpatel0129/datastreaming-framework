package com.datastreaming.transformer;

import java.util.ArrayList;
import java.util.List;

public class ContractMapping {
    private String sourceContract;
    private String targetContract;
    private List<FieldMapping> fieldMappings = new ArrayList<>();

    public String getSourceContract() {
        return sourceContract;
    }

    public void setSourceContract(String sourceContract) {
        this.sourceContract = sourceContract;
    }

    public String getTargetContract() {
        return targetContract;
    }

    public void setTargetContract(String targetContract) {
        this.targetContract = targetContract;
    }

    public List<FieldMapping> getFieldMappings() {
        return fieldMappings;
    }

    public void setFieldMappings(List<FieldMapping> fieldMappings) {
        this.fieldMappings = fieldMappings;
    }

    public void addFieldMapping(FieldMapping fieldMapping) {
        this.fieldMappings.add(fieldMapping);
    }
}