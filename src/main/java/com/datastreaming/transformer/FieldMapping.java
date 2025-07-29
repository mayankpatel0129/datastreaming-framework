package com.datastreaming.transformer;

public class FieldMapping {
    private String sourcePath;
    private String targetPath;
    private String transformationType;
    private boolean required;

    public FieldMapping() {}

    public FieldMapping(String sourcePath, String targetPath, String transformationType, boolean required) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.transformationType = transformationType;
        this.required = required;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    public String getTransformationType() {
        return transformationType;
    }

    public void setTransformationType(String transformationType) {
        this.transformationType = transformationType;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
}