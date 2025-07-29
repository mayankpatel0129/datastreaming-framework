package com.datastreaming.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class ProfileManager {

    private static final Logger logger = LoggerFactory.getLogger(ProfileManager.class);
    
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<String, ProcessorProfile> profileCache;
    private final ReadWriteLock cacheLock;
    
    @Value("${processor.profile.name:default}")
    private String activeProfileName;
    
    @Value("${processor.profile.directory:src/main/resources/profiles}")
    private String profileDirectory;
    
    public ProfileManager() {
        this.objectMapper = new ObjectMapper();
        this.profileCache = new ConcurrentHashMap<>();
        this.cacheLock = new ReentrantReadWriteLock();
    }
    
    @PostConstruct
    public void initialize() {
        loadProfiles();
        validateActiveProfile();
    }
    
    public ProcessorProfile getActiveProfile() {
        cacheLock.readLock().lock();
        try {
            ProcessorProfile profile = profileCache.get(activeProfileName);
            if (profile == null) {
                throw new IllegalStateException("Active profile '" + activeProfileName + "' not found");
            }
            return profile;
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    public ProcessorProfile getProfile(String profileName) {
        cacheLock.readLock().lock();
        try {
            return profileCache.get(profileName);
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    public void reloadProfiles() {
        cacheLock.writeLock().lock();
        try {
            profileCache.clear();
            loadProfiles();
            validateActiveProfile();
            logger.info("Profiles reloaded successfully");
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    private void loadProfiles() {
        try {
            Path profilePath = Paths.get(profileDirectory);
            if (!Files.exists(profilePath)) {
                logger.warn("Profile directory does not exist: {}", profileDirectory);
                createDefaultProfile();
                return;
            }
            
            Files.walk(profilePath)
                    .filter(path -> path.toString().endsWith(".json"))
                    .forEach(this::loadProfileFromFile);
                    
            logger.info("Loaded {} profiles from directory: {}", profileCache.size(), profileDirectory);
            
        } catch (IOException e) {
            logger.error("Error loading profiles from directory: {}", profileDirectory, e);
            createDefaultProfile();
        }
    }
    
    private void loadProfileFromFile(Path filePath) {
        try {
            String content = Files.readString(filePath);
            ProcessorProfile profile = objectMapper.readValue(content, ProcessorProfile.class);
            
            validateProfile(profile);
            
            profileCache.put(profile.getProfileName(), profile);
            logger.debug("Loaded profile: {} from file: {}", profile.getProfileName(), filePath);
            
        } catch (Exception e) {
            logger.error("Error loading profile from file: {}", filePath, e);
        }
    }
    
    private void validateProfile(ProcessorProfile profile) {
        if (profile.getProfileName() == null || profile.getProfileName().trim().isEmpty()) {
            throw new IllegalArgumentException("Profile name cannot be null or empty");
        }
        
        if (profile.getMqConfigurations() == null || profile.getMqConfigurations().isEmpty()) {
            throw new IllegalArgumentException("Profile must have at least one MQ configuration");
        }
        
        if (profile.getKafkaConfiguration() == null) {
            throw new IllegalArgumentException("Profile must have Kafka configuration");
        }
        
        // Validate MQ configurations
        for (ProcessorProfile.MQConfiguration mqConfig : profile.getMqConfigurations()) {
            if (mqConfig.getQueueName() == null || mqConfig.getTargetKafkaTopic() == null) {
                throw new IllegalArgumentException("MQ configuration must have queue name and target Kafka topic");
            }
        }
    }
    
    private void validateActiveProfile() {
        if (!profileCache.containsKey(activeProfileName)) {
            logger.error("Active profile '{}' not found. Available profiles: {}", 
                        activeProfileName, profileCache.keySet());
            throw new IllegalStateException("Active profile '" + activeProfileName + "' not found");
        }
        
        logger.info("Active profile set to: {}", activeProfileName);
    }
    
    private void createDefaultProfile() {
        logger.info("Creating default profile");
        
        ProcessorProfile defaultProfile = new ProcessorProfile();
        defaultProfile.setProfileName("default");
        
        // Set default configurations - this would be populated from actual config
        // This is just a placeholder to prevent startup failures
        
        profileCache.put("default", defaultProfile);
    }
    
    public String getActiveProfileName() {
        return activeProfileName;
    }
    
    public void setActiveProfile(String profileName) {
        cacheLock.writeLock().lock();
        try {
            if (!profileCache.containsKey(profileName)) {
                throw new IllegalArgumentException("Profile '" + profileName + "' not found");
            }
            
            this.activeProfileName = profileName;
            logger.info("Active profile changed to: {}", profileName);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
}