package com.datastreaming.consumer;

import com.datastreaming.config.ProcessorProfile;
import com.datastreaming.config.ProfileManager;
import com.datastreaming.monitoring.MetricsCollector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class MQConsumerManager {

    private static final Logger logger = LoggerFactory.getLogger(MQConsumerManager.class);

    @Autowired
    private ProfileManager profileManager;

    @Autowired
    private MetricsCollector metricsCollector;

    @Autowired
    private MQConsumerFactory consumerFactory;

    private final ConcurrentHashMap<String, MQConsumerPool> consumerPools = new ConcurrentHashMap<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private ExecutorService managementExecutor;

    @PostConstruct
    public void initialize() {
        logger.info("Initializing MQ Consumer Manager");
        
        managementExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "mq-consumer-manager");
            t.setDaemon(true);
            return t;
        });

        startConsumers();
    }

    public void startConsumers() {
        if (isRunning.compareAndSet(false, true)) {
            ProcessorProfile profile = profileManager.getActiveProfile();
            
            for (ProcessorProfile.MQConfiguration mqConfig : profile.getMqConfigurations()) {
                startConsumerPool(mqConfig);
            }
            
            logger.info("Started {} consumer pools", consumerPools.size());
        }
    }

    private void startConsumerPool(ProcessorProfile.MQConfiguration mqConfig) {
        String poolKey = generatePoolKey(mqConfig);
        
        if (consumerPools.containsKey(poolKey)) {
            logger.warn("Consumer pool already exists for key: {}", poolKey);
            return;
        }

        try {
            MQConsumerPool pool = new MQConsumerPool(mqConfig, consumerFactory, metricsCollector);
            consumerPools.put(poolKey, pool);
            
            managementExecutor.submit(() -> {
                try {
                    pool.start();
                    logger.info("Started consumer pool for queue: {} -> topic: {}", 
                               mqConfig.getQueueName(), mqConfig.getTargetKafkaTopic());
                } catch (Exception e) {
                    logger.error("Failed to start consumer pool for queue: {}", mqConfig.getQueueName(), e);
                    consumerPools.remove(poolKey);
                }
            });
            
        } catch (Exception e) {
            logger.error("Failed to create consumer pool for queue: {}", mqConfig.getQueueName(), e);
        }
    }

    public void stopConsumers() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping all consumer pools");
            
            List<CompletableFuture<Void>> shutdownFutures = consumerPools.values().stream()
                    .map(pool -> CompletableFuture.runAsync(() -> {
                        try {
                            pool.shutdown();
                        } catch (Exception e) {
                            logger.error("Error shutting down consumer pool", e);
                        }
                    }, managementExecutor))
                    .toList();

            try {
                CompletableFuture.allOf(shutdownFutures.toArray(new CompletableFuture[0]))
                        .get(30, TimeUnit.SECONDS);
                logger.info("All consumer pools stopped successfully");
            } catch (Exception e) {
                logger.error("Timeout or error while stopping consumer pools", e);
            } finally {
                consumerPools.clear();
            }
        }
    }

    public void restartConsumerPool(String queueName) {
        String poolKey = consumerPools.keySet().stream()
                .filter(key -> key.contains(queueName))
                .findFirst()
                .orElse(null);

        if (poolKey != null) {
            MQConsumerPool pool = consumerPools.remove(poolKey);
            if (pool != null) {
                managementExecutor.submit(() -> {
                    try {
                        pool.shutdown();
                        Thread.sleep(5000); // Wait before restart
                        
                        ProcessorProfile profile = profileManager.getActiveProfile();
                        ProcessorProfile.MQConfiguration mqConfig = profile.getMqConfigurations().stream()
                                .filter(config -> config.getQueueName().equals(queueName))
                                .findFirst()
                                .orElse(null);
                        
                        if (mqConfig != null) {
                            startConsumerPool(mqConfig);
                        }
                    } catch (Exception e) {
                        logger.error("Failed to restart consumer pool for queue: {}", queueName, e);
                    }
                });
            }
        }
    }

    public MQConsumerStats getConsumerStats() {
        MQConsumerStats stats = new MQConsumerStats();
        
        for (MQConsumerPool pool : consumerPools.values()) {
            MQConsumerStats poolStats = pool.getStats();
            stats.addPoolStats(poolStats);
        }
        
        return stats;
    }

    public boolean isHealthy() {
        return isRunning.get() && consumerPools.values().stream()
                .allMatch(MQConsumerPool::isHealthy);
    }

    private String generatePoolKey(ProcessorProfile.MQConfiguration mqConfig) {
        return String.format("%s:%s:%s", 
                           mqConfig.getQueueManagerName(), 
                           mqConfig.getQueueName(), 
                           mqConfig.getTargetKafkaTopic());
    }

    @PreDestroy
    public void destroy() {
        logger.info("Destroying MQ Consumer Manager");
        stopConsumers();
        
        if (managementExecutor != null && !managementExecutor.isShutdown()) {
            managementExecutor.shutdown();
            try {
                if (!managementExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    managementExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                managementExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}