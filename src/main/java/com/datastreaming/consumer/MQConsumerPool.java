package com.datastreaming.consumer;

import com.datastreaming.config.ProcessorProfile;
import com.datastreaming.monitoring.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MQConsumerPool {

    private static final Logger logger = LoggerFactory.getLogger(MQConsumerPool.class);

    private final ProcessorProfile.MQConfiguration mqConfig;
    private final MQConsumerFactory consumerFactory;
    private final MetricsCollector metricsCollector;
    
    private final List<MQConsumerWorker> workers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    
    private ExecutorService workerExecutor;
    private ScheduledExecutorService healthCheckExecutor;

    public MQConsumerPool(ProcessorProfile.MQConfiguration mqConfig, 
                         MQConsumerFactory consumerFactory,
                         MetricsCollector metricsCollector) {
        this.mqConfig = mqConfig;
        this.consumerFactory = consumerFactory;
        this.metricsCollector = metricsCollector;
    }

    public void start() throws Exception {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("Consumer pool already running for queue: {}", mqConfig.getQueueName());
            return;
        }

        logger.info("Starting consumer pool for queue: {} with {} threads", 
                   mqConfig.getQueueName(), mqConfig.getConsumerThreads());

        // Create thread pool with custom thread factory
        workerExecutor = new ThreadPoolExecutor(
                mqConfig.getConsumerThreads(),
                mqConfig.getConsumerThreads(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(mqConfig.getMaxBatchSize() * 2),
                r -> {
                    Thread t = new Thread(r, "mq-consumer-" + mqConfig.getQueueName() + "-" + 
                                         workers.size());
                    t.setDaemon(false);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // Create health check scheduler
        healthCheckExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "health-check-" + mqConfig.getQueueName());
            t.setDaemon(true);
            return t;
        });

        // Start worker threads
        for (int i = 0; i < mqConfig.getConsumerThreads(); i++) {
            MQConsumerWorker worker = consumerFactory.createWorker(mqConfig, this);
            workers.add(worker);
            
            workerExecutor.submit(() -> {
                try {
                    worker.start();
                    activeWorkers.incrementAndGet();
                } catch (Exception e) {
                    logger.error("Error starting worker for queue: {}", mqConfig.getQueueName(), e);
                    totalErrors.incrementAndGet();
                } finally {
                    activeWorkers.decrementAndGet();
                }
            });
        }

        // Start health monitoring
        healthCheckExecutor.scheduleAtFixedRate(this::performHealthCheck, 30, 30, TimeUnit.SECONDS);
        
        logger.info("Started {} workers for queue: {}", workers.size(), mqConfig.getQueueName());
    }

    public void shutdown() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }

        logger.info("Shutting down consumer pool for queue: {}", mqConfig.getQueueName());

        // Stop health check
        if (healthCheckExecutor != null) {
            healthCheckExecutor.shutdown();
        }

        // Gracefully stop workers
        workers.parallelStream().forEach(worker -> {
            try {
                worker.stop();
            } catch (Exception e) {
                logger.error("Error stopping worker", e);
            }
        });

        // Shutdown executor
        if (workerExecutor != null) {
            workerExecutor.shutdown();
            try {
                if (!workerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    workerExecutor.shutdownNow();
                    if (!workerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                        logger.warn("Worker executor did not terminate gracefully");
                    }
                }
            } catch (InterruptedException e) {
                workerExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        workers.clear();
        logger.info("Consumer pool shutdown complete for queue: {}", mqConfig.getQueueName());
    }

    private void performHealthCheck() {
        try {
            int activeCount = activeWorkers.get();
            int expectedCount = mqConfig.getConsumerThreads();
            
            if (activeCount < expectedCount) {
                logger.warn("Health check failed for queue: {}. Active workers: {}, Expected: {}", 
                           mqConfig.getQueueName(), activeCount, expectedCount);
                
                metricsCollector.recordWorkerHealthCheck(mqConfig.getQueueName(), activeCount, expectedCount);
                
                // Attempt to restart failed workers
                restartFailedWorkers();
            }
            
        } catch (Exception e) {
            logger.error("Error during health check for queue: {}", mqConfig.getQueueName(), e);
        }
    }

    private void restartFailedWorkers() {
        int currentWorkers = workers.size();
        int expectedWorkers = mqConfig.getConsumerThreads();
        
        if (currentWorkers < expectedWorkers) {
            for (int i = currentWorkers; i < expectedWorkers; i++) {
                try {
                    MQConsumerWorker worker = consumerFactory.createWorker(mqConfig, this);
                    workers.add(worker);
                    
                    workerExecutor.submit(() -> {
                        try {
                            worker.start();
                            activeWorkers.incrementAndGet();
                            logger.info("Restarted worker for queue: {}", mqConfig.getQueueName());
                        } catch (Exception e) {
                            logger.error("Failed to restart worker for queue: {}", mqConfig.getQueueName(), e);
                            workers.remove(worker);
                        }
                    });
                    
                } catch (Exception e) {
                    logger.error("Failed to create replacement worker for queue: {}", mqConfig.getQueueName(), e);
                }
            }
        }
    }

    public void onMessageProcessed() {
        totalMessagesProcessed.incrementAndGet();
        metricsCollector.recordMessageProcessed(mqConfig.getQueueName());
    }

    public void onError() {
        totalErrors.incrementAndGet();
        metricsCollector.recordError(mqConfig.getQueueName());
    }

    public MQConsumerStats getStats() {
        return new MQConsumerStats(
                mqConfig.getQueueName(),
                mqConfig.getTargetKafkaTopic(),
                workers.size(),
                activeWorkers.get(),
                totalMessagesProcessed.get(),
                totalErrors.get(),
                isRunning.get()
        );
    }

    public boolean isHealthy() {
        return isRunning.get() && 
               activeWorkers.get() >= (mqConfig.getConsumerThreads() * 0.8); // Allow 20% tolerance
    }

    public ProcessorProfile.MQConfiguration getMqConfig() {
        return mqConfig;
    }
}