package com.datastreaming.config;

import com.datastreaming.config.ProfileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class AsyncConfiguration implements AsyncConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(AsyncConfiguration.class);

    @Autowired
    private ProfileManager profileManager;

    @Override
    @Bean(name = "taskExecutor")
    public Executor getAsyncExecutor() {
        ProcessorProfile profile = profileManager.getActiveProfile();
        ProcessorProfile.PerformanceConfiguration perfConfig = profile.getPerformanceConfiguration();

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(perfConfig.getGlobalThreadPoolSize() / 2);
        executor.setMaxPoolSize(perfConfig.getGlobalThreadPoolSize());
        executor.setQueueCapacity(perfConfig.getQueueCapacity());
        executor.setKeepAliveSeconds((int) (perfConfig.getKeepAliveTimeMs() / 1000));
        executor.setThreadNamePrefix("async-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();

        logger.info("Async executor configured with {} core threads, {} max threads, {} queue capacity",
                   executor.getCorePoolSize(), executor.getMaxPoolSize(), executor.getQueueCapacity());

        return executor;
    }
}