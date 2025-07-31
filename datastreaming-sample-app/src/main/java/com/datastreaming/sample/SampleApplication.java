package com.datastreaming.sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Sample application using the Data Streaming Framework
 * 
 * This demonstrates how easy it is to use the framework:
 * 1. Add the framework starter dependency
 * 2. Configure via application.yml 
 * 3. Optionally provide custom transformers
 * 4. Run the application!
 */
@SpringBootApplication
public class SampleApplication implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(SampleApplication.class);

    @Autowired(required = false)
    private MQParsingDemo mqParsingDemo;
    
    @Autowired(required = false) 
    private JsonMQDemo jsonMQDemo;

    public static void main(String[] args) {
        SpringApplication.run(SampleApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("=== Data Streaming Framework Sample Application ===");
        
        // Check if demos should run based on profile or argument
        boolean runDemos = args.length == 0 || 
                          java.util.Arrays.asList(args).contains("--run-demos");
        
        if (runDemos) {
            logger.info("Running MQ parsing demonstrations...");
            
            // Run existing MQ parsing demos
            if (mqParsingDemo != null) {
                mqParsingDemo.runAllDemos();
            }
            
            // Run new JSON parsing demos
            if (jsonMQDemo != null) {
                jsonMQDemo.displayCapabilities();
                jsonMQDemo.runAllDemos();
            }
            
            logger.info("=== All demonstrations completed ===");
        } else {
            logger.info("Application started. Use --run-demos argument to see parsing demonstrations.");
        }
        
        logger.info("Data Streaming Framework Sample Application is ready!");
    }
}