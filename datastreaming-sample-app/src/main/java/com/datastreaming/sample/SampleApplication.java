package com.datastreaming.sample;

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
public class SampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(SampleApplication.class, args);
    }
}