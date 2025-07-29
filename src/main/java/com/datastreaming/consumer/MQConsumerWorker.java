package com.datastreaming.consumer;

import com.datastreaming.config.ProcessorProfile;
import com.datastreaming.producer.HighThroughputKafkaProducer;
import com.datastreaming.transformer.MessageTransformer;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.mq.jms.MQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MQConsumerWorker {

    private static final Logger logger = LoggerFactory.getLogger(MQConsumerWorker.class);

    private final ProcessorProfile.MQConfiguration mqConfig;
    private final HighThroughputKafkaProducer kafkaProducer;
    private final MessageTransformer messageTransformer;
    private final MQConsumerPool pool;
    
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private final String workerId;

    public MQConsumerWorker(ProcessorProfile.MQConfiguration mqConfig,
                           HighThroughputKafkaProducer kafkaProducer,
                           MessageTransformer messageTransformer,
                           MQConsumerPool pool) {
        this.mqConfig = mqConfig;
        this.kafkaProducer = kafkaProducer;
        this.messageTransformer = messageTransformer;
        this.pool = pool;
        this.workerId = UUID.randomUUID().toString();
    }

    public void start() throws Exception {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("Worker already running: {}", workerId);
            return;
        }

        logger.info("Starting MQ consumer worker: {} for queue: {}", workerId, mqConfig.getQueueName());

        try {
            setupConnection();
            consumeMessages();
        } catch (Exception e) {
            logger.error("Error starting worker: {}", workerId, e);
            stop();
            throw e;
        }
    }

    private void setupConnection() throws JMSException {
        MQConnectionFactory factory = new MQConnectionFactory();
        factory.setHostName(mqConfig.getHostname());
        factory.setPort(mqConfig.getPort());
        factory.setChannel(mqConfig.getChannel());
        factory.setQueueManager(mqConfig.getQueueManagerName());
        factory.setTransportType(1); // MQC.MQJMS_TP_CLIENT_MQ_TCPIP

        connection = factory.createConnection(mqConfig.getUserId(), mqConfig.getPassword());
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MQQueue queue = new MQQueue(mqConfig.getQueueName());
        consumer = session.createConsumer(queue);

        logger.debug("Connection established for worker: {}", workerId);
    }

    private void consumeMessages() {
        while (isRunning.get()) {
            try {
                Message message = consumer.receive(5000); // 5 second timeout
                
                if (message != null) {
                    processMessage(message);
                }
                
            } catch (Exception e) {
                if (isRunning.get()) {
                    logger.error("Error consuming message in worker: {}", workerId, e);
                    pool.onError();
                    
                    // Brief pause before retrying
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        logger.info("Message consumption stopped for worker: {}", workerId);
    }

    private void processMessage(Message message) {
        String correlationId = null;
        
        try {
            correlationId = generateCorrelationId(message);
            
            // Transform message
            String transformedMessage = messageTransformer.transform(
                message, 
                mqConfig.getMessageContract(), 
                correlationId
            );
            
            // Send to Kafka
            kafkaProducer.sendAsync(
                mqConfig.getTargetKafkaTopic(),
                extractKey(message),
                transformedMessage,
                correlationId
            ).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    logger.error("Failed to send message to Kafka for correlationId: {}", correlationId, throwable);
                    pool.onError();
                } else {
                    logger.debug("Message successfully sent to Kafka topic: {} with correlationId: {}", 
                               mqConfig.getTargetKafkaTopic(), correlationId);
                }
            });
            
            messagesProcessed.incrementAndGet();
            pool.onMessageProcessed();
            
        } catch (Exception e) {
            logger.error("Error processing message with correlationId: {}", correlationId, e);
            pool.onError();
        }
    }

    private String generateCorrelationId(Message message) throws JMSException {
        String messageId = message.getJMSMessageID();
        if (messageId != null) {
            return messageId;
        }
        return workerId + "-" + System.currentTimeMillis() + "-" + messagesProcessed.get();
    }

    private String extractKey(Message message) throws JMSException {
        // Extract key from message properties or use a default strategy
        String key = message.getStringProperty("messageKey");
        if (key == null) {
            key = message.getJMSCorrelationID();
        }
        if (key == null) {
            key = String.valueOf(message.getJMSTimestamp());
        }
        return key;
    }

    public void stop() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping MQ consumer worker: {}", workerId);

        try {
            if (consumer != null) {
                consumer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            logger.error("Error stopping worker: {}", workerId, e);
        }

        logger.info("Worker stopped: {}", workerId);
    }

    public String getWorkerId() {
        return workerId;
    }

    public long getMessagesProcessed() {
        return messagesProcessed.get();
    }

    public boolean isRunning() {
        return isRunning.get();
    }
}