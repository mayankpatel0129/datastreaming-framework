package com.datastreaming.consumer;

import com.datastreaming.config.ProcessorProfile;
import com.datastreaming.producer.HighThroughputKafkaProducer;
import com.datastreaming.transformer.MessageTransformer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MQConsumerFactory {

    @Autowired
    private HighThroughputKafkaProducer kafkaProducer;

    @Autowired
    private MessageTransformer messageTransformer;

    public MQConsumerWorker createWorker(ProcessorProfile.MQConfiguration mqConfig, MQConsumerPool pool) {
        return new MQConsumerWorker(mqConfig, kafkaProducer, messageTransformer, pool);
    }
}