package com.github.pruthwee.kafka.learn1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProduceDemoCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProduceDemoCallback.class);
        String bootStrapServer = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 20; i++) {

            //Create a Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "HelloWorld!!" + Integer.toString(i));

            //send Data (Async call)
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null == exception) {
                        logger.info("Received new Metadata. \n  Topic: {} \n Partition: {} \n Offset: {} \n TimeStamp: {}\n",
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    } else {
                        logger.warn("Got Exception while producing message to Topic 'first_topic', Exception: {}", exception.getMessage());
                    }
                }
            });
        }

        //flush the producer
        kafkaProducer.flush();

        //flush and close
        kafkaProducer.close();

    }
}
