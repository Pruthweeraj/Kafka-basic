package com.github.pruthwee.kafka.learn1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProduceDemoCallbackWithKey {

    private static String value;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProduceDemoCallbackWithKey.class);
        String bootStrapServer = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 70; i++) {
            String key;
            if (i < 10) {
                key = "1";
            } else if (i < 20) {
                key = "2";
            } else if (i < 30) {
                key = "3";
            } else if (i < 40) {
                key = "4";
            } else if (i < 50) {
                key = "5";
            } else if (i < 60) {
                key = "6";
            } else
                key = "x";

            logger.info("key: {}", key);
            value = "HelloWorld!!" + Integer.toString(i);
            logger.info("Value: {}", value);

            //Create a Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", key, value);

            //send Data
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
            }).get(); //(Synchronized the send method)
            // Just used 'get()' for demonstration, never uses this on Production!! (Massively affect the performance)
        }

        //flush the producer
        kafkaProducer.flush();

        //flush and close
        kafkaProducer.close();

    }
}
