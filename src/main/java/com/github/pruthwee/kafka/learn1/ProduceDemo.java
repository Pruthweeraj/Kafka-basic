package com.github.pruthwee.kafka.learn1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProduceDemo {
    public static void main(String[] args) {
        String bootStrapServer = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //Create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "HelloWorld!!");

        //send Data (Async call)
        kafkaProducer.send(producerRecord);

        //flush the producer
//        kafkaProducer.flush();

        //flush and close
        kafkaProducer.close();

    }
}
