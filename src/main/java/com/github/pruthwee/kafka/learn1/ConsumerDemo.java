package com.github.pruthwee.kafka.learn1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootStrapServer = "127.0.0.1:9092";
        String group_id = "My_Fourth_Application";
        String topicName = "first_topic";

        //preparing Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Creating KafkaConsumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer to Out Topic
        kafkaConsumer.subscribe(Arrays.asList(topicName));

        //Polling the Records
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            consumerRecords.forEach(consumerRecord -> {
                logger.info("consumerRecord.key(): {} \n consumerRecord.value(): {} \n, consumerRecord.offset(): {} \n, consumerRecord.partition(): {} \n, consumerRecord.topic(): {} \n,",
                        consumerRecord.key(), consumerRecord.value(), consumerRecord.offset(), consumerRecord.partition(), consumerRecord.topic());
            });
        }
    }
}
