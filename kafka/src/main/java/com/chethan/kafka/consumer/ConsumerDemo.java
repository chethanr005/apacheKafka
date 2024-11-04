package com.chethan.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Chethan on Sep 18, 2024.
 */

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        String groupId = "my-java-application";

        //Connecting to local host
        properties.setProperty("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());


        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        consumer.subscribe(Arrays.asList("Moon_Dust"));


        while (true) {

            int c = 0;
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));


            for (ConsumerRecord<String, String> record : records) {
                c = c + 1;
                log.info("key: " + record.key() + ", value: " + record.value() + "partition: " + record.partition() + ", offset: " + record.offset() + ", Message count: " + c);
            }
        }

//        for (int i = 0; i <= 1; i++) {
//
//            log.info("Polling");
//
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//
//
//            for (ConsumerRecord<String, String> record : records) {
//                log.info("key : " + record.key() + ", Value : " + record.value() + ", Topic : " + record.topic());
//            }
//
//
//            Thread.sleep(1000);
//        }


    }
}