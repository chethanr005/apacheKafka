package com.chethan.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class ConsumerDemoRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(com.chethan.kafka.consumer.ConsumerDemoRebalanceListener.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer with a Rebalance");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-java-application";
        String topic = "Moon_Dust";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        // we disable Auto Commit of offsets
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        ConsumerRebalanceListenerImpl listener = new ConsumerRebalanceListenerImpl(consumer);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        try {
            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic), listener);
            int messageCount = 0;

            // poll for new data
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    messageCount = messageCount + 1;
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset:" + record.offset() + ", Message No : " + messageCount);

                    // we track the offset we have been committed in the listener
                    listener.trackOffsets(record.topic(), record.partition(), record.offset());
                }

                // We commitAsync as we have processed all data and we don't want to block until the next .poll() call
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            log.info("Wake up exception! Consumer has gracefully shut down.");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            try {
                consumer.commitSync(listener.getCurrentOffsets()); // we must commit the offsets synchronously here
            } finally {
                consumer.close();
                log.info("The consumer is now gracefully closed.");
            }
        }
    }
}
