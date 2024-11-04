package kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by Chethan on Sep 16, 2024.
 */

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        log.info("this is printing from logger");

        // create Producer Properties
        Properties properties = new Properties();


        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //connect to condktor
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.jaas.config", "SASL_SSL");
//        properties.setProperty("sasl.mechanism", "PLAIN");


        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


//        properties.setProperty("partition.class", RoundRobinPartitioner.class.getName());
        //default batch size is 16kb
//        properties.setProperty("batch.size", "400");


        // create the Producer Record
        // send data


//        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 10; i++) {
                String key     = "id_" + i;
                String message = "hello_world_" + i;
                publishMesssage(producer, "Star_Dust", message, key);
//            Thread.sleep(100);
            }
//        }


        //tell the producer to send all the data and block until done -- synchronous
        producer.flush();

        // close the producer
        producer.close();


    }

    private static void publishMesssage(KafkaProducer<String, String> producer, String topic, String value, String key) {

        // create the Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>( /*topic name*/ topic, key,value);

        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) log.info("Key : " + key + "  |   Partition : " + recordMetadata.partition());
            else log.info("Error while producing", e);
        });
    }
}
