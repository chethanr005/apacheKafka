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

public class ProducerDemo {


    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        log.info("this is printing from logger");

        // create Producer Properties
        Properties properties = new Properties();


        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

//        //connect to condktor
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.jaas.config", "SASL_SSL");
//        properties.setProperty("bootstrap.servers", "XXXX");


        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


//        properties.setProperty("partition.class", RoundRobinPartitioner.class.getName());
//        properties.setProperty("batch.size", "400");


        // create the Producer Record
        // send data


//        for (int j = 0; j < 2; j++) {
        for (int i = 0; i < 10; i++) {
            String message = "Hello World_123_" + i;
            publishMesssage(producer, "Moon_Dust", message);


        }
//            Thread.sleep(1000);
//        }


        //tell the producer to send all the data and block until done -- synchronous
        producer.flush();

        // close the producer
        producer.close();


    }

    private static void publishMesssage(KafkaProducer<String, String> producer, String topic, String value) {

        // create the Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(/*topic name*/ topic, value);

        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) log.info("Received meta data \n" +
                    "Topic : " + recordMetadata.topic() + "\n" +
                    "Partition : " + recordMetadata.partition() + "\n" +
                    "Offset : " + recordMetadata.offset() + "\n" +
                    "Timestamp : " + recordMetadata.timestamp());
            else log.info("Error while producing", e);
        });
    }
}
