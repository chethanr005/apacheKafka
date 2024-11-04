package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {

    static Logger log = LoggerFactory.getLogger(ConsumerRebalanceListenerImpl.class);

    KafkaConsumer<String, String> consumer;

    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();


    ConsumerRebalanceListenerImpl(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }


    public void trackOffsets(String topic, int partition, long offset) {
        offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(partition, null));
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        log.info("onPartitionRevoked callback triggered");
        log.info("Committing offsets:", offsets);
        consumer.commitSync(offsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        log.info("onPartitionsAssigned callback triggered");
    }


    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return offsets;
    }
}
